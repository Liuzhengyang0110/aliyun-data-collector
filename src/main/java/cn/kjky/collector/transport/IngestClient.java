package cn.kjky.collector.transport;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.contract.BatchEnvelope;
import cn.kjky.collector.contract.IngestJson;
import cn.kjky.collector.contract.IngestResponse;
import cn.kjky.collector.security.ClientCertificateInfo;
import cn.kjky.collector.security.MtlsHttpClientProvider;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;

/** 调用 POST /collector/v1/events:batch 的基础客户端。 */
public final class IngestClient implements AutoCloseable {
    public static final String BATCH_PATH = "/collector/v1/events:batch";

    private final CollectorConfig.UploadConfig config;
    private final URI endpoint;
    private final HttpClient http;
    private final MtlsHttpClientProvider mtls;

    /** 开发模式兼容入口：使用 JVM 默认服务端信任配置，不提供客户端证书。 */
    public IngestClient(CollectorConfig.UploadConfig config) {
        this(config, HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(config.connectTimeoutSeconds))
                .build());
    }

    /**
     * 使用完整配置创建客户端。启用 mTLS 时从 Secret Volume 构造隔离的 SSLContext；
     * 生产模式启用上传却未启用 mTLS 时在网络请求前失败关闭。
     */
    public IngestClient(CollectorConfig config) {
        this(requireConfig(config), createMtlsProvider(config));
    }

    /** 允许测试注入 HttpClient，同时执行与生产构造器相同的契约校验。 */
    public IngestClient(CollectorConfig.UploadConfig config, HttpClient http) {
        this(config, http, null);
    }

    private IngestClient(CollectorConfig config, MtlsHttpClientProvider mtls) {
        this(config.upload, mtls == null ? defaultHttpClient(config.upload) : null, mtls);
    }

    private IngestClient(CollectorConfig.UploadConfig config, HttpClient http, MtlsHttpClientProvider mtls) {
        this.config = Objects.requireNonNull(config, "upload 配置不能为空");
        if (http == null && mtls == null) throw new IllegalArgumentException("HTTP 客户端不能为空");
        this.http = http;
        this.mtls = mtls;
        if (!config.enabled) throw new IllegalStateException("upload.enabled=false，不能发送批次");
        if (config.ingestUrl == null || config.ingestUrl.isBlank()) {
            throw new IllegalArgumentException("upload.ingestUrl 不能为空");
        }
        this.endpoint = URI.create(config.ingestUrl);
        if (!BATCH_PATH.equals(endpoint.getPath()) || endpoint.getRawQuery() != null || endpoint.getFragment() != null) {
            throw new IllegalArgumentException("cc-ingest 地址必须使用固定路径 " + BATCH_PATH + "，且不能包含查询参数或片段");
        }
        if (mtls != null && !"https".equalsIgnoreCase(endpoint.getScheme())) {
            mtls.close();
            throw new IllegalArgumentException("启用 mTLS 时 cc-ingest 地址必须使用 HTTPS");
        }
    }

    /**
     * gzip 压缩并发送不可变批次。HTTP 失败会转换为明确 disposition；网络异常向上抛出，
     * 后续 BatchUploader 应把它作为可重试失败保存到 retry。
     */
    public IngestResult send(BatchEnvelope batch) throws IOException, InterruptedException {
        validateBatch(batch);
        byte[] json = IngestJson.writeBatch(batch);
        if (json.length > config.maxRequestBytes) {
            throw new IllegalArgumentException("批次未压缩 JSON 超过 upload.maxRequestBytes: " + json.length);
        }
        byte[] gzip = IngestJson.gzip(json);
        if (gzip.length > config.maxCompressedBytes) {
            throw new IllegalArgumentException("批次 gzip 后超过 upload.maxCompressedBytes: " + gzip.length);
        }

        HttpRequest request = HttpRequest.newBuilder(endpoint)
                .timeout(Duration.ofSeconds(config.requestTimeoutSeconds))
                .header("Content-Type", "application/json; charset=utf-8")
                .header("Content-Encoding", "gzip")
                .header("Accept", "application/json")
                .header("User-Agent", "aliyun-data-collector/0.1.0")
                .POST(HttpRequest.BodyPublishers.ofByteArray(gzip))
                .build();
        HttpClient activeClient = mtls == null ? http : mtls.client();
        HttpResponse<byte[]> response = activeClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        return classify(batch, response);
    }

    /** mTLS 启用时返回当前客户端证书安全摘要；不会返回证书正文或私钥。 */
    public ClientCertificateInfo clientCertificateInfo() {
        return mtls == null ? null : mtls.certificateInfo();
    }

    private void validateBatch(BatchEnvelope batch) {
        Objects.requireNonNull(batch, "batch 不能为空");
        if (!IngestJson.SCHEMA_VERSION.equals(batch.schemaVersion())) {
            throw new IllegalArgumentException("只支持 schema_version=" + IngestJson.SCHEMA_VERSION);
        }
        if (batch.recordCount() > config.maxRecords) {
            throw new IllegalArgumentException("批次记录数超过 upload.maxRecords");
        }
        String expectedHash = IngestJson.eventsSha256(batch.events());
        if (!expectedHash.equalsIgnoreCase(batch.contentSha256())) {
            throw new IllegalArgumentException("content_sha256 与 events 内容不一致");
        }
        batch.events().forEach(event -> {
            String payloadHash = IngestJson.sha256Canonical(event.payload());
            if (!payloadHash.equalsIgnoreCase(event.payloadSha256())) {
                throw new IllegalArgumentException("事件 " + event.eventId() + " 的 payload_sha256 与 payload 不一致");
            }
        });
    }

    private IngestResult classify(BatchEnvelope batch, HttpResponse<byte[]> httpResponse) {
        int code = httpResponse.statusCode();
        IngestResponse response = parseResponse(httpResponse.body());
        Long retryAfter = retryAfterSeconds(httpResponse);

        if (code == 200 && reliableCommit(batch, response)) {
            return new IngestResult(code, IngestDisposition.COMMITTED, response, null);
        }
        if (code == 408 || code == 429 || code >= 500) {
            return new IngestResult(code, IngestDisposition.RETRYABLE, response, retryAfter);
        }
        if (code == 400 || code == 422) {
            return new IngestResult(code, IngestDisposition.BLOCKED, response, null);
        }
        if (code == 401 || code == 403) {
            return new IngestResult(code, IngestDisposition.AUTHENTICATION_FAILED, response, null);
        }
        if (code == 409) {
            return new IngestResult(code, IngestDisposition.CONFLICT, response, null);
        }
        if (code == 413) {
            return new IngestResult(code, IngestDisposition.PAYLOAD_TOO_LARGE, response, null);
        }
        if (code >= 400 && code < 500) {
            return new IngestResult(code, IngestDisposition.BLOCKED, response, null);
        }
        return new IngestResult(code, IngestDisposition.UNKNOWN_RESPONSE, response, null);
    }

    private boolean reliableCommit(BatchEnvelope batch, IngestResponse response) {
        if (response == null || !IngestJson.SCHEMA_VERSION.equals(response.schemaVersion())) return false;
        String status = response.status() == null ? "" : response.status().toUpperCase(Locale.ROOT);
        if (!"COMMITTED".equals(status) && !"ALREADY_COMMITTED".equals(status)) return false;
        return batch.batchId().equals(response.batchId())
                && batch.contentSha256().equalsIgnoreCase(response.contentSha256())
                && response.committedAt() != null;
    }

    private IngestResponse parseResponse(byte[] body) {
        if (body == null || body.length == 0) return null;
        try {
            return IngestJson.readResponse(body);
        } catch (IOException ignored) {
            // 不保留或输出服务端原始正文，避免错误页泄露内部信息。
            return null;
        }
    }

    private Long retryAfterSeconds(HttpResponse<byte[]> response) {
        return response.headers().firstValue("Retry-After").flatMap(value -> {
            try {
                long seconds = Long.parseLong(value);
                return seconds >= 0 ? java.util.Optional.of(seconds) : java.util.Optional.empty();
            } catch (NumberFormatException ignored) {
                return java.util.Optional.empty();
            }
        }).orElse(null);
    }

    private static CollectorConfig requireConfig(CollectorConfig config) {
        Objects.requireNonNull(config, "config 不能为空");
        Objects.requireNonNull(config.upload, "upload 配置不能为空");
        Objects.requireNonNull(config.security, "security 配置不能为空");
        if (config.runtime != null && "PRODUCTION".equalsIgnoreCase(config.runtime.deploymentMode)
                && config.upload.enabled && !config.security.mtlsEnabled) {
            throw new IllegalStateException("PRODUCTION 模式启用上传时必须启用 mTLS");
        }
        return config;
    }

    private static MtlsHttpClientProvider createMtlsProvider(CollectorConfig config) {
        CollectorConfig checked = requireConfig(config);
        if (!checked.upload.enabled) throw new IllegalStateException("upload.enabled=false，不能发送批次");
        if (checked.upload.ingestUrl == null || checked.upload.ingestUrl.isBlank()) {
            throw new IllegalArgumentException("upload.ingestUrl 不能为空");
        }
        URI endpoint = URI.create(checked.upload.ingestUrl);
        if (!BATCH_PATH.equals(endpoint.getPath()) || endpoint.getRawQuery() != null || endpoint.getFragment() != null) {
            throw new IllegalArgumentException("cc-ingest 地址必须使用固定路径 " + BATCH_PATH + "，且不能包含查询参数或片段");
        }
        if (checked.security.mtlsEnabled && !"https".equalsIgnoreCase(endpoint.getScheme())) {
            throw new IllegalArgumentException("启用 mTLS 时 cc-ingest 地址必须使用 HTTPS");
        }
        return checked.security.mtlsEnabled
                ? new MtlsHttpClientProvider(checked.security, checked.upload.connectTimeoutSeconds)
                : null;
    }

    private static HttpClient defaultHttpClient(CollectorConfig.UploadConfig config) {
        return HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(config.connectTimeoutSeconds))
                .build();
    }

    @Override
    public void close() {
        if (mtls != null) mtls.close();
    }

}

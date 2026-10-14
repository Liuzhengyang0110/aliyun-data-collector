package cn.kjky.collector.control;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.security.MtlsHttpClientProvider;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/** 仅出站访问 cc-control-service 的 HTTP/mTLS 客户端。 */
public final class ControlClient implements AutoCloseable {
    public static final String CLAIM_PATH = "/collector/v1/jobs:claim";
    public static final String STATUS_PATH_TEMPLATE = "/collector/v1/jobs/{job_id}/status";
    private static final int MAX_RESPONSE_BYTES = 1024 * 1024;

    enum FailureKind { RETRYABLE, AUTHENTICATION, NOT_FOUND, CONFLICT, PERMANENT }

    static final class ControlException extends IOException {
        private final FailureKind kind;
        private final Long retryAfterMillis;

        ControlException(FailureKind kind, String message, Long retryAfterMillis) {
            super(message);
            this.kind = kind;
            this.retryAfterMillis = retryAfterMillis;
        }

        FailureKind kind() { return kind; }
        Long retryAfterMillis() { return retryAfterMillis; }
    }

    private final CollectorConfig.ControlConfig config;
    private final URI claimEndpoint;
    private final String statusEndpointTemplate;
    private final HttpClient http;
    private final MtlsHttpClientProvider mtls;
    private final ObjectMapper json = new ObjectMapper().registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    public ControlClient(CollectorConfig config) {
        Objects.requireNonNull(config, "config 不能为空");
        Objects.requireNonNull(config.control, "control 配置不能为空");
        Objects.requireNonNull(config.security, "security 配置不能为空");
        if (config.runtime != null && "PRODUCTION".equalsIgnoreCase(config.runtime.deploymentMode)
                && !config.security.mtlsEnabled) {
            throw new IllegalStateException("PRODUCTION 模式启用控制时必须启用 mTLS");
        }
        this.config = config.control;
        this.claimEndpoint = validateClaim(this.config.taskClaimUrl, config.security.mtlsEnabled);
        this.statusEndpointTemplate = validateStatus(this.config.jobStatusUrl, config.security.mtlsEnabled);
        this.mtls = config.security.mtlsEnabled
                ? new MtlsHttpClientProvider(config.security, config.control.connectTimeoutSeconds) : null;
        this.http = mtls == null ? defaultClient(this.config) : null;
    }

    /** 测试及开发态注入客户端；不会改变固定路径和响应契约校验。 */
    public ControlClient(CollectorConfig.ControlConfig config, HttpClient http) {
        this.config = Objects.requireNonNull(config, "control 配置不能为空");
        this.http = Objects.requireNonNull(http, "http 不能为空");
        this.mtls = null;
        this.claimEndpoint = validateClaim(config.taskClaimUrl, false);
        this.statusEndpointTemplate = validateStatus(config.jobStatusUrl, false);
    }

    Optional<ControlProtocol.Job> claim(ControlProtocol.ClaimRequest body)
            throws IOException, InterruptedException {
        byte[] encoded = json.writeValueAsBytes(body);
        HttpRequest request = HttpRequest.newBuilder(claimEndpoint)
                .timeout(Duration.ofSeconds(config.requestTimeoutSeconds))
                .header("Content-Type", "application/json; charset=utf-8")
                .header("Accept", "application/json")
                .header("User-Agent", "aliyun-data-collector/0.1.0")
                .POST(HttpRequest.BodyPublishers.ofByteArray(encoded)).build();
        HttpResponse<byte[]> response = client().send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() == 204) return Optional.empty();
        if (response.statusCode() != 200) throw classify(response);
        byte[] bytes = response.body();
        if (bytes == null || bytes.length == 0 || bytes.length > MAX_RESPONSE_BYTES) {
            throw new ControlException(FailureKind.PERMANENT, "领取任务响应为空或超过大小限制", null);
        }
        try {
            return Optional.of(json.readValue(bytes, ControlProtocol.Job.class));
        } catch (IOException e) {
            throw new ControlException(FailureKind.PERMANENT, "领取任务响应不符合冻结契约", null);
        }
    }

    void report(ControlProtocol.StatusUpdate body) throws IOException, InterruptedException {
        URI endpoint = URI.create(statusEndpointTemplate.replace("{job_id}", body.jobId()));
        HttpRequest request = HttpRequest.newBuilder(endpoint)
                .timeout(Duration.ofSeconds(config.requestTimeoutSeconds))
                .header("Content-Type", "application/json; charset=utf-8")
                .header("Accept", "application/json")
                .header("User-Agent", "aliyun-data-collector/0.1.0")
                .POST(HttpRequest.BodyPublishers.ofByteArray(json.writeValueAsBytes(body))).build();
        HttpResponse<byte[]> response = client().send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() != 200) throw classify(response);
        byte[] bytes = response.body();
        if (bytes == null || bytes.length == 0 || bytes.length > MAX_RESPONSE_BYTES) {
            throw new ControlException(FailureKind.PERMANENT, "任务状态回执为空或超过大小限制", null);
        }
        try {
            ControlProtocol.StatusAck ack = json.readValue(bytes, ControlProtocol.StatusAck.class);
            if (!ControlProtocol.SCHEMA_VERSION.equals(ack.schemaVersion())
                    || !body.jobId().equals(ack.jobId()) || body.status() != ack.acceptedStatus()
                    || ack.receivedAt() == null) {
                throw new ControlException(FailureKind.PERMANENT, "任务状态回执与请求不匹配", null);
            }
        } catch (ControlException e) {
            throw e;
        } catch (IOException e) {
            throw new ControlException(FailureKind.PERMANENT, "任务状态回执不符合冻结契约", null);
        }
    }

    private ControlException classify(HttpResponse<byte[]> response) {
        int code = response.statusCode();
        if (code == 408 || code == 429 || code >= 500) {
            return new ControlException(FailureKind.RETRYABLE, "控制服务暂时不可用，HTTP " + code,
                    retryAfterMillis(response));
        }
        if (code == 401 || code == 403) return new ControlException(FailureKind.AUTHENTICATION,
                "控制服务拒绝客户端身份，HTTP " + code, null);
        if (code == 404) return new ControlException(FailureKind.NOT_FOUND,
                "控制任务状态端点或任务不存在，HTTP 404", null);
        if (code == 409) return new ControlException(FailureKind.CONFLICT,
                "控制服务报告任务冲突，HTTP 409", null);
        return new ControlException(FailureKind.PERMANENT, "控制服务返回未支持状态，HTTP " + code, null);
    }

    private Long retryAfterMillis(HttpResponse<byte[]> response) {
        return response.headers().firstValue("Retry-After").flatMap(value -> {
            try {
                long seconds = Long.parseLong(value);
                return seconds >= 0 && seconds <= 86400 ? java.util.Optional.of(seconds * 1000) : java.util.Optional.empty();
            } catch (NumberFormatException ignored) {
                return java.util.Optional.empty();
            }
        }).orElse(null);
    }

    private HttpClient client() throws IOException {
        return mtls == null ? http : mtls.client();
    }

    private static URI validateClaim(String value, boolean httpsRequired) {
        URI uri = endpoint(value, "control.taskClaimUrl");
        if (!CLAIM_PATH.equals(uri.getPath()) || uri.getRawQuery() != null || uri.getFragment() != null) {
            throw new IllegalArgumentException("任务领取地址必须使用固定路径 " + CLAIM_PATH);
        }
        requireHttps(uri, httpsRequired);
        return uri;
    }

    private static String validateStatus(String value, boolean httpsRequired) {
        if (value == null || value.isBlank() || count(value, "{job_id}") != 1) {
            throw new IllegalArgumentException("control.jobStatusUrl 必须且只能包含一个 {job_id}");
        }
        URI uri = endpoint(value.replace("{job_id}", "job-1"), "control.jobStatusUrl");
        if (!STATUS_PATH_TEMPLATE.equals(uri.getPath().replace("job-1", "{job_id}"))
                || uri.getRawQuery() != null || uri.getFragment() != null) {
            throw new IllegalArgumentException("任务状态地址必须使用固定路径 " + STATUS_PATH_TEMPLATE);
        }
        requireHttps(uri, httpsRequired);
        return value;
    }

    private static URI endpoint(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " 不能为空");
        URI uri = URI.create(value);
        if (!"http".equalsIgnoreCase(uri.getScheme()) && !"https".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException(name + " 必须使用 HTTP 或 HTTPS");
        }
        if (uri.getHost() == null) throw new IllegalArgumentException(name + " 缺少主机名");
        return uri;
    }

    private static void requireHttps(URI uri, boolean required) {
        if (required && !"https".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("启用 mTLS 时控制地址必须使用 HTTPS");
        }
    }

    private static int count(String value, String token) {
        int result = 0;
        for (int offset = 0; (offset = value.indexOf(token, offset)) >= 0; offset += token.length()) result++;
        return result;
    }

    private static HttpClient defaultClient(CollectorConfig.ControlConfig config) {
        return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(config.connectTimeoutSeconds)).build();
    }

    @Override
    public void close() {
        if (mtls != null) mtls.close();
    }
}

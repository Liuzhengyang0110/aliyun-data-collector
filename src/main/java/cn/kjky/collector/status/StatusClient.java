package cn.kjky.collector.status;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.security.MtlsHttpClientProvider;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** 仅出站访问固定 /collector/v1/status 的 HTTP/mTLS 客户端。 */
public final class StatusClient implements StatusReporter.StatusTransport, AutoCloseable {
    public static final String STATUS_PATH = "/collector/v1/status";
    private static final int MAX_RESPONSE_BYTES = 64 * 1024;

    private final CollectorConfig.ControlConfig config;
    private final URI endpoint;
    private final HttpClient http;
    private final MtlsHttpClientProvider mtls;
    private final ObjectMapper json = new ObjectMapper().registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    public StatusClient(CollectorConfig root) {
        Objects.requireNonNull(root, "config 不能为空");
        this.config = Objects.requireNonNull(root.control, "control 配置不能为空");
        boolean production = root.runtime != null
                && "PRODUCTION".equalsIgnoreCase(root.runtime.deploymentMode);
        if (production && !root.security.mtlsEnabled) {
            throw new IllegalStateException("PRODUCTION 模式启用节点状态上报时必须启用 mTLS");
        }
        this.endpoint = validate(config.statusReportUrl, root.security.mtlsEnabled);
        this.mtls = root.security.mtlsEnabled
                ? new MtlsHttpClientProvider(root.security, config.connectTimeoutSeconds) : null;
        this.http = mtls == null ? defaultClient(config) : null;
    }

    StatusClient(CollectorConfig.ControlConfig config, HttpClient http) {
        this.config = Objects.requireNonNull(config, "control 配置不能为空");
        this.endpoint = validate(config.statusReportUrl, false);
        this.http = Objects.requireNonNull(http, "http 不能为空");
        this.mtls = null;
    }

    @Override
    public void report(CollectorStatusSnapshot snapshot) throws IOException, InterruptedException {
        byte[] payload = json.writeValueAsBytes(snapshot);
        HttpRequest request = HttpRequest.newBuilder(endpoint)
                .timeout(Duration.ofSeconds(config.requestTimeoutSeconds))
                .header("Content-Type", "application/json; charset=utf-8")
                .header("Accept", "application/json")
                .header("User-Agent", "aliyun-data-collector/0.1.0")
                .POST(HttpRequest.BodyPublishers.ofByteArray(payload)).build();
        HttpResponse<byte[]> response = client().send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() != 200) throw classify(response);
        byte[] body = response.body();
        if (body == null || body.length == 0 || body.length > MAX_RESPONSE_BYTES) {
            throw new StatusException("节点状态回执为空或超过大小限制", null);
        }
        try {
            StatusAcknowledgement ack = json.readValue(body, StatusAcknowledgement.class);
            if (!CollectorStatusSnapshot.SCHEMA_VERSION.equals(ack.schemaVersion())
                    || !snapshot.collectorId().equals(ack.collectorId())
                    || !"ACCEPTED".equals(ack.status()) || ack.receivedAt() == null) {
                throw new StatusException("节点状态回执与请求不匹配", null);
            }
        } catch (StatusException e) {
            throw e;
        } catch (IOException e) {
            throw new StatusException("节点状态回执不符合冻结契约", null);
        }
    }

    private StatusException classify(HttpResponse<byte[]> response) {
        Long retryAfter = response.statusCode() == 429 ? retryAfterMillis(response) : null;
        return new StatusException("节点状态服务暂时不可用，HTTP " + response.statusCode(), retryAfter);
    }

    private Long retryAfterMillis(HttpResponse<byte[]> response) {
        return response.headers().firstValue("Retry-After").flatMap(value -> {
            try {
                long seconds = Long.parseLong(value);
                return seconds >= 0 && seconds <= 86400
                        ? java.util.Optional.of(seconds * 1000) : java.util.Optional.empty();
            } catch (NumberFormatException ignored) {
                return java.util.Optional.empty();
            }
        }).orElse(null);
    }

    private HttpClient client() throws IOException {
        return mtls == null ? http : mtls.client();
    }

    private static URI validate(String value, boolean httpsRequired) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("control.statusReportUrl 不能为空");
        URI uri = URI.create(value);
        if ((!"http".equalsIgnoreCase(uri.getScheme()) && !"https".equalsIgnoreCase(uri.getScheme()))
                || uri.getHost() == null) {
            throw new IllegalArgumentException("control.statusReportUrl 必须是包含主机名的 HTTP(S) URL");
        }
        if (!STATUS_PATH.equals(uri.getPath()) || uri.getRawQuery() != null || uri.getFragment() != null) {
            throw new IllegalArgumentException("节点状态地址必须使用固定路径 " + STATUS_PATH);
        }
        if (httpsRequired && !"https".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("启用 mTLS 时节点状态地址必须使用 HTTPS");
        }
        return uri;
    }

    private static HttpClient defaultClient(CollectorConfig.ControlConfig config) {
        return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(config.connectTimeoutSeconds)).build();
    }

    @Override
    public void close() {
        if (mtls != null) mtls.close();
    }

    static final class StatusException extends IOException {
        private final Long retryAfterMillis;
        StatusException(String message, Long retryAfterMillis) {
            super(message);
            this.retryAfterMillis = retryAfterMillis;
        }
        Long retryAfterMillis() { return retryAfterMillis; }
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = false)
    record StatusAcknowledgement(String schemaVersion, String collectorId, String status, Instant receivedAt) {}
}

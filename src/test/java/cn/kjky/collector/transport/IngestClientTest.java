package cn.kjky.collector.transport;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.contract.BatchEnvelope;
import cn.kjky.collector.contract.IngestJson;
import cn.kjky.collector.contract.IngestResponse;
import cn.kjky.collector.contract.StandardEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 使用真实本地 HTTP 服务验证采集端与 cc-ingest v1 的线协议。 */
class IngestClientTest {
    private HttpServer server;
    private final AtomicReference<CapturedRequest> captured = new AtomicReference<>();
    private final AtomicReference<ResponsePlan> response = new AtomicReference<>();

    @BeforeEach
    void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(IngestClient.BATCH_PATH, this::handle);
        server.start();
    }

    @AfterEach
    void stopServer() {
        if (server != null) server.stop(0);
    }

    @Test
    void postsGzipBatchAndAcceptsCommittedReceipt() throws Exception {
        BatchEnvelope batch = batch();
        response.set(new ResponsePlan(200, "COMMITTED", null, null, false));

        IngestResult result = client().send(batch);

        assertThat(result.committed()).isTrue();
        assertThat(result.disposition()).isEqualTo(IngestDisposition.COMMITTED);
        CapturedRequest request = captured.get();
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.path()).isEqualTo(IngestClient.BATCH_PATH);
        assertThat(request.contentType()).isEqualTo("application/json; charset=utf-8");
        assertThat(request.contentEncoding()).isEqualTo("gzip");
        assertThat(request.json()).contains("\"is_last_batch\":true").doesNotContain("\"last_batch\"");
        assertThat(request.batch().batchId()).isEqualTo(batch.batchId());
        assertThat(request.batch().recordCount()).isEqualTo(1);
        assertThat(request.batch().events().get(0).payload().get("message").asText()).isEqualTo("hello");
        assertThat(request.batch().contentSha256()).isEqualTo(IngestJson.eventsSha256(request.batch().events()));
    }

    @Test
    void acceptsExplicitAlreadyCommittedReceipt() throws Exception {
        response.set(new ResponsePlan(200, "ALREADY_COMMITTED", null, null, false));

        assertThat(client().send(batch()).disposition()).isEqualTo(IngestDisposition.COMMITTED);
    }

    @Test
    void neverTreatsAcceptedOrMismatchedReceiptAsCommitted() throws Exception {
        response.set(new ResponsePlan(202, "RECEIVED", null, null, false));
        assertThat(client().send(batch()).disposition()).isEqualTo(IngestDisposition.UNKNOWN_RESPONSE);

        response.set(new ResponsePlan(200, "COMMITTED", "another-batch", null, false));
        assertThat(client().send(batch()).disposition()).isEqualTo(IngestDisposition.UNKNOWN_RESPONSE);

        response.set(new ResponsePlan(200, "COMMITTED", null, "different-hash", false));
        assertThat(client().send(batch()).disposition()).isEqualTo(IngestDisposition.UNKNOWN_RESPONSE);
    }

    @ParameterizedTest
    @MethodSource("failureResponses")
    void classifiesFailureResponses(int status, String serverStatus, IngestDisposition expected) throws Exception {
        response.set(new ResponsePlan(status, serverStatus, null, null, false));

        IngestResult result = client().send(batch());

        assertThat(result.committed()).isFalse();
        assertThat(result.disposition()).isEqualTo(expected);
    }

    static Stream<Arguments> failureResponses() {
        return Stream.of(
                Arguments.of(400, "SCHEMA_ERROR", IngestDisposition.BLOCKED),
                Arguments.of(401, "UNAUTHORIZED", IngestDisposition.AUTHENTICATION_FAILED),
                Arguments.of(403, "FORBIDDEN", IngestDisposition.AUTHENTICATION_FAILED),
                Arguments.of(409, "CONFLICT", IngestDisposition.CONFLICT),
                Arguments.of(413, "PAYLOAD_TOO_LARGE", IngestDisposition.PAYLOAD_TOO_LARGE),
                Arguments.of(503, "SERVICE_UNAVAILABLE", IngestDisposition.RETRYABLE));
    }

    @Test
    void returnsRetryAfterForRateLimit() throws Exception {
        response.set(new ResponsePlan(429, "RATE_LIMITED", null, null, false));

        IngestResult result = client().send(batch());

        assertThat(result.disposition()).isEqualTo(IngestDisposition.RETRYABLE);
        assertThat(result.retryAfterSeconds()).isEqualTo(17L);
    }

    @Test
    void malformedSuccessfulResponseCannotDeleteBatch() throws Exception {
        response.set(new ResponsePlan(200, null, null, null, true));

        IngestResult result = client().send(batch());

        assertThat(result.disposition()).isEqualTo(IngestDisposition.UNKNOWN_RESPONSE);
        assertThat(result.response()).isNull();
    }

    @Test
    void rejectsBatchWhoseHashDoesNotMatchEventsBeforeNetworkCall() {
        BatchEnvelope valid = batch();
        BatchEnvelope changed = new BatchEnvelope(valid.schemaVersion(), valid.batchId(), valid.pollId(),
                valid.batchSequence(), valid.lastBatch(), valid.collectorId(), valid.siteId(), valid.collectorType(),
                valid.sourceType(), valid.sourceKey(), valid.windowStart(), valid.windowEnd(), valid.recordCount(),
                "bad-hash", valid.events());

        assertThatThrownBy(() -> client().send(changed))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("content_sha256");
        assertThat(captured.get()).isNull();
    }

    @Test
    void productionUploadCannotBypassMtlsAtRuntime() {
        CollectorConfig config = new CollectorConfig();
        config.runtime.deploymentMode = "PRODUCTION";
        config.upload.enabled = true;
        config.upload.ingestUrl = "https://cc-ingest.example.invalid" + IngestClient.BATCH_PATH;
        config.security.mtlsEnabled = false;

        assertThatThrownBy(() -> new IngestClient(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("必须启用 mTLS");
    }

    @Test
    void mtlsCannotBeConfiguredForPlainHttpEndpoint() {
        CollectorConfig config = new CollectorConfig();
        config.upload.enabled = true;
        config.upload.ingestUrl = "http://127.0.0.1:8080" + IngestClient.BATCH_PATH;
        config.security.mtlsEnabled = true;

        assertThatThrownBy(() -> new IngestClient(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("必须使用 HTTPS");
    }

    private IngestClient client() {
        CollectorConfig.UploadConfig config = new CollectorConfig.UploadConfig();
        config.enabled = true;
        config.ingestUrl = "http://127.0.0.1:" + server.getAddress().getPort() + IngestClient.BATCH_PATH;
        config.maxRecords = 1000;
        config.maxCompressedBytes = 2L * 1024 * 1024;
        config.maxRequestBytes = 10L * 1024 * 1024;
        return new IngestClient(config);
    }

    private BatchEnvelope batch() {
        var payload = new ObjectMapper().createObjectNode();
        payload.put("message", "hello");
        payload.put("service", "order-service");
        StandardEvent event = StandardEvent.create("event-001", "flow_record",
                Instant.parse("2026-09-02T08:00:03Z"), null, null, "order-service", "pod-01",
                "sls/project-a/logstore-b/1700000000", payload);
        return BatchEnvelope.create("batch-001", "poll-001", 0, true,
                "aliyun-collector-site01", "site01", "ALIYUN", "SLS", "project-a/logstore-b",
                Instant.parse("2026-09-02T08:00:00Z"), Instant.parse("2026-09-02T08:00:30Z"), List.of(event));
    }

    private void handle(HttpExchange exchange) throws IOException {
        byte[] compressed = exchange.getRequestBody().readAllBytes();
        byte[] json = gunzip(compressed);
        BatchEnvelope batch = IngestJson.readBatch(json);
        captured.set(new CapturedRequest(exchange.getRequestMethod(), exchange.getRequestURI().getPath(),
                exchange.getRequestHeaders().getFirst("Content-Type"),
                exchange.getRequestHeaders().getFirst("Content-Encoding"),
                new String(json, StandardCharsets.UTF_8), batch));

        ResponsePlan plan = response.get();
        if (plan == null) plan = new ResponsePlan(500, "SERVICE_UNAVAILABLE", null, null, false);
        byte[] body;
        if (plan.malformed()) {
            body = "not-json".getBytes(StandardCharsets.UTF_8);
        } else {
            IngestResponse receipt = new IngestResponse(IngestJson.SCHEMA_VERSION,
                    plan.batchId() == null ? batch.batchId() : plan.batchId(),
                    plan.contentHash() == null ? batch.contentSha256() : plan.contentHash(),
                    plan.status(), plan.status() != null && plan.status().contains("COMMITTED") ? Instant.now() : null,
                    plan.status(), null);
            body = IngestJson.writeResponse(receipt);
        }
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        if (plan.httpStatus() == 429) exchange.getResponseHeaders().set("Retry-After", "17");
        exchange.sendResponseHeaders(plan.httpStatus(), body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }

    private byte[] gunzip(byte[] compressed) throws IOException {
        try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressed));
             ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            gzip.transferTo(output);
            return output.toByteArray();
        }
    }

    private record ResponsePlan(int httpStatus, String status, String batchId, String contentHash, boolean malformed) {}

    private record CapturedRequest(String method, String path, String contentType, String contentEncoding,
                                   String json, BatchEnvelope batch) {}
}

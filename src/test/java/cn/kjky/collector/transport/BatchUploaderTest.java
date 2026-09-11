package cn.kjky.collector.transport;

import cn.kjky.collector.batch.BatchBuilder;
import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.contract.BatchEnvelope;
import cn.kjky.collector.contract.IngestJson;
import cn.kjky.collector.contract.IngestResponse;
import cn.kjky.collector.contract.StandardEvent;
import cn.kjky.collector.outbox.OutboxDelivery;
import cn.kjky.collector.outbox.OutboxStore;
import cn.kjky.collector.runtime.UploadWatermarkStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 使用本地 HTTP 服务和真实 PVC 文件验证 BatchUploader 的端到端状态转换。 */
class BatchUploaderTest {
    private static final Instant NOW = Instant.parse("2026-09-03T06:00:00Z");

    @TempDir
    Path temp;

    private HttpServer server;
    private final ConcurrentLinkedQueue<ResponsePlan> plans = new ConcurrentLinkedQueue<>();
    private final List<BatchEnvelope> received = new CopyOnWriteArrayList<>();
    private final AtomicInteger activeRequests = new AtomicInteger();
    private final AtomicInteger maximumActiveRequests = new AtomicInteger();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(IngestClient.BATCH_PATH, this::handle);
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
    }

    @AfterEach
    void stopServer() {
        if (server != null) server.stop(0);
    }

    @Test
    void committedUpdatesWatermarkBeforeDeletingBatch() throws Exception {
        Context context = context("committed", 1);
        BatchEnvelope batch = batch("batch-committed", 0, true, 1);
        context.outbox.enqueue(batch);
        plans.add(ResponsePlan.receipt(200, "COMMITTED"));

        try (BatchUploader uploader = context.uploader()) {
            BatchUploader.UploadCycleResult result = uploader.uploadAvailable();
            assertThat(result.committed()).isEqualTo(1);
        }

        assertThat(context.outbox.pendingBatches()).isEmpty();
        assertThat(context.watermarks.get(batch.pollId())).get().satisfies(watermark -> {
            assertThat(watermark.completed()).isTrue();
            assertThat(watermark.continuousSequence()).isZero();
        });
    }

    @Test
    void alreadyCommittedIsReliableAndDeletesIdempotently() throws Exception {
        Context context = context("already", 1);
        BatchEnvelope batch = batch("batch-already", 0, true, 1);
        context.outbox.enqueue(batch);
        plans.add(ResponsePlan.receipt(200, "ALREADY_COMMITTED"));

        try (BatchUploader uploader = context.uploader()) {
            assertThat(uploader.uploadAvailable().committed()).isEqualTo(1);
        }

        assertThat(context.outbox.pendingBatches()).isEmpty();
        assertThat(context.watermarks.get(batch.pollId())).get().extracting(w -> w.completed()).isEqualTo(true);
    }

    @Test
    void lostResponseRetriesTheExactSameBatchAndAcceptsAlreadyCommitted() throws Exception {
        Context context = context("lost-response", 1);
        BatchEnvelope batch = batch("batch-lost", 0, true, 1);
        context.outbox.enqueue(batch);
        plans.add(ResponsePlan.drop());

        try (BatchUploader uploader = context.uploader()) {
            assertThat(uploader.uploadAvailable().retryScheduled()).isEqualTo(1);
            assertThat(context.outbox.retryBatches()).singleElement().satisfies(stored -> {
                assertThat(stored.batch().batchId()).isEqualTo(batch.batchId());
                assertThat(stored.batch().pollId()).isEqualTo(batch.pollId());
                assertThat(stored.batch().contentSha256()).isEqualTo(batch.contentSha256());
                assertThat(stored.batch().events()).extracting(StandardEvent::eventId)
                        .containsExactlyElementsOf(batch.events().stream().map(StandardEvent::eventId).toList());
            });

            context.clock.advance(Duration.ofSeconds(1));
            plans.add(ResponsePlan.receipt(200, "ALREADY_COMMITTED"));
            assertThat(uploader.uploadAvailable().committed()).isEqualTo(1);
        }

        assertThat(received).hasSize(2);
        assertThat(received).extracting(BatchEnvelope::batchId).containsOnly(batch.batchId());
        assertThat(received).extracting(BatchEnvelope::contentSha256).containsOnly(batch.contentSha256());
        assertThat(context.outbox.retryBatches()).isEmpty();
    }

    @Test
    void requestTimeoutSchedulesTimeoutRetryWithoutChangingIdentity() throws Exception {
        Context context = context("timeout", 1);
        context.config.upload.requestTimeoutSeconds = 1;
        BatchEnvelope batch = batch("batch-timeout", 0, true, 1);
        context.outbox.enqueue(batch);
        plans.add(ResponsePlan.delayed(200, "COMMITTED", 1500));

        try (BatchUploader uploader = context.uploader()) {
            assertThat(uploader.uploadAvailable().retryScheduled()).isEqualTo(1);
        }

        assertThat(context.outbox.retryBatches()).singleElement().satisfies(stored -> {
            assertThat(stored.delivery().lastErrorCategory()).isEqualTo(OutboxDelivery.ErrorCategory.TIMEOUT);
            assertThat(stored.batch().batchId()).isEqualTo(batch.batchId());
        });
    }

    @Test
    void rateLimitUsesRetryAfterExactly() throws Exception {
        Context context = context("rate-limit", 1);
        context.outbox.enqueue(batch("batch-429", 0, true, 1));
        plans.add(ResponsePlan.rateLimited(17));

        try (BatchUploader uploader = context.uploader()) {
            assertThat(uploader.uploadAvailable().retryScheduled()).isEqualTo(1);
        }

        assertThat(context.outbox.retryBatches()).singleElement().satisfies(stored -> {
            assertThat(stored.delivery().lastErrorCategory()).isEqualTo(OutboxDelivery.ErrorCategory.RATE_LIMITED);
            assertThat(stored.delivery().attemptCount()).isEqualTo(1);
            assertThat(stored.delivery().nextRetryAt()).isEqualTo(NOW.plusSeconds(17));
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {500, 502, 503})
    void serverFailuresUseJitteredExponentialBackoff(int status) throws Exception {
        Context context = context("server-" + status, 1);
        context.outbox.enqueue(batch("batch-" + status, 0, true, 1));
        plans.add(ResponsePlan.receipt(status, "SERVICE_UNAVAILABLE"));

        try (BatchUploader uploader = context.uploader()) {
            assertThat(uploader.uploadAvailable().retryScheduled()).isEqualTo(1);
        }

        assertThat(context.outbox.retryBatches()).singleElement().satisfies(stored -> {
            assertThat(stored.delivery().lastErrorCategory()).isEqualTo(OutboxDelivery.ErrorCategory.SERVER_ERROR);
            assertThat(stored.delivery().nextRetryAt()).isEqualTo(NOW.plusSeconds(1));
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 422})
    void schemaErrorsBecomeBlockedAndRemainInPending(int status) throws Exception {
        Context context = context("schema-" + status, 1);
        context.outbox.enqueue(batch("batch-schema-" + status, 0, true, 1));
        plans.add(ResponsePlan.receipt(status, "SCHEMA_ERROR"));

        try (BatchUploader uploader = context.uploader()) {
            assertThat(uploader.uploadAvailable().blocked()).isEqualTo(1);
        }

        assertThat(context.outbox.pendingBatches()).singleElement().satisfies(stored -> {
            assertThat(stored.delivery().state()).isEqualTo(OutboxDelivery.State.BLOCKED);
            assertThat(stored.delivery().lastErrorCategory()).isEqualTo(OutboxDelivery.ErrorCategory.VALIDATION);
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {401, 403})
    void authenticationFailurePausesUploaderWithoutMutatingBatch(int status) throws Exception {
        Context context = context("auth-" + status, 1);
        context.outbox.enqueue(batch("batch-auth-" + status, 0, true, 1));
        plans.add(ResponsePlan.receipt(status, "UNAUTHORIZED"));

        try (BatchUploader uploader = context.uploader()) {
            assertThat(uploader.uploadAvailable().authenticationPaused()).isEqualTo(1);
            assertThat(uploader.status().state()).isEqualTo(BatchUploader.UploaderState.PAUSED_AUTHENTICATION);
            assertThat(uploader.status().priority()).isEqualTo(BatchUploader.ErrorPriority.HIGH);
            assertThat(uploader.uploadAvailable().claimed()).isZero();
        }

        assertThat(received).hasSize(1);
        assertThat(context.outbox.pendingBatches()).singleElement().satisfies(stored -> {
            assertThat(stored.delivery().state()).isEqualTo(OutboxDelivery.State.PENDING);
            assertThat(stored.delivery().attemptCount()).isZero();
        });
    }

    @Test
    void unknownSuccessfulResponseIsRetriedAndNeverDeleted() throws Exception {
        Context context = context("unknown-2xx", 1);
        context.outbox.enqueue(batch("batch-202", 0, true, 1));
        plans.add(ResponsePlan.receipt(202, "RECEIVED"));

        try (BatchUploader uploader = context.uploader()) {
            assertThat(uploader.uploadAvailable().retryScheduled()).isEqualTo(1);
        }

        assertThat(context.watermarks.snapshot()).isEmpty();
        assertThat(context.outbox.retryBatches()).singleElement().satisfies(stored ->
                assertThat(stored.delivery().lastErrorCategory()).isEqualTo(OutboxDelivery.ErrorCategory.UNKNOWN));
    }

    @Test
    void malformedOrMismatchedReceiptIsRetriedAndNeverDeleted() throws Exception {
        Context malformed = context("malformed", 1);
        malformed.outbox.enqueue(batch("batch-malformed", 0, true, 1));
        plans.add(ResponsePlan.malformed(200));
        try (BatchUploader uploader = malformed.uploader()) {
            assertThat(uploader.uploadAvailable().retryScheduled()).isEqualTo(1);
        }
        assertThat(malformed.outbox.retryBatches()).hasSize(1);

        Context mismatch = context("mismatch", 1);
        mismatch.outbox.enqueue(batch("batch-mismatch", 0, true, 1));
        plans.add(ResponsePlan.mismatch("other-batch", "f".repeat(64)));
        try (BatchUploader uploader = mismatch.uploader()) {
            assertThat(uploader.uploadAvailable().retryScheduled()).isEqualTo(1);
        }
        assertThat(mismatch.outbox.retryBatches()).hasSize(1);
        assertThat(mismatch.watermarks.snapshot()).isEmpty();
    }

    @Test
    void conflictBecomesBlockedWithoutDeletingBatch() throws Exception {
        Context context = context("conflict", 1);
        context.outbox.enqueue(batch("batch-conflict", 0, true, 1));
        plans.add(ResponsePlan.receipt(409, "CONFLICT"));

        try (BatchUploader uploader = context.uploader()) {
            assertThat(uploader.uploadAvailable().blocked()).isEqualTo(1);
        }

        assertThat(context.outbox.pendingBatches()).singleElement().satisfies(stored ->
                assertThat(stored.delivery().lastErrorCategory()).isEqualTo(OutboxDelivery.ErrorCategory.CONFLICT));
    }

    @Test
    void payloadTooLargeCreatesStableChildrenAndKeepsParentUntilAllCommit() throws Exception {
        Context context = context("split", 2);
        BatchEnvelope parent = batch("batch-parent", 0, true, 4);
        context.outbox.enqueue(parent);
        plans.add(ResponsePlan.receipt(413, "PAYLOAD_TOO_LARGE"));

        List<String> childIds;
        try (BatchUploader uploader = context.uploader()) {
            assertThat(uploader.uploadAvailable().split()).isEqualTo(1);
        }
        assertThat(context.outbox.pendingBatches()).hasSize(3);
        var splitParent = context.outbox.pendingBatches().stream()
                .filter(stored -> stored.batch().batchId().equals(parent.batchId())).findFirst().orElseThrow();
        assertThat(splitParent.delivery().state()).isEqualTo(OutboxDelivery.State.SPLIT);
        childIds = splitParent.delivery().childBatchIds();
        assertThat(childIds).hasSize(2);

        OutboxStore restarted = new OutboxStore(context.root, "outbox", "state", context.clock);
        var restartedParent = restarted.pendingBatches().stream()
                .filter(stored -> stored.batch().batchId().equals(parent.batchId())).findFirst().orElseThrow();
        assertThat(restartedParent.delivery().childBatchIds()).containsExactlyElementsOf(childIds);
        plans.add(ResponsePlan.receipt(200, "COMMITTED"));
        plans.add(ResponsePlan.receipt(200, "COMMITTED"));
        try (BatchUploader uploader = context.uploader(restarted, batch -> { })) {
            BatchUploader.UploadCycleResult result = uploader.uploadAvailable();
            assertThat(result.committed()).isEqualTo(2);
        }

        assertThat(restarted.pendingBatches()).isEmpty();
        assertThat(restarted.retryBatches()).isEmpty();
        assertThat(context.watermarks.get(parent.pollId())).get().extracting(w -> w.completed()).isEqualTo(true);
        assertThat(received).extracting(BatchEnvelope::batchId)
                .contains(parent.batchId()).containsAll(childIds);
    }

    @Test
    void crashAfterWatermarkBeforeDeleteRetransmitsAndDeletesIdempotently() throws Exception {
        Context context = context("crash-after-watermark", 1);
        BatchEnvelope batch = batch("batch-crash", 0, true, 1);
        context.outbox.enqueue(batch);
        plans.add(ResponsePlan.receipt(200, "COMMITTED"));

        try (BatchUploader crashing = context.uploader(context.outbox,
                ignored -> { throw new IOException("simulated crash"); })) {
            assertThatThrownBy(crashing::uploadAvailable).isInstanceOf(IOException.class)
                    .hasMessageContaining("simulated crash");
        }
        assertThat(context.watermarks.get(batch.pollId())).get().extracting(w -> w.completed()).isEqualTo(true);
        assertThat(context.outbox.pendingBatches()).hasSize(1);

        OutboxStore restarted = new OutboxStore(context.root, "outbox", "state", context.clock);
        plans.add(ResponsePlan.receipt(200, "ALREADY_COMMITTED"));
        try (BatchUploader uploader = context.uploader(restarted, ignored -> { })) {
            assertThat(uploader.uploadAvailable().committed()).isEqualTo(1);
        }
        assertThat(restarted.pendingBatches()).isEmpty();
        assertThat(received).extracting(BatchEnvelope::batchId).containsExactly(batch.batchId(), batch.batchId());
    }

    @Test
    void concurrentCyclesCannotClaimOrSendTheSameBatchTwice() throws Exception {
        Context context = context("concurrent-claim", 2);
        BatchEnvelope batch = batch("batch-one", 0, true, 1);
        context.outbox.enqueue(batch);
        plans.add(ResponsePlan.delayed(200, "COMMITTED", 250));

        try (BatchUploader uploader = context.uploader()) {
            var callers = Executors.newFixedThreadPool(2);
            try {
                var first = callers.submit(uploader::uploadAvailable);
                var second = callers.submit(uploader::uploadAvailable);
                assertThat(first.get().committed() + second.get().committed()).isEqualTo(1);
            } finally {
                callers.shutdownNow();
            }
        }

        assertThat(received).extracting(BatchEnvelope::batchId).containsExactly(batch.batchId());
        assertThat(maximumActiveRequests.get()).isLessThanOrEqualTo(2);
    }

    @Test
    void maximumInFlightRequestsComesFromUploadConfiguration() throws Exception {
        Context context = context("max-in-flight", 2);
        for (int i = 0; i < 4; i++) {
            context.outbox.enqueue(batch("batch-concurrency-" + i, 0, true, 1));
            plans.add(ResponsePlan.delayed(200, "COMMITTED", 150));
        }

        try (BatchUploader uploader = context.uploader()) {
            assertThat(uploader.uploadAvailable().claimed()).isEqualTo(2);
            assertThat(uploader.uploadAvailable().claimed()).isEqualTo(2);
        }

        assertThat(received).hasSize(4);
        assertThat(maximumActiveRequests.get()).isEqualTo(2);
        assertThat(context.outbox.pendingBatches()).isEmpty();
    }

    @Test
    void warningAttemptDoesNotDeleteOrStopRetrying() throws Exception {
        Context context = context("warning", 1);
        context.config.upload.retryWarningAttempts = 1;
        context.outbox.enqueue(batch("batch-warning", 0, true, 1));
        plans.add(ResponsePlan.receipt(503, "SERVICE_UNAVAILABLE"));

        try (BatchUploader uploader = context.uploader()) {
            assertThat(uploader.uploadAvailable().retryScheduled()).isEqualTo(1);
            assertThat(uploader.lastWarning()).isNotNull();
            assertThat(uploader.lastWarning().attempts()).isEqualTo(1);
        }
        assertThat(context.outbox.retryBatches()).hasSize(1);
    }

    private Context context(String name, int maxInFlight) throws Exception {
        Path root = temp.resolve(name);
        MutableClock clock = new MutableClock(NOW);
        CollectorConfig config = new CollectorConfig();
        config.upload.enabled = true;
        config.upload.ingestUrl = "http://127.0.0.1:" + server.getAddress().getPort() + IngestClient.BATCH_PATH;
        config.upload.maxInFlightBatches = maxInFlight;
        config.upload.maxRecords = 1000;
        config.upload.maxCompressedBytes = 2L * 1024 * 1024;
        config.upload.maxRequestBytes = 10L * 1024 * 1024;
        config.upload.retryInitialDelayMillis = 1000;
        config.upload.retryMaxDelayMillis = 60_000;
        config.upload.retryJitterRatio = 0.2;
        config.upload.retryWarningAttempts = 10;
        OutboxStore outbox = new OutboxStore(root, "outbox", "state", clock);
        UploadWatermarkStore watermarks = new UploadWatermarkStore(root.resolve("state/upload-watermarks.json"));
        return new Context(root, config, clock, outbox, watermarks);
    }

    private BatchEnvelope batch(String batchId, int sequence, boolean last, int eventCount) {
        List<StandardEvent> events = new ArrayList<>();
        for (int i = 0; i < eventCount; i++) {
            var payload = new ObjectMapper().createObjectNode();
            payload.put("message", "event-value-" + i);
            payload.put("service", "order-service");
            events.add(StandardEvent.create("event-" + batchId + "-" + i, "flow_record",
                    NOW.plusSeconds(i), null, null, "order-service", "pod-01",
                    "sls/project/logstore/" + i, payload));
        }
        return BatchEnvelope.create(batchId, "poll-" + batchId, sequence, last,
                "collector-001", "site-001", "ALIYUN", "SLS", "project/logstore",
                NOW.minusSeconds(60), NOW, events);
    }

    private void handle(HttpExchange exchange) throws IOException {
        int active = activeRequests.incrementAndGet();
        maximumActiveRequests.accumulateAndGet(active, Math::max);
        try {
            BatchEnvelope batch = IngestJson.readBatch(gunzip(exchange.getRequestBody().readAllBytes()));
            received.add(batch);
            ResponsePlan plan = plans.poll();
            if (plan == null) plan = ResponsePlan.receipt(200, "COMMITTED");
            if (plan.delayMillis() > 0) {
                try {
                    Thread.sleep(plan.delayMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (plan.dropConnection()) {
                exchange.close();
                return;
            }

            byte[] responseBody;
            if (plan.malformed()) {
                responseBody = "not-json".getBytes(StandardCharsets.UTF_8);
            } else {
                String responseBatchId = plan.batchId() == null ? batch.batchId() : plan.batchId();
                String responseHash = plan.contentHash() == null ? batch.contentSha256() : plan.contentHash();
                Instant committedAt = plan.receiptStatus() != null && plan.receiptStatus().contains("COMMITTED")
                        ? NOW : null;
                responseBody = IngestJson.writeResponse(new IngestResponse(IngestJson.SCHEMA_VERSION,
                        responseBatchId, responseHash, plan.receiptStatus(), committedAt,
                        plan.receiptStatus(), null));
            }
            if (plan.retryAfterSeconds() != null) {
                exchange.getResponseHeaders().set("Retry-After", Long.toString(plan.retryAfterSeconds()));
            }
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(plan.httpStatus(), responseBody.length);
            exchange.getResponseBody().write(responseBody);
            exchange.close();
        } catch (IOException e) {
            exchange.close();
        } finally {
            activeRequests.decrementAndGet();
        }
    }

    private byte[] gunzip(byte[] compressed) throws IOException {
        try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressed));
             ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            gzip.transferTo(output);
            return output.toByteArray();
        }
    }

    private record Context(Path root, CollectorConfig config, MutableClock clock,
                           OutboxStore outbox, UploadWatermarkStore watermarks) {
        BatchUploader uploader() {
            return uploader(outbox, batch -> { });
        }

        BatchUploader uploader(OutboxStore selectedOutbox, BatchUploader.CommitHook hook) {
            IngestClient client = new IngestClient(config.upload);
            UploadRetryPolicy retry = new UploadRetryPolicy(config.upload.retryInitialDelayMillis,
                    config.upload.retryMaxDelayMillis, config.upload.retryJitterRatio,
                    config.upload.retryWarningAttempts, () -> 0.5d);
            return new BatchUploader(config, client, selectedOutbox, watermarks, retry,
                    new BatchBuilder(config.upload), clock, hook);
        }
    }

    private record ResponsePlan(int httpStatus, String receiptStatus, Long retryAfterSeconds,
                                long delayMillis, boolean malformed, boolean dropConnection,
                                String batchId, String contentHash) {
        static ResponsePlan receipt(int status, String receiptStatus) {
            return new ResponsePlan(status, receiptStatus, null, 0, false, false, null, null);
        }

        static ResponsePlan rateLimited(long retryAfterSeconds) {
            return new ResponsePlan(429, "RATE_LIMITED", retryAfterSeconds, 0,
                    false, false, null, null);
        }

        static ResponsePlan delayed(int status, String receiptStatus, long delayMillis) {
            return new ResponsePlan(status, receiptStatus, null, delayMillis,
                    false, false, null, null);
        }

        static ResponsePlan malformed(int status) {
            return new ResponsePlan(status, null, null, 0, true, false, null, null);
        }

        static ResponsePlan drop() {
            return new ResponsePlan(0, null, null, 0, false, true, null, null);
        }

        static ResponsePlan mismatch(String batchId, String hash) {
            return new ResponsePlan(200, "COMMITTED", null, 0, false, false, batchId, hash);
        }
    }

    private static final class MutableClock extends Clock {
        private final AtomicReference<Instant> now;

        private MutableClock(Instant initial) {
            this.now = new AtomicReference<>(initial);
        }

        void advance(Duration duration) {
            now.updateAndGet(value -> value.plus(duration));
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now.get();
        }
    }
}

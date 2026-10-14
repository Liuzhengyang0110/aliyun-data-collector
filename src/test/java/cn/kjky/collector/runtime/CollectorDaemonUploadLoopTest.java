package cn.kjky.collector.runtime;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.control.CollectionGate;
import cn.kjky.collector.control.ManualPauseStateStore;
import cn.kjky.collector.contract.BatchEnvelope;
import cn.kjky.collector.contract.IngestJson;
import cn.kjky.collector.contract.IngestResponse;
import cn.kjky.collector.contract.StandardEvent;
import cn.kjky.collector.outbox.OutboxStore;
import cn.kjky.collector.transport.BatchUploader;
import cn.kjky.collector.transport.IngestClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/** CollectorDaemon 采集/上传双循环以及 UploadLoop 常驻恢复测试。 */
class CollectorDaemonUploadLoopTest {
    private static final Instant WINDOW_END = Instant.parse("2026-09-04T01:00:00Z");

    @TempDir
    Path temp;

    private HttpServer server;
    private final AtomicInteger responseStatus = new AtomicInteger(200);
    private final AtomicInteger received = new AtomicInteger();
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicInteger maximumActive = new AtomicInteger();
    private final AtomicInteger responseDelayMillis = new AtomicInteger();

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
    void unavailableIngestDoesNotStopCollectionAndPendingAutoUploadsAfterRecovery() throws Exception {
        Context context = context("unavailable", 2);
        responseStatus.set(503);
        EnqueueLoop collection = new EnqueueLoop(context.outbox(), 4, 15);
        UploadLoop upload = uploadLoop(context);

        try (CollectorDaemon daemon = new CollectorDaemon(collection, upload)) {
            daemon.start();
            eventually(() -> collection.produced.get() == 4);
            eventually(() -> queued(context.outbox()) == 4);
            assertThat(collection.running.get()).isTrue();

            responseStatus.set(200);
            context.outbox().signalAvailabilityChange();
            eventually(() -> queued(context.outbox()) == 0);
        }

        assertThat(received.get()).isGreaterThanOrEqualTo(4);
    }

    @Test
    void slowerUploadKeepsEveryBatchUntilReliableReceipt() throws Exception {
        Context context = context("slow", 2);
        responseDelayMillis.set(250);
        EnqueueLoop collection = new EnqueueLoop(context.outbox(), 8, 5);
        UploadLoop upload = uploadLoop(context);

        try (CollectorDaemon daemon = new CollectorDaemon(collection, upload)) {
            daemon.start();
            eventually(() -> collection.produced.get() == 8);
            assertThat(queued(context.outbox())).isGreaterThan(0);
            eventually(() -> queued(context.outbox()) == 0);
        }

        assertThat(received).hasValue(8);
    }

    @Test
    void failedCollectionLoopDoesNotPreventExistingPendingFromUploading() throws Exception {
        Context context = context("collection-failure", 1);
        context.outbox().enqueue(batch("existing"));
        FailingCollectionLoop collection = new FailingCollectionLoop();
        UploadLoop upload = uploadLoop(context);

        try (CollectorDaemon daemon = new CollectorDaemon(collection, upload)) {
            daemon.start();
            eventually(() -> collection.attempts.get() >= 2);
            eventually(() -> queued(context.outbox()) == 0);
            assertThat(collection.running.get()).isTrue();
        }

        assertThat(received).hasValue(1);
    }

    @Test
    void authenticationFailurePausesOnlyUploadAndCanBeExplicitlyResumed() throws Exception {
        Context context = context("authentication", 2);
        responseStatus.set(401);
        EnqueueLoop collection = new EnqueueLoop(context.outbox(), 5, 15);
        UploadLoop upload = uploadLoop(context);

        try (CollectorDaemon daemon = new CollectorDaemon(collection, upload)) {
            daemon.start();
            eventually(() -> daemon.uploadStatus().state() == BatchUploader.UploaderState.PAUSED_AUTHENTICATION);
            eventually(() -> collection.produced.get() == 5);
            assertThat(collection.running.get()).isTrue();
            assertThat(queued(context.outbox())).isEqualTo(5);

            responseStatus.set(200);
            daemon.resumeUploadAfterAuthenticationRepair();
            eventually(() -> queued(context.outbox()) == 0);
        }
    }

    @Test
    void daemonCloseGracefullyStopsBothLoops() throws Exception {
        Context context = context("close", 1);
        EnqueueLoop collection = new EnqueueLoop(context.outbox(), 100, 20);
        UploadLoop upload = uploadLoop(context);
        CollectorDaemon daemon = new CollectorDaemon(collection, upload);
        daemon.start();
        eventually(() -> collection.produced.get() > 0 && upload.running());

        daemon.close();

        assertThat(collection.closed.get()).isTrue();
        assertThat(collection.running.get()).isFalse();
        assertThat(upload.awaitTermination(Duration.ofSeconds(1))).isTrue();
        assertThat(upload.running()).isFalse();
    }

    @Test
    void retryIsRecoveredAndScheduledAutomaticallyAfterRestart() throws Exception {
        Context first = context("restart", 1);
        responseStatus.set(503);
        first.outbox().enqueue(batch("retry-after-restart"));
        UploadLoop firstLoop = uploadLoop(first);
        firstLoop.start();
        eventually(() -> first.outbox().retryBatches().size() == 1);
        firstLoop.close();

        responseStatus.set(200);
        OutboxStore restartedOutbox = new OutboxStore(first.root(), "outbox", "state", Clock.systemUTC());
        UploadWatermarkStore restartedWatermarks = new UploadWatermarkStore(
                first.root().resolve("state").resolve(UploadWatermarkStore.FILE_NAME));
        Context restarted = new Context(first.root(), first.config(), restartedOutbox, restartedWatermarks);
        try (UploadLoop restartedLoop = uploadLoop(restarted)) {
            restartedLoop.start();
            eventually(() -> queued(restartedOutbox) == 0);
        }

        assertThat(restartedWatermarks.get("poll-retry-after-restart")).isPresent();
    }

    @Test
    void configuredMaximumConcurrencyIsNeverExceeded() throws Exception {
        Context context = context("concurrency", 3);
        responseDelayMillis.set(200);
        for (int i = 0; i < 9; i++) context.outbox().enqueue(batch("concurrent-" + i));

        try (UploadLoop loop = uploadLoop(context)) {
            loop.start();
            eventually(() -> queued(context.outbox()) == 0);
        }

        assertThat(maximumActive.get()).isEqualTo(3);
        assertThat(received).hasValue(9);
    }

    @Test
    void criticalCapacityPausesCollectionWhileUploadContinuesDrainingOutbox() throws Exception {
        Context context = context("critical-capacity", 1);
        context.config().runtime.deploymentMode = "PRODUCTION";
        context.config().storage.rawPersistence.enabled = false;
        context.config().arms.enabled = true;
        context.config().sls.enabled = false;
        context.config().scheduler.jitterSeconds = 0;
        context.config().scheduler.arms.pollIntervalSeconds = 1;
        context.outbox().enqueue(batch("already-pending"));

        CapacityStateStore capacityStore = new CapacityStateStore(
                context.root().resolve("state").resolve(CapacityStateStore.FILE_NAME));
        DiskUsageProbe highUsage = ignored -> new DiskUsageProbe.Usage(100, 5);
        CapacityMonitor capacity = new CapacityMonitor(context.config().storage,
                Files.createDirectories(context.root().resolve("outbox")), capacityStore, highUsage,
                Clock.fixed(WINDOW_END, java.time.ZoneOffset.UTC),
                Executors.newSingleThreadScheduledExecutor(), ignored -> { }, ignored -> { });
        AtomicInteger queryWindows = new AtomicInteger();
        ProductionPollScheduler collection = new ProductionPollScheduler(context.config(),
                (source, queryEnd, lookback) -> queryWindows.incrementAndGet(),
                Clock.fixed(WINDOW_END, java.time.ZoneOffset.UTC),
                Executors.newSingleThreadScheduledExecutor(), new Random(1),
                ignored -> { }, ignored -> { }, capacity::collectionAllowed);
        UploadLoop upload = uploadLoop(context);

        try (CollectorDaemon daemon = new CollectorDaemon(capacity, collection, upload)) {
            daemon.start();
            eventually(() -> daemon.capacityState() == CapacityState.CRITICAL);
            eventually(() -> queued(context.outbox()) == 0);
            Thread.sleep(100);
            assertThat(queryWindows).hasValue(0);
            assertThat(received).hasValue(1);
        }
    }

    @Test
    void manualPauseStopsOnlyNewCollectionWindowsWhileUploadContinues() throws Exception {
        Context context = context("manual-pause", 1);
        context.config().runtime.deploymentMode = "PRODUCTION";
        context.config().storage.rawPersistence.enabled = false;
        context.config().arms.enabled = true;
        context.config().sls.enabled = false;
        context.config().scheduler.jitterSeconds = 0;
        context.config().scheduler.arms.pollIntervalSeconds = 1;
        context.outbox().enqueue(batch("pending-before-pause"));

        ManualPauseStateStore pauses = new ManualPauseStateStore(
                context.root().resolve("state").resolve(ManualPauseStateStore.FILE_NAME));
        pauses.pause(null, 1, "operator pause", WINDOW_END);
        CollectionGate gate = new CollectionGate(pauses, () -> true, () -> true, () -> true);
        AtomicInteger queryWindows = new AtomicInteger();
        ProductionPollScheduler collection = new ProductionPollScheduler(context.config(),
                (source, queryEnd, lookback) -> queryWindows.incrementAndGet(), gate::collectionAllowed);
        UploadLoop upload = uploadLoop(context);

        try (CollectorDaemon daemon = new CollectorDaemon(null, null, gate, null, collection, upload)) {
            daemon.start();
            eventually(() -> queued(context.outbox()) == 0);
            Thread.sleep(100);
            assertThat(queryWindows).hasValue(0);
            assertThat(received).hasValue(1);
        }
    }

    @Test
    void daemonCloseSafelyStopsIndependentControlLoop() {
        TrackingLoop control = new TrackingLoop();
        TrackingLoop collection = new TrackingLoop();
        CollectorDaemon daemon = new CollectorDaemon(null, null, null, control, collection, null);

        daemon.start();
        daemon.close();

        assertThat(control.started).isTrue();
        assertThat(control.closed).isTrue();
        assertThat(collection.started).isTrue();
        assertThat(collection.closed).isTrue();
    }

    @Test
    void daemonStartsAndClosesStatusLoopIndependently() {
        TrackingLoop status = new TrackingLoop();
        TrackingLoop control = new TrackingLoop();
        TrackingLoop collection = new TrackingLoop();
        CollectorDaemon daemon = new CollectorDaemon(null, null, null, control,
                status, collection, null);

        daemon.start();
        daemon.close();

        assertThat(status.started).isTrue();
        assertThat(status.closed).isTrue();
        assertThat(control.started).isTrue();
        assertThat(collection.closed).isTrue();
    }

    private Context context(String name, int maxInFlight) throws Exception {
        Path root = temp.resolve(name);
        CollectorConfig config = new CollectorConfig();
        config.upload.enabled = true;
        config.upload.ingestUrl = "http://127.0.0.1:" + server.getAddress().getPort() + IngestClient.BATCH_PATH;
        config.upload.maxInFlightBatches = maxInFlight;
        config.upload.retryInitialDelayMillis = 100;
        config.upload.retryMaxDelayMillis = 100;
        config.upload.retryJitterRatio = 0;
        config.upload.connectTimeoutSeconds = 1;
        config.upload.requestTimeoutSeconds = 2;
        config.scheduler.shutdownGraceSeconds = 2;
        OutboxStore outbox = new OutboxStore(root, "outbox", "state", Clock.systemUTC());
        UploadWatermarkStore watermarks = new UploadWatermarkStore(
                root.resolve("state").resolve(UploadWatermarkStore.FILE_NAME));
        return new Context(root, config, outbox, watermarks);
    }

    private UploadLoop uploadLoop(Context context) {
        // 本测试只验证上传循环/容量协作，显式注入本地 HTTP 测试客户端；生产构造路径仍强制 mTLS。
        BatchUploader uploader = new BatchUploader(context.config(), new IngestClient(context.config().upload),
                context.outbox(), context.watermarks());
        return new UploadLoop(uploader, context.outbox(), Clock.systemUTC(), Duration.ofMillis(50),
                2, ignored -> { }, ignored -> { });
    }

    private int queued(OutboxStore outbox) throws IOException {
        return outbox.pendingBatches().size() + outbox.retryBatches().size();
    }

    private BatchEnvelope batch(String id) {
        var payload = new ObjectMapper().createObjectNode().put("message", id);
        StandardEvent event = StandardEvent.create("event-" + id, "flow_record", WINDOW_END.minusSeconds(1),
                null, null, "orders", "pod-1", "sls/project/logstore/" + id, payload);
        return BatchEnvelope.create("batch-" + id, "poll-" + id, 0, true,
                "collector-1", "site-1", "ALIYUN", "SLS", "project/logstore",
                WINDOW_END.minusSeconds(60), WINDOW_END, java.util.List.of(event));
    }

    private void handle(HttpExchange exchange) throws IOException {
        int nowActive = active.incrementAndGet();
        maximumActive.accumulateAndGet(nowActive, Math::max);
        try {
            BatchEnvelope request = IngestJson.readBatch(gunzip(exchange.getRequestBody().readAllBytes()));
            received.incrementAndGet();
            int delay = responseDelayMillis.get();
            if (delay > 0) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            int status = responseStatus.get();
            byte[] body = status == 200
                    ? IngestJson.writeResponse(new IngestResponse(IngestJson.SCHEMA_VERSION,
                    request.batchId(), request.contentSha256(), "COMMITTED", Instant.now(), "COMMITTED", null))
                    : "{}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
        } finally {
            active.decrementAndGet();
            exchange.close();
        }
    }

    private byte[] gunzip(byte[] compressed) throws IOException {
        try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressed));
             ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            gzip.transferTo(output);
            return output.toByteArray();
        }
    }

    private void eventually(CheckedCondition condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        Throwable last = null;
        while (System.nanoTime() < deadline) {
            try {
                if (condition.test()) return;
            } catch (Throwable e) {
                last = e;
            }
            Thread.sleep(20);
        }
        if (last instanceof Exception exception) throw exception;
        throw new AssertionError("条件在超时时间内未满足", last);
    }

    private record Context(Path root, CollectorConfig config, OutboxStore outbox,
                           UploadWatermarkStore watermarks) {
    }

    @FunctionalInterface
    private interface CheckedCondition {
        boolean test() throws Exception;
    }

    private final class EnqueueLoop implements DaemonLoop {
        private final OutboxStore outbox;
        private final int target;
        private final long intervalMillis;
        private final AtomicBoolean accepting = new AtomicBoolean(true);
        private final AtomicBoolean running = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicInteger produced = new AtomicInteger();
        private Thread worker;

        private EnqueueLoop(OutboxStore outbox, int target, long intervalMillis) {
            this.outbox = outbox;
            this.target = target;
            this.intervalMillis = intervalMillis;
        }

        @Override
        public void start() {
            worker = new Thread(() -> {
                running.set(true);
                try {
                    while (accepting.get() && produced.get() < target) {
                        int sequence = produced.get();
                        outbox.enqueue(batch("collected-" + sequence));
                        produced.incrementAndGet();
                        Thread.sleep(intervalMillis);
                    }
                    while (accepting.get()) Thread.sleep(20);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                } finally {
                    running.set(false);
                }
            }, "test-collection-loop");
            worker.start();
        }

        @Override
        public void close() {
            closed.set(true);
            accepting.set(false);
            if (worker != null) {
                worker.interrupt();
                try {
                    worker.join(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private static final class FailingCollectionLoop implements DaemonLoop {
        private final AtomicBoolean accepting = new AtomicBoolean(true);
        private final AtomicBoolean running = new AtomicBoolean();
        private final AtomicInteger attempts = new AtomicInteger();
        private Thread worker;

        @Override
        public void start() {
            worker = new Thread(() -> {
                running.set(true);
                try {
                    while (accepting.get()) {
                        attempts.incrementAndGet();
                        try {
                            throw new IOException("simulated ARMS failure");
                        } catch (IOException ignored) {
                            // 模拟采集调度器记录错误后继续下一轮。
                        }
                        Thread.sleep(20);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    running.set(false);
                }
            }, "test-failing-collection-loop");
            worker.start();
        }

        @Override
        public void close() {
            accepting.set(false);
            if (worker != null) {
                worker.interrupt();
                try {
                    worker.join(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private static final class TrackingLoop implements DaemonLoop {
        private boolean started;
        private boolean closed;

        @Override public void start() { started = true; }
        @Override public void close() { closed = true; }
    }
}

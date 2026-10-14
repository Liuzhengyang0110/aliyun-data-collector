package cn.kjky.collector.status;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.contract.BatchEnvelope;
import cn.kjky.collector.contract.StandardEvent;
import cn.kjky.collector.control.JobStateStore;
import cn.kjky.collector.control.ManualPauseStateStore;
import cn.kjky.collector.outbox.OutboxStore;
import cn.kjky.collector.runtime.CapacityMonitor;
import cn.kjky.collector.runtime.CollectionWatermarkStore;
import cn.kjky.collector.runtime.OutboxQuotaManager;
import cn.kjky.collector.runtime.ProductionPollScheduler;
import cn.kjky.collector.runtime.UploadWatermarkStore;
import com.sun.net.httpserver.HttpServer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Random;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class StatusReporterIntegrationTest {
    private static final Instant NOW = Instant.parse("2026-09-14T02:00:00Z");
    @TempDir Path temp;

    @Test
    void localControlServiceReceivesFrozenContractWithoutSensitivePayload() throws Exception {
        var bodies = new CopyOnWriteArrayList<String>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(StatusClient.STATUS_PATH, exchange -> {
            bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] response = ("{\"schema_version\":\"1.0\",\"collector_id\":\"collector-001\","
                    + "\"status\":\"ACCEPTED\",\"received_at\":\"2026-09-14T02:00:01Z\"}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try (Context context = context(temp.resolve("http"))) {
            context.config.control.statusReportUrl = "http://127.0.0.1:" + server.getAddress().getPort()
                    + StatusClient.STATUS_PATH;
            try (StatusClient client = new StatusClient(context.config.control, HttpClient.newHttpClient())) {
                client.report(context.registry.snapshot(NOW.plusSeconds(1)));
            }
        } finally {
            server.stop(0);
        }

        assertThat(bodies).hasSize(1);
        assertThat(bodies.get(0))
                .contains("\"collector_id\":\"collector-001\"", "\"status_reasons\"",
                        "\"pvc_total_bytes\"", "\"pending\"", "\"watermarks\"")
                .doesNotContain("secret_payload", "AccessKey", "privateKey", "statusReportUrl",
                        "http://", "payload", "Secret");
    }

    @Test
    void retriesWithBoundedDelayThenSendsOnlyLatestAndCoalescesRapidChanges() throws Exception {
        try (Context context = context(temp.resolve("reporter"))) {
            context.config.control.heartbeatIntervalSeconds = 1;
            context.config.control.statusChangeMinIntervalMillis = 300;
            context.config.control.retryInitialDelayMillis = 50;
            context.config.control.retryMaxDelayMillis = 100;
            context.config.control.retryJitterRatio = 0;
            AtomicInteger attempts = new AtomicInteger();
            var accepted = new CopyOnWriteArrayList<CollectorStatusSnapshot>();
            StatusReporter.StatusTransport transport = snapshot -> {
                int attempt = attempts.incrementAndGet();
                if (attempt <= 2) throw new java.io.IOException("mock status service unavailable");
                accepted.add(snapshot);
            };
            try (StatusReporter reporter = new StatusReporter(context.config, context.registry, transport,
                    null, Clock.fixed(NOW, ZoneOffset.UTC), new Random(1), value -> { }, value -> { })) {
                reporter.start();
                await(() -> accepted.size() >= 1, 2000);
                assertThat(attempts.get()).isGreaterThanOrEqualTo(3);

                context.pause.pause(null, 1, "maintenance", NOW.plusSeconds(1));
                await(() -> accepted.stream().anyMatch(snapshot -> snapshot.statusReasons()
                        .contains(CollectorStatusSnapshot.StatusReason.MANUAL_PAUSE)), 1500);

                context.outbox.enqueue(batch("blocked-change"));
                var claim = context.outbox.claimNext(NOW.plusSeconds(2)).orElseThrow();
                context.outbox.markBlocked(claim,
                        cn.kjky.collector.outbox.OutboxDelivery.ErrorCategory.VALIDATION);
                await(() -> accepted.stream().anyMatch(snapshot -> snapshot.upload().blocked().count() == 1),
                        1500);

                int beforeBurst = accepted.size();
                for (int i = 0; i < 30; i++) reporter.signalChange();
                await(() -> accepted.size() > beforeBurst, 1000);
                Thread.sleep(200);
                assertThat(accepted.size() - beforeBurst).isEqualTo(1);

                int beforeHeartbeat = accepted.size();
                await(() -> accepted.size() > beforeHeartbeat, 1600);
            }
        }
    }

    @Test
    void concurrentSnapshotsNeverContradictCombinedPauseState() throws Exception {
        try (Context context = context(temp.resolve("consistent"))) {
            var pool = Executors.newFixedThreadPool(4);
            try {
                var writer = pool.submit(() -> {
                    for (int version = 1; version <= 20; version++) {
                        if ((version & 1) == 1) {
                            context.pause.pause(null, version, "test", NOW.plusSeconds(version));
                        } else {
                            context.pause.resume(null, version, NOW.plusSeconds(version));
                        }
                    }
                    return null;
                });
                var readers = java.util.stream.IntStream.range(0, 3).mapToObj(index -> pool.submit(() -> {
                    for (int i = 0; i < 60; i++) {
                        CollectorStatusSnapshot snapshot = context.registry.snapshot(NOW.plusSeconds(100 + i));
                        boolean manual = snapshot.statusReasons()
                                .contains(CollectorStatusSnapshot.StatusReason.MANUAL_PAUSE);
                        assertThat(snapshot.overallStatus() == CollectorStatusSnapshot.OverallStatus.PAUSED)
                                .isEqualTo(manual);
                        assertThat(snapshot.capacity().pvcUsedBytes()).isEqualTo(
                                snapshot.capacity().pvcTotalBytes() - snapshot.capacity().pvcUsableBytes());
                    }
                    return null;
                })).toList();
                writer.get();
                for (var reader : readers) reader.get();
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test
    void reportsCombinedPauseReasonsBacklogAndPhysicalLogicalCapacity() throws Exception {
        try (Context context = context(temp.resolve("combined"))) {
            context.outbox.enqueue(batch("quota"));
            context.capacity.checkNow();
            context.quota.checkNow();
            context.quota.applyRequestedQuota(10, 1);
            context.pause.pause(null, 2, "operator", NOW.plusSeconds(1));

            CollectorStatusSnapshot snapshot = context.registry.snapshot(NOW.plusSeconds(120));

            assertThat(snapshot.overallStatus()).isEqualTo(CollectorStatusSnapshot.OverallStatus.PAUSED);
            assertThat(snapshot.statusReasons()).contains(
                    CollectorStatusSnapshot.StatusReason.MANUAL_PAUSE,
                    CollectorStatusSnapshot.StatusReason.LOGICAL_QUOTA_CRITICAL);
            assertThat(snapshot.upload().pending().count()).isEqualTo(1);
            assertThat(snapshot.upload().pending().bytes()).isPositive();
            assertThat(snapshot.capacity().outboxUsedBytes()).isPositive();
            assertThat(snapshot.capacity().requestedQuotaBytes()).isEqualTo(10);
            assertThat(snapshot.capacity().effectiveQuotaBytes()).isEqualTo(10);
            assertThat(snapshot.capacity().state()).isEqualTo(CollectorStatusSnapshot.CapacityState.CRITICAL);
            assertThat(snapshot.capacity().pvcTotalBytes()).isPositive();
            assertThat(snapshot.capacity().pvcUsableBytes()).isPositive();
        }
    }

    @Test
    void statusFailureDoesNotBlockIndependentCollectionOrUploadWork() throws Exception {
        try (Context context = context(temp.resolve("isolation"))) {
            context.config.control.heartbeatIntervalSeconds = 1;
            context.config.control.statusChangeMinIntervalMillis = 100;
            context.config.control.retryInitialDelayMillis = 20;
            context.config.control.retryMaxDelayMillis = 40;
            context.config.control.retryJitterRatio = 0;
            AtomicInteger statusAttempts = new AtomicInteger();
            AtomicInteger collectionWork = new AtomicInteger();
            AtomicInteger uploadWork = new AtomicInteger();
            var businessLoops = Executors.newScheduledThreadPool(2);
            businessLoops.scheduleAtFixedRate(collectionWork::incrementAndGet, 0, 10, TimeUnit.MILLISECONDS);
            businessLoops.scheduleAtFixedRate(uploadWork::incrementAndGet, 0, 10, TimeUnit.MILLISECONDS);
            try (StatusReporter reporter = new StatusReporter(context.config, context.registry, snapshot -> {
                statusAttempts.incrementAndGet();
                throw new java.io.IOException("status unavailable");
            }, null, Clock.fixed(NOW, ZoneOffset.UTC), new Random(2), value -> { }, value -> { })) {
                reporter.start();
                await(() -> statusAttempts.get() >= 3, 1000);
                assertThat(collectionWork.get()).isGreaterThan(5);
                assertThat(uploadWork.get()).isGreaterThan(5);
                assertThat(context.outbox.pendingBatches()).isEmpty();
                assertThat(context.outbox.retryBatches()).isEmpty();
            } finally {
                businessLoops.shutdownNow();
            }
        }
    }

    private Context context(Path root) throws Exception {
        CollectorConfig config = new CollectorConfig();
        config.runtime.deploymentMode = "PRODUCTION";
        config.storage.rawPersistence.enabled = false;
        config.storage.dataRoot = root.toString();
        config.storage.outboxQuota.defaultBytes = 10 * 1024 * 1024;
        config.storage.outboxQuota.minBytes = 1;
        config.storage.outboxQuota.maxBytes = 1024L * 1024 * 1024;
        config.storage.outboxQuota.reservedFreeBytes = 0;
        config.collector.collectorId = "collector-001";
        config.collector.siteId = "site-001";
        config.arms.enabled = false;
        config.sls.enabled = false;
        OutboxStore outbox = new OutboxStore(config);
        CapacityMonitor capacity = new CapacityMonitor(config, outbox.outboxRoot(), outbox.stateRoot());
        OutboxQuotaManager quota = new OutboxQuotaManager(config, outbox.outboxRoot(), outbox.stateRoot());
        ManualPauseStateStore pause = new ManualPauseStateStore(
                outbox.stateRoot().resolve(ManualPauseStateStore.FILE_NAME));
        JobStateStore jobs = new JobStateStore(outbox.stateRoot().resolve(JobStateStore.FILE_NAME));
        ProductionPollScheduler scheduler = new ProductionPollScheduler(config, (source, end, lookback) -> { });
        CollectionWatermarkStore collection = new CollectionWatermarkStore(
                outbox.stateRoot().resolve("collection-watermarks.json"));
        UploadWatermarkStore upload = new UploadWatermarkStore(
                outbox.stateRoot().resolve(UploadWatermarkStore.FILE_NAME));
        RuntimeStatusRegistry registry = new RuntimeStatusRegistry(config, NOW, scheduler, collection,
                null, upload, outbox, capacity, quota, pause, jobs);
        return new Context(config, registry, pause, outbox, scheduler, quota, capacity);
    }

    private BatchEnvelope batch(String id) {
        var payload = new ObjectMapper().createObjectNode().put("secret_payload", id);
        StandardEvent event = StandardEvent.create("event-" + id, "flow_record", NOW,
                null, null, "service-a", "instance-a", "sls/project/logstore", payload);
        return BatchEnvelope.create("batch-" + id, "poll-001", 0, true,
                "collector-001", "site-001", "ALIYUN", "SLS", "project/logstore",
                NOW.minusSeconds(60), NOW, List.of(event));
    }

    private void await(java.util.function.BooleanSupplier condition, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(10);
        assertThat(condition.getAsBoolean()).isTrue();
    }

    private record Context(CollectorConfig config, RuntimeStatusRegistry registry,
                           ManualPauseStateStore pause, OutboxStore outbox,
                           ProductionPollScheduler scheduler,
                           OutboxQuotaManager quota, CapacityMonitor capacity) implements AutoCloseable {
        @Override public void close() {
            scheduler.close();
            quota.close();
            capacity.close();
        }
    }
}

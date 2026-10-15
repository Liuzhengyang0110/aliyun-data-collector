package cn.kjky.collector.runtime;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.control.CollectionGate;
import cn.kjky.collector.control.ControlClient;
import cn.kjky.collector.control.JobExecutor;
import cn.kjky.collector.control.JobPoller;
import cn.kjky.collector.control.JobStateStore;
import cn.kjky.collector.control.ManualPauseStateStore;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** CTRL-02：把实际任务执行、持久暂停状态和生产调度门禁连在一起验证。 */
class ControlledCollectionLifecycleTest {
    @TempDir Path temp;
    private HttpServer server;
    private java.util.concurrent.ExecutorService serverExecutor;
    private final Queue<String> jobsToDeliver = new ConcurrentLinkedQueue<>();
    private final List<String> acceptedStatuses = new CopyOnWriteArrayList<>();
    private final AtomicInteger claims = new AtomicInteger();

    @AfterEach
    void stopServer() {
        if (server != null) server.stop(0);
        if (serverExecutor != null) serverExecutor.shutdownNow();
    }

    @Test
    void waitingResumePauseContinueAndSameDirectoryRestartKeepWatermarkBehindGate() throws Exception {
        startServer();
        Path data = temp.resolve("collector-data");
        Path state = Files.createDirectories(data.resolve("state"));
        Path outbox = Files.createDirectories(data.resolve("outbox"));
        CollectorConfig config = config();
        MutableClock clock = new MutableClock(Instant.parse("2026-10-14T02:00:00Z"));
        Path pauseFile = state.resolve(ManualPauseStateStore.FILE_NAME);
        Path jobsFile = state.resolve(JobStateStore.FILE_NAME);
        Path watermarkFile = state.resolve("collection-watermarks.json");

        ManualPauseStateStore pauses = new ManualPauseStateStore(pauseFile, true, false);
        JobStateStore jobStates = new JobStateStore(jobsFile);
        OutboxQuotaManager quotas = new OutboxQuotaManager(config, outbox, state);
        CollectionGate gate = new CollectionGate(pauses, quotas::collectionAllowed, () -> true, () -> true);
        CollectionWatermarkStore watermarks = new CollectionWatermarkStore(watermarkFile, clock);
        AtomicInteger cloudQueries = new AtomicInteger();
        AtomicBoolean blockNextQuery = new AtomicBoolean();
        CountDownLatch inFlightQueryStarted = new CountDownLatch(1);
        CountDownLatch allowInFlightQueryToFinish = new CountDownLatch(1);
        List<Instant> queryStarts = new CopyOnWriteArrayList<>();
        ProductionPollScheduler scheduler = new ProductionPollScheduler(config,
                (source, queryEnd, initialLookbackSeconds) -> {
                    String key = source.name() + "/orders";
                    Instant start = watermarks.startFor(key, queryEnd.minusSeconds(initialLookbackSeconds));
                    queryStarts.add(start);
                    // 该计数函数代表 ARMS/SLS 查询边界；只有通过组合门禁才能调用。
                    cloudQueries.incrementAndGet();
                    if (blockNextQuery.compareAndSet(true, false)) {
                        inFlightQueryStarted.countDown();
                        if (!allowInFlightQueryToFinish.await(3, TimeUnit.SECONDS)) {
                            throw new IOException("test did not release in-flight source request");
                        }
                    }
                    watermarks.advance(key, queryEnd, clock.instant());
                }, clock, Executors.newSingleThreadScheduledExecutor(), new java.util.Random(1),
                ignored -> { }, ignored -> { }, gate::collectionAllowed);
        ControlClient client = new ControlClient(config.control, HttpClient.newHttpClient());
        JobExecutor executor = new JobExecutor(config, jobStates, pauses, quotas);
        JobPoller poller = new JobPoller(config, client, executor, jobStates);

        try {
            // 等待期推进多个受控调度周期；控制轮询仍工作，采集函数和水位均不动。
            poller.start();
            eventually(() -> claims.get() >= 2);
            scheduler.pollOnce(CollectionSource.ARMS);
            clock.advance(Duration.ofSeconds(20));
            scheduler.pollOnce(CollectionSource.ARMS);
            scheduler.pollOnce(CollectionSource.ARMS);
            assertThat(cloudQueries).hasValue(0);
            assertThat(watermarks.snapshot()).isEmpty();
            assertThat(Files.exists(watermarkFile)).isFalse();
            assertThat(pauses.snapshot().configVersion()).isZero();
            assertThat(pauses.snapshot().globalPaused()).isTrue();

            // 同一 PVC/数据目录重启后，首次等待状态和版本 0 保持不变。
            ManualPauseStateStore waitingRestart = new ManualPauseStateStore(pauseFile, true, true);
            assertThat(waitingRestart.snapshot()).isEqualTo(pauses.snapshot());

            deliver("resume-1", "RESUME_COLLECTION", 1);
            eventually(() -> acceptedStatuses.contains("resume-1:SUCCEEDED"));
            Instant resumedQueryEnd = clock.instant().minusSeconds(config.scheduler.arms.visibilityDelaySeconds);
            scheduler.pollOnce(CollectionSource.ARMS);
            assertThat(cloudQueries).hasValue(1);
            Instant expectedInitialStart = resumedQueryEnd.minusSeconds(config.scheduler.arms.initialLookbackSeconds);
            assertThat(queryStarts).containsExactly(expectedInitialStart);
            String taskKey = "ARMS/orders";
            assertThat(watermarks.get(taskKey)).get().extracting(CollectionWatermarkStore.Watermark::windowEnd)
                    .isEqualTo(resumedQueryEnd);

            // 暂停只约束新窗口；一个已经进入云查询的调用按安全检查点完成，不承诺取消在途请求。
            clock.advance(Duration.ofSeconds(30));
            blockNextQuery.set(true);
            Thread inFlight = new Thread(() -> scheduler.pollOnce(CollectionSource.ARMS), "test-in-flight-query");
            inFlight.start();
            assertThat(inFlightQueryStarted.await(2, TimeUnit.SECONDS)).isTrue();
            Instant inFlightQueryEnd = clock.instant().minusSeconds(config.scheduler.arms.visibilityDelaySeconds);
            deliver("pause-2", "PAUSE_COLLECTION", 2);
            eventually(() -> acceptedStatuses.contains("pause-2:SUCCEEDED"));
            assertThat(cloudQueries).hasValue(2);
            assertThat(inFlight.isAlive()).isTrue();
            allowInFlightQueryToFinish.countDown();
            inFlight.join(2000);
            assertThat(inFlight.isAlive()).isFalse();
            ManualPauseStateStore pausedRestart = new ManualPauseStateStore(pauseFile, true, true);
            assertThat(pausedRestart.paused(CollectionSource.ARMS)).isTrue();
            scheduler.pollOnce(CollectionSource.ARMS);
            assertThat(cloudQueries).hasValue(2);
            assertThat(watermarks.get(taskKey)).get().extracting(CollectionWatermarkStore.Watermark::windowEnd)
                    .isEqualTo(inFlightQueryEnd);

            deliver("resume-3", "RESUME_COLLECTION", 3);
            eventually(() -> acceptedStatuses.contains("resume-3:SUCCEEDED"));
            ManualPauseStateStore runningRestart = new ManualPauseStateStore(pauseFile, true, true);
            assertThat(runningRestart.paused(CollectionSource.ARMS)).isFalse();
            scheduler.pollOnce(CollectionSource.ARMS);
            assertThat(cloudQueries).hasValue(3);
            // 已有水位优先；initialLookbackSeconds 不会在等待/暂停期间产生回看窗口。
            assertThat(queryStarts).containsExactly(expectedInitialStart, resumedQueryEnd, inFlightQueryEnd);
            assertThat(watermarks.get(taskKey)).get().satisfies(value ->
                    assertThat(value.windowEnd()).isAfter(resumedQueryEnd));
            assertThat(pauses.snapshot().configVersion()).isEqualTo(3);
            assertThat(jobStates.lastAppliedConfigVersion()).isEqualTo(3);
        } finally {
            poller.close();
            scheduler.close();
            quotas.close();
        }
    }

    private void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(ControlClient.CLAIM_PATH, this::claim);
        server.createContext("/collector/v1/jobs/", this::status);
        serverExecutor = Executors.newCachedThreadPool();
        server.setExecutor(serverExecutor);
        server.start();
    }

    private CollectorConfig config() {
        CollectorConfig config = new CollectorConfig();
        config.collector.collectorId = "collector-1";
        config.collector.siteId = "site-1";
        config.arms.enabled = true;
        config.scheduler.arms.pollIntervalSeconds = 5;
        config.scheduler.arms.visibilityDelaySeconds = 2;
        config.scheduler.arms.initialLookbackSeconds = 120;
        config.scheduler.jitterSeconds = 0;
        config.control.enabled = true;
        String base = "http://127.0.0.1:" + server.getAddress().getPort();
        config.control.taskClaimUrl = base + ControlClient.CLAIM_PATH;
        config.control.jobStatusUrl = base + "/collector/v1/jobs/{job_id}/status";
        config.control.connectTimeoutSeconds = 1;
        config.control.requestTimeoutSeconds = 2;
        config.control.taskPollIntervalSeconds = 1;
        config.control.taskPollJitterSeconds = 0;
        config.control.retryInitialDelayMillis = 20;
        config.control.retryMaxDelayMillis = 100;
        config.control.retryJitterRatio = 0;
        config.storage.outboxQuota.minBytes = 1024;
        config.storage.outboxQuota.defaultBytes = 1024 * 1024;
        config.storage.outboxQuota.maxBytes = 10L * 1024 * 1024;
        config.storage.outboxQuota.reservedFreeBytes = 1024;
        return config;
    }

    private void deliver(String id, String command, long version) {
        Instant now = Instant.now();
        jobsToDeliver.add("{\"schema_version\":\"1.0\",\"job_id\":\"" + id
                + "\",\"collector_id\":\"collector-1\",\"site_id\":\"site-1\",\"command_type\":\""
                + command + "\",\"config_version\":" + version + ",\"created_at\":\"" + now
                + "\",\"expires_at\":\"" + now.plusSeconds(60) + "\",\"parameters\":{}}");
    }

    private void claim(HttpExchange exchange) throws IOException {
        exchange.getRequestBody().readAllBytes();
        claims.incrementAndGet();
        String job = jobsToDeliver.poll();
        if (job == null) {
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        } else {
            send(exchange, 200, job);
        }
    }

    private void status(HttpExchange exchange) throws IOException {
        String request = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        String id = stringField(request, "job_id");
        String status = stringField(request, "status");
        acceptedStatuses.add(id + ":" + status);
        send(exchange, 200, "{\"schema_version\":\"1.0\",\"job_id\":\"" + id
                + "\",\"accepted_status\":\"" + status + "\",\"received_at\":\"" + Instant.now() + "\"}");
    }

    private String stringField(String json, String field) {
        String marker = "\"" + field + "\":\"";
        int start = json.indexOf(marker);
        if (start < 0) return "";
        start += marker.length();
        int end = json.indexOf('"', start);
        return end < 0 ? "" : json.substring(start, end);
    }

    private void send(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private void eventually(Checked condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        while (System.nanoTime() < deadline) {
            if (condition.test()) return;
            Thread.sleep(20);
        }
        throw new AssertionError("等待 HTTP 控制事件超时");
    }

    @FunctionalInterface private interface Checked { boolean test() throws Exception; }

    private static final class MutableClock extends Clock {
        private Instant instant;
        private MutableClock(Instant instant) { this.instant = instant; }
        synchronized void advance(Duration duration) { instant = instant.plus(duration); }
        @Override public synchronized Instant instant() { return instant; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
    }
}

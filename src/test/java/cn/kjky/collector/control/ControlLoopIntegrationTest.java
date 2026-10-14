package cn.kjky.collector.control;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.runtime.CollectionSource;
import cn.kjky.collector.runtime.OutboxQuotaManager;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ControlLoopIntegrationTest {
    @TempDir
    Path temp;

    private HttpServer server;
    private ExecutorService serverExecutor;
    private final AtomicBoolean claimUnavailable = new AtomicBoolean();
    private final AtomicBoolean statusUnavailable = new AtomicBoolean();
    private final AtomicBoolean includeUnknownField = new AtomicBoolean();
    private final AtomicInteger claims = new AtomicInteger();
    private final AtomicBoolean jobDelivered = new AtomicBoolean();
    private final List<String> reportedStatuses = new CopyOnWriteArrayList<>();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(ControlClient.CLAIM_PATH, this::claim);
        server.createContext("/collector/v1/jobs/", this::status);
        serverExecutor = Executors.newCachedThreadPool();
        server.setExecutor(serverExecutor);
        server.start();
    }

    @AfterEach
    void stopServer() {
        if (server != null) server.stop(0);
        if (serverExecutor != null) serverExecutor.shutdownNow();
    }

    @Test
    void clientAcceptsNoJobAndRejectsResponseOutsideFrozenContract() throws Exception {
        CollectorConfig config = config();
        try (ControlClient client = new ControlClient(config.control, HttpClient.newHttpClient())) {
            jobDelivered.set(true);
            assertThat(client.claim(claimRequest())).isEmpty();

            jobDelivered.set(false);
            includeUnknownField.set(true);
            assertThatThrownBy(() -> client.claim(claimRequest()))
                    .isInstanceOf(ControlClient.ControlException.class)
                    .hasMessageContaining("冻结契约");
        }
    }

    @Test
    void pollerSurvivesControlOutageAndRetriesLatestStatusIdempotentlyAfterRecovery() throws Exception {
        CollectorConfig config = config();
        Path outbox = Files.createDirectories(temp.resolve("outbox"));
        Path state = Files.createDirectories(temp.resolve("state"));
        config.storage.outboxQuota.minBytes = 1024;
        config.storage.outboxQuota.defaultBytes = 1024 * 1024;
        config.storage.outboxQuota.maxBytes = 10L * 1024 * 1024;
        config.storage.outboxQuota.reservedFreeBytes = 1024;
        JobStateStore jobs = new JobStateStore(state.resolve(JobStateStore.FILE_NAME));
        ManualPauseStateStore pauses = new ManualPauseStateStore(state.resolve(ManualPauseStateStore.FILE_NAME));

        claimUnavailable.set(true);
        statusUnavailable.set(true);
        try (OutboxQuotaManager quotas = new OutboxQuotaManager(config, outbox, state);
             ControlClient client = new ControlClient(config.control, HttpClient.newHttpClient());
             JobPoller poller = new JobPoller(config, client,
                     new JobExecutor(config, jobs, pauses, quotas), jobs)) {
            poller.start();
            eventually(() -> claims.get() > 0);
            claimUnavailable.set(false);
            eventually(() -> pauses.paused(CollectionSource.ARMS));
            assertThat(jobs.pendingReports()).isNotEmpty();

            statusUnavailable.set(false);
            eventually(() -> jobs.pendingReports().isEmpty());
            assertThat(reportedStatuses).contains("SUCCEEDED");
            assertThat(pauses.snapshot().configVersion()).isEqualTo(1);
        }
    }

    private CollectorConfig config() {
        CollectorConfig config = new CollectorConfig();
        config.collector.collectorId = "collector-1";
        config.collector.siteId = "site-1";
        String base = "http://127.0.0.1:" + server.getAddress().getPort();
        config.control.enabled = true;
        config.control.taskClaimUrl = base + ControlClient.CLAIM_PATH;
        config.control.jobStatusUrl = base + "/collector/v1/jobs/{job_id}/status";
        config.control.connectTimeoutSeconds = 1;
        config.control.requestTimeoutSeconds = 2;
        config.control.taskPollIntervalSeconds = 1;
        config.control.taskPollJitterSeconds = 0;
        config.control.retryInitialDelayMillis = 25;
        config.control.retryMaxDelayMillis = 100;
        config.control.retryJitterRatio = 0;
        return config;
    }

    private ControlProtocol.ClaimRequest claimRequest() {
        return new ControlProtocol.ClaimRequest("1.0", "collector-1", "site-1", "0.1.0", 0, null);
    }

    private void claim(HttpExchange exchange) throws IOException {
        exchange.getRequestBody().readAllBytes();
        claims.incrementAndGet();
        if (claimUnavailable.get()) {
            send(exchange, 503, "{}");
            return;
        }
        if (!jobDelivered.compareAndSet(false, true)) {
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
            return;
        }
        String unknown = includeUnknownField.get() ? ",\"unexpected\":true" : "";
        String body = "{" +
                "\"schema_version\":\"1.0\"," +
                "\"job_id\":\"pause-1\"," +
                "\"collector_id\":\"collector-1\"," +
                "\"site_id\":\"site-1\"," +
                "\"command_type\":\"PAUSE_COLLECTION\"," +
                "\"config_version\":1," +
                "\"created_at\":\"" + Instant.now().minusSeconds(1) + "\"," +
                "\"expires_at\":\"" + Instant.now().plusSeconds(120) + "\"," +
                "\"parameters\":{}" + unknown + "}";
        send(exchange, 200, body);
    }

    private void status(HttpExchange exchange) throws IOException {
        String request = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        String status = field(request, "status");
        if (status != null) reportedStatuses.add(status);
        if (statusUnavailable.get()) {
            send(exchange, 503, "{}");
            return;
        }
        String jobId = field(request, "job_id");
        String body = "{\"schema_version\":\"1.0\",\"job_id\":\"" + jobId
                + "\",\"accepted_status\":\"" + status
                + "\",\"received_at\":\"" + Instant.now() + "\"}";
        send(exchange, 200, body);
    }

    private String field(String json, String name) {
        String marker = "\"" + name + "\":\"";
        int start = json.indexOf(marker);
        if (start < 0) return null;
        start += marker.length();
        int end = json.indexOf('"', start);
        return end < 0 ? null : json.substring(start, end);
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
        Throwable last = null;
        while (System.nanoTime() < deadline) {
            try { if (condition.test()) return; }
            catch (Throwable e) { last = e; }
            Thread.sleep(20);
        }
        throw new AssertionError("条件在超时时间内未满足", last);
    }

    @FunctionalInterface
    private interface Checked { boolean test() throws Exception; }
}

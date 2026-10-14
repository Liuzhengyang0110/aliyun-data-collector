package cn.kjky.collector.control;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.contract.IngestJson;
import cn.kjky.collector.runtime.CollectionSource;
import cn.kjky.collector.runtime.OutboxQuotaManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobExecutorTest {
    private static final Instant NOW = Instant.parse("2026-09-14T01:00:00Z");
    private final ObjectMapper json = new ObjectMapper();

    @TempDir
    Path temp;

    @Test
    void duplicateJobIdDoesNotExecuteTwiceAndOutOfOrderVersionIsRejected() throws Exception {
        try (Fixture fixture = fixture("duplicate")) {
            List<ControlProtocol.StatusUpdate> reports = new ArrayList<>();
            ControlProtocol.Job pause = job("job-1", "PAUSE_COLLECTION", 2, json.createObjectNode());

            fixture.executor.execute(pause, reporter(reports));
            fixture.executor.execute(pause, reporter(reports));

            assertThat(fixture.pauses.snapshot().configVersion()).isEqualTo(2);
            assertThat(reports).extracting(ControlProtocol.StatusUpdate::status)
                    .containsExactly(ControlProtocol.Status.RECEIVED, ControlProtocol.Status.RUNNING,
                            ControlProtocol.Status.SUCCEEDED, ControlProtocol.Status.SUCCEEDED);

            ControlProtocol.StatusUpdate conflict = fixture.executor.execute(
                    job("job-1", "RESUME_COLLECTION", 2, json.createObjectNode()), reporter(reports));
            assertThat(conflict.status()).isEqualTo(ControlProtocol.Status.REJECTED);
            assertThat(conflict.rejectReason()).isEqualTo("JOB_ID_CONFLICT");

            ControlProtocol.StatusUpdate stale = fixture.executor.execute(
                    job("job-2", "RESUME_COLLECTION", 1, json.createObjectNode()), reporter(reports));
            assertThat(stale.status()).isEqualTo(ControlProtocol.Status.REJECTED);
            assertThat(stale.rejectReason()).isEqualTo("STALE_CONFIG_VERSION");
            assertThat(fixture.pauses.paused(CollectionSource.ARMS)).isTrue();
        }
    }

    @Test
    void rejectsExpiredUnknownIdentityMismatchAndUnapprovedParameters() throws Exception {
        try (Fixture fixture = fixture("rejections")) {
            ControlProtocol.Job expired = new ControlProtocol.Job("1.0", "job-expired", "collector-1", "site-1",
                    "PAUSE_COLLECTION", 1, NOW.minusSeconds(20), NOW.minusSeconds(1), json.createObjectNode());
            assertRejected(fixture, expired, "EXPIRED_JOB");
            assertRejected(fixture, job("job-unknown", "RUN_SHELL", 2, json.createObjectNode()), "UNKNOWN_COMMAND");

            ControlProtocol.Job mismatch = new ControlProtocol.Job("1.0", "job-wrong-site", "collector-1", "site-2",
                    "PAUSE_COLLECTION", 3, NOW.minusSeconds(1), NOW.plusSeconds(60), json.createObjectNode());
            assertRejected(fixture, mismatch, "IDENTITY_MISMATCH");

            ObjectNode forbidden = json.createObjectNode().put("url", "https://unapproved.invalid")
                    .put("access_key", "must-not-be-accepted");
            assertRejected(fixture, job("job-forbidden", "PAUSE_COLLECTION", 4, forbidden), "INVALID_PARAMETERS");
            assertThat(fixture.pauses.snapshot().configVersion()).isZero();
        }
    }

    @Test
    void collectionGateComposesManualLogicalPhysicalAndLocalConditions() throws Exception {
        try (Fixture fixture = fixture("gate")) {
            AtomicBoolean logical = new AtomicBoolean(true);
            AtomicBoolean physical = new AtomicBoolean(true);
            AtomicBoolean local = new AtomicBoolean(true);
            CollectionGate gate = new CollectionGate(fixture.pauses, logical::get, physical::get, local::get);

            fixture.executor.execute(job("pause", "PAUSE_COLLECTION", 1, json.createObjectNode()), ignored -> true);
            assertThat(gate.collectionAllowed(CollectionSource.ARMS)).isFalse();

            fixture.executor.execute(job("resume", "RESUME_COLLECTION", 2, json.createObjectNode()), ignored -> true);
            physical.set(false);
            assertThat(gate.collectionAllowed(CollectionSource.ARMS)).isFalse();
            physical.set(true);
            assertThat(gate.collectionAllowed(CollectionSource.ARMS)).isTrue();

            fixture.executor.execute(job("pause-again", "PAUSE_COLLECTION", 3, json.createObjectNode()), ignored -> true);
            physical.set(false);
            physical.set(true); // 容量恢复不能清除中控人工暂停。
            assertThat(gate.collectionAllowed(CollectionSource.ARMS)).isFalse();
            local.set(false);
            assertThat(gate.collectionAllowed(CollectionSource.SLS)).isFalse();
        }
    }

    @Test
    void sourcePauseDoesNotBlockOtherSourceAndResumeDoesNotBypassQuotaCritical() throws Exception {
        try (Fixture fixture = fixture("source-gate")) {
            AtomicBoolean logical = new AtomicBoolean(true);
            CollectionGate gate = new CollectionGate(fixture.pauses, logical::get, () -> true, () -> true);
            ObjectNode arms = json.createObjectNode().put("source_type", "ARMS");
            fixture.executor.execute(job("pause-arms", "PAUSE_COLLECTION", 1, arms), ignored -> true);
            assertThat(gate.collectionAllowed(CollectionSource.ARMS)).isFalse();
            assertThat(gate.collectionAllowed(CollectionSource.SLS)).isTrue();

            fixture.executor.execute(job("resume-arms", "RESUME_COLLECTION", 2, arms), ignored -> true);
            logical.set(false);
            assertThat(gate.collectionAllowed(CollectionSource.ARMS)).isFalse();
        }
    }

    @Test
    void appliesValidQuotaAndReportsEffectiveBoundsWhileRejectingOutOfRangeQuota() throws Exception {
        try (Fixture fixture = fixture("quota")) {
            ObjectNode valid = json.createObjectNode().put("requested_quota_bytes", 20L * 1024 * 1024);
            ControlProtocol.StatusUpdate applied = fixture.executor.execute(
                    job("quota-ok", "SET_OUTBOX_QUOTA", 1, valid), ignored -> true);
            assertThat(applied.status()).isEqualTo(ControlProtocol.Status.SUCCEEDED);
            assertThat(applied.requestedQuotaBytes()).isEqualTo(20L * 1024 * 1024);
            assertThat(applied.effectiveQuotaBytes()).isEqualTo(20L * 1024 * 1024);
            assertThat(applied.localMinBytes()).isEqualTo(1024L);
            assertThat(applied.localMaxBytes()).isEqualTo(100L * 1024 * 1024);
            assertThat(applied.configVersion()).isEqualTo(1);

            ObjectNode excessive = json.createObjectNode().put("requested_quota_bytes", 101L * 1024 * 1024);
            ControlProtocol.StatusUpdate rejected = fixture.executor.execute(
                    job("quota-high", "SET_OUTBOX_QUOTA", 2, excessive), ignored -> true);
            assertThat(rejected.status()).isEqualTo(ControlProtocol.Status.REJECTED);
            assertThat(rejected.rejectReason()).isEqualTo("QUOTA_ABOVE_LOCAL_MAX");
            assertThat(fixture.quotas.lastAppliedConfigVersion()).isEqualTo(1);
        }
    }

    @Test
    void runningJobRecoversFromPersistedSideEffectAfterCrash() throws Exception {
        try (Fixture fixture = fixture("crash")) {
            ControlProtocol.Job pause = job("job-crash", "PAUSE_COLLECTION", 5, json.createObjectNode());
            String fingerprint = IngestJson.sha256Canonical(pause);
            fixture.jobs.transition(new JobStateStore.JobRecord(pause.jobId(), fingerprint,
                    pause.collectorId(), pause.siteId(), pause.commandType(), pause.configVersion(),
                    ControlProtocol.Status.RUNNING, NOW, null, null, null, null,
                    null, null, false), false);
            fixture.pauses.pause(null, 5, "persisted-before-crash", NOW);

            JobStateStore restartedJobs = new JobStateStore(fixture.state.resolve(JobStateStore.FILE_NAME));
            ManualPauseStateStore restartedPauses = new ManualPauseStateStore(
                    fixture.state.resolve(ManualPauseStateStore.FILE_NAME));
            JobExecutor restarted = new JobExecutor(fixture.config, restartedJobs, restartedPauses,
                    fixture.quotas, Clock.fixed(NOW, ZoneOffset.UTC));
            ControlProtocol.StatusUpdate recovered = restarted.execute(pause, ignored -> true);

            assertThat(recovered.status()).isEqualTo(ControlProtocol.Status.SUCCEEDED);
            assertThat(restartedJobs.lastAppliedConfigVersion()).isEqualTo(5);
            assertThat(restartedPauses.snapshot().reason()).isEqualTo("persisted-before-crash");
        }
    }

    @Test
    void stateStoresFailClosedOnCorruptionOrAbandonedPartialFile() throws Exception {
        Path corrupt = temp.resolve("corrupt/control-jobs.json");
        Files.createDirectories(corrupt.getParent());
        Files.writeString(corrupt, "{broken");
        assertThatThrownBy(() -> new JobStateStore(corrupt))
                .isInstanceOf(IOException.class).hasMessageContaining("失败关闭");

        Path pause = temp.resolve("partial/manual-pause-state.json");
        Files.createDirectories(pause.getParent());
        Files.writeString(pause.resolveSibling(pause.getFileName() + ".partial"), "partial");
        assertThatThrownBy(() -> new ManualPauseStateStore(pause))
                .isInstanceOf(IOException.class).hasMessageContaining("失败关闭");
    }

    private void assertRejected(Fixture fixture, ControlProtocol.Job job, String reason) throws Exception {
        ControlProtocol.StatusUpdate update = fixture.executor.execute(job, ignored -> true);
        assertThat(update.status()).isEqualTo(ControlProtocol.Status.REJECTED);
        assertThat(update.rejectReason()).isEqualTo(reason);
    }

    private JobExecutor.StatusReporter reporter(List<ControlProtocol.StatusUpdate> reports) {
        return update -> { reports.add(update); return true; };
    }

    private ControlProtocol.Job job(String id, String command, long version, JsonNode parameters) {
        return new ControlProtocol.Job("1.0", id, "collector-1", "site-1", command, version,
                NOW.minusSeconds(1), NOW.plusSeconds(60), parameters);
    }

    private Fixture fixture(String name) throws Exception {
        Path root = temp.resolve(name);
        Path outbox = Files.createDirectories(root.resolve("outbox"));
        Path state = Files.createDirectories(root.resolve("state"));
        CollectorConfig config = new CollectorConfig();
        config.collector.collectorId = "collector-1";
        config.collector.siteId = "site-1";
        config.storage.outboxQuota.minBytes = 1024;
        config.storage.outboxQuota.defaultBytes = 10L * 1024 * 1024;
        config.storage.outboxQuota.maxBytes = 100L * 1024 * 1024;
        config.storage.outboxQuota.reservedFreeBytes = 1024;
        OutboxQuotaManager quotas = new OutboxQuotaManager(config, outbox, state);
        JobStateStore jobs = new JobStateStore(state.resolve(JobStateStore.FILE_NAME));
        ManualPauseStateStore pauses = new ManualPauseStateStore(state.resolve(ManualPauseStateStore.FILE_NAME));
        JobExecutor executor = new JobExecutor(config, jobs, pauses, quotas,
                Clock.fixed(NOW, ZoneOffset.UTC));
        return new Fixture(config, state, jobs, pauses, quotas, executor);
    }

    private record Fixture(CollectorConfig config, Path state, JobStateStore jobs, ManualPauseStateStore pauses,
                           OutboxQuotaManager quotas, JobExecutor executor) implements AutoCloseable {
        @Override public void close() { quotas.close(); }
    }
}

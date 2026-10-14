package cn.kjky.collector.runtime;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.outbox.OutboxSpaceGuard;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxQuotaManagerTest {
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-09-11T08:00:00Z"), ZoneOffset.UTC);

    @TempDir
    Path temp;

    @Test
    void startupScanCountsOnlyWritingPendingRetryAndPeriodicCheckCorrectsDrift() throws Exception {
        Path root = outbox("scan");
        write(root.resolve("writing/a.partial/batch.json"), 11);
        write(root.resolve("pending/b/delivery.json"), 13); // BLOCKED 等状态的投递元数据同样计入。
        write(root.resolve("retry/c/outbox.json"), 17);
        write(temp.resolve("scan/raw/ignored.json"), 101);
        write(temp.resolve("scan/manifest.json"), 103);
        write(temp.resolve("scan/state/watermark.json"), 107);

        try (OutboxQuotaManager manager = manager("scan", quota(1000, 100, 2000, 100),
                new MutableDisk(5000, 4000))) {
            assertThat(manager.snapshot().outboxUsedBytes()).isEqualTo(41);

            write(root.resolve("pending/b/batch.json"), 19);
            assertThat(manager.snapshot().outboxUsedBytes()).isEqualTo(41);
            assertThat(manager.checkNow().outboxUsedBytes()).isEqualTo(60);
        }
    }

    @Test
    void effectiveDefaultIsBoundedAndRemoteRequestsAreRejectedWithoutChangingPolicy() throws Exception {
        MutableDisk disk = new MutableDisk(700, 600);
        try (OutboxQuotaManager manager = manager("bounds", quota(650, 100, 1000, 100), disk)) {
            assertThat(manager.snapshot().effectiveQuotaBytes()).isEqualTo(600);

            assertThatThrownBy(() -> manager.applyRequestedQuota(99, 1))
                    .isInstanceOfSatisfying(QuotaPolicyRejectedException.class,
                            failure -> assertThat(failure.reason())
                                    .isEqualTo(QuotaPolicyRejectedException.Reason.QUOTA_BELOW_LOCAL_MIN));
            assertThatThrownBy(() -> manager.applyRequestedQuota(1001, 1))
                    .isInstanceOfSatisfying(QuotaPolicyRejectedException.class,
                            failure -> assertThat(failure.reason())
                                    .isEqualTo(QuotaPolicyRejectedException.Reason.QUOTA_ABOVE_LOCAL_MAX));
            assertThatThrownBy(() -> manager.applyRequestedQuota(650, 1))
                    .isInstanceOfSatisfying(QuotaPolicyRejectedException.class,
                            failure -> assertThat(failure.reason())
                                    .isEqualTo(QuotaPolicyRejectedException.Reason.PVC_SAFETY_LIMIT));
            assertThat(manager.lastAppliedConfigVersion()).isZero();
            assertThat(manager.snapshot().requestedQuotaBytes()).isEqualTo(650);
        }
    }

    @Test
    void policyBelowCurrentUsageIsCriticalAndUploadReleaseRecoversAtResumeWatermark() throws Exception {
        Path root = outbox("lower");
        Path retained = write(root.resolve("pending/batch-retained/batch.json"), 850);
        try (OutboxQuotaManager manager = manager("lower", quota(1000, 100, 2000, 100),
                new MutableDisk(5000, 4000))) {
            manager.applyRequestedQuota(900, 1);
            assertThat(manager.state()).isEqualTo(CapacityState.CRITICAL);
            assertThat(manager.collectionAllowed()).isFalse();
            assertThat(retained).exists();

            Files.write(retained, new byte[700]);
            manager.adjustUsage(-150);
            assertThat(manager.state()).isEqualTo(CapacityState.CRITICAL);

            Files.write(retained, new byte[600]);
            manager.adjustUsage(-100);
            assertThat(manager.state()).isEqualTo(CapacityState.RECOVERING);
            assertThat(manager.collectionAllowed()).isTrue();
            assertThat(retained).exists();
        }
    }

    @Test
    void concurrentReservationsNeverOversellHardQuota() throws Exception {
        try (OutboxQuotaManager manager = manager("concurrent", quota(1000, 100, 1000, 100),
                new MutableDisk(5000, 4000))) {
            var executor = Executors.newFixedThreadPool(20);
            CountDownLatch attempted = new CountDownLatch(20);
            CountDownLatch release = new CountDownLatch(1);
            AtomicInteger accepted = new AtomicInteger();
            List<Throwable> unexpected = java.util.Collections.synchronizedList(new ArrayList<>());
            for (int i = 0; i < 20; i++) {
                executor.submit(() -> {
                    try (OutboxSpaceGuard.Reservation reservation = manager.reserveNewBatch(100)) {
                        accepted.incrementAndGet();
                        attempted.countDown();
                        release.await(5, TimeUnit.SECONDS);
                    } catch (OutboxQuotaManager.OutboxQuotaExceededException expected) {
                        attempted.countDown();
                    } catch (Throwable failure) {
                        unexpected.add(failure);
                        attempted.countDown();
                    }
                });
            }
            assertThat(attempted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(accepted).hasValue(10);
            assertThat(manager.snapshot().reservedBytes()).isEqualTo(1000);
            assertThat(unexpected).isEmpty();
            release.countDown();
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(manager.snapshot().reservedBytes()).isZero();
        }
    }

    @Test
    void lastRemotePolicyAndCriticalHysteresisSurviveRestart() throws Exception {
        Path root = outbox("restart");
        Path payload = write(root.resolve("pending/batch-a/batch.json"), 850);
        CollectorConfig.OutboxQuotaConfig quota = quota(1000, 100, 2000, 100);
        MutableDisk disk = new MutableDisk(5000, 4000);
        try (OutboxQuotaManager first = manager("restart", quota, disk)) {
            first.applyRequestedQuota(900, 7);
            assertThat(first.state()).isEqualTo(CapacityState.CRITICAL);
            Files.write(payload, new byte[700]);
            first.adjustUsage(-150);
            assertThat(first.state()).isEqualTo(CapacityState.CRITICAL);
        }

        try (OutboxQuotaManager restarted = manager("restart", quota, disk)) {
            assertThat(restarted.lastAppliedConfigVersion()).isEqualTo(7);
            assertThat(restarted.snapshot().requestedQuotaBytes()).isEqualTo(900);
            assertThat(restarted.snapshot().outboxUsedBytes()).isEqualTo(700);
            assertThat(restarted.state()).isEqualTo(CapacityState.CRITICAL);
        }
    }

    @Test
    void corruptedPolicyOrPartialFailsClosedAtConstruction() throws Exception {
        Path root = outbox("corrupt");
        Path state = Files.createDirectories(temp.resolve("corrupt/state"));
        Files.writeString(state.resolve(RuntimePolicyStore.FILE_NAME), "{not-json");
        assertThatThrownBy(() -> new RuntimePolicyStore(state.resolve(RuntimePolicyStore.FILE_NAME)))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("损坏");

        Files.delete(state.resolve(RuntimePolicyStore.FILE_NAME));
        Files.writeString(state.resolve(RuntimePolicyStore.FILE_NAME + ".partial"), "partial");
        assertThatThrownBy(() -> new RuntimePolicyStore(state.resolve(RuntimePolicyStore.FILE_NAME)))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("失败关闭");
        assertThat(root).isDirectory();
    }

    @Test
    void physicalSafetyBelowLocalMinimumIsCriticalAndCannotBeBypassedByQuotaIncrease() throws Exception {
        try (OutboxQuotaManager logical = manager("physical-min", quota(600, 500, 1000, 100),
                new MutableDisk(550, 50))) {
            assertThat(logical.snapshot().effectiveQuotaBytes()).isEqualTo(450);
            assertThat(logical.state()).isEqualTo(CapacityState.CRITICAL);
            assertThat(logical.collectionAllowed()).isFalse();
            assertThatThrownBy(() -> logical.applyRequestedQuota(500, 1))
                    .isInstanceOfSatisfying(QuotaPolicyRejectedException.class,
                            failure -> assertThat(failure.reason())
                                    .isEqualTo(QuotaPolicyRejectedException.Reason.PVC_SAFETY_LIMIT));
        }
    }

    @Test
    void physicalAndLogicalCriticalAreIndependentAndRaisingQuotaCannotBypassPhysicalCritical() throws Exception {
        Path root = outbox("double-critical");
        write(root.resolve("pending/batch-a/batch.json"), 950);
        CollectorConfig.OutboxQuotaConfig logicalConfig = quota(1000, 100, 2000, 100);
        try (OutboxQuotaManager logical = manager("double-critical", logicalConfig,
                new MutableDisk(5000, 4000))) {
            CollectorConfig.StorageConfig physicalConfig = new CollectorConfig.StorageConfig();
            physicalConfig.diskWarningWatermarkPercent = 80;
            physicalConfig.diskCriticalWatermarkPercent = 90;
            physicalConfig.diskResumeWatermarkPercent = 75;
            CapacityMonitor physical = new CapacityMonitor(physicalConfig, root,
                    new CapacityStateStore(temp.resolve("double-critical/state/capacity-state.json")),
                    new MutableDisk(5000, 100), CLOCK, Executors.newSingleThreadScheduledExecutor(),
                    ignored -> { }, ignored -> { });
            try (physical) {
                physical.checkNow();
                assertThat(logical.state()).isEqualTo(CapacityState.CRITICAL);
                assertThat(physical.state()).isEqualTo(CapacityState.CRITICAL);

                logical.applyRequestedQuota(2000, 1);
                assertThat(logical.collectionAllowed()).isTrue();
                assertThat(physical.collectionAllowed()).isFalse();
                assertThat(physical.collectionAllowed() && logical.collectionAllowed()).isFalse();
            }
        }
    }

    private OutboxQuotaManager manager(String name, CollectorConfig.OutboxQuotaConfig quota,
                                       MutableDisk disk) throws Exception {
        Path outbox = outbox(name);
        Path state = Files.createDirectories(temp.resolve(name).resolve("state"));
        return new OutboxQuotaManager(quota, outbox,
                new RuntimePolicyStore(state.resolve(RuntimePolicyStore.FILE_NAME)),
                new CapacityStateStore(state.resolve(OutboxQuotaManager.STATE_FILE_NAME)),
                disk, new OutboxUsageProbe(), CLOCK, Executors.newSingleThreadScheduledExecutor(),
                ignored -> { }, ignored -> { });
    }

    private Path outbox(String name) throws Exception {
        Path root = Files.createDirectories(temp.resolve(name).resolve("outbox"));
        Files.createDirectories(root.resolve("writing"));
        Files.createDirectories(root.resolve("pending"));
        Files.createDirectories(root.resolve("retry"));
        return root;
    }

    private Path write(Path path, int bytes) throws Exception {
        Files.createDirectories(path.getParent());
        return Files.write(path, new byte[bytes]);
    }

    private CollectorConfig.OutboxQuotaConfig quota(long defaultBytes, long minBytes,
                                                     long maxBytes, long reservedBytes) {
        CollectorConfig.OutboxQuotaConfig quota = new CollectorConfig.OutboxQuotaConfig();
        quota.defaultBytes = defaultBytes;
        quota.minBytes = minBytes;
        quota.maxBytes = maxBytes;
        quota.reservedFreeBytes = reservedBytes;
        quota.warningPercent = 80;
        quota.criticalPercent = 90;
        quota.resumePercent = 75;
        quota.checkIntervalSeconds = 10;
        return quota;
    }

    private static final class MutableDisk implements DiskUsageProbe {
        private final long total;
        private final long usable;

        private MutableDisk(long total, long usable) {
            this.total = total;
            this.usable = usable;
        }

        @Override
        public Usage measure(Path path) {
            return new Usage(total, usable);
        }
    }
}

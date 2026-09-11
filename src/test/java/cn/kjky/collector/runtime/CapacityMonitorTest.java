package cn.kjky.collector.runtime;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.service.CollectorEngine;
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
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class CapacityMonitorTest {
    private static final Instant NOW = Instant.parse("2026-09-04T02:00:00Z");

    @TempDir
    Path temp;

    @Test
    void appliesWarningCriticalAndResumeHysteresis() throws Exception {
        MutableProbe probe = new MutableProbe(50);
        List<String> warnings = new ArrayList<>();
        try (CapacityMonitor monitor = monitor("hysteresis", probe, warnings)) {
            assertThat(monitor.checkNow().state()).isEqualTo(CapacityState.NORMAL);
            assertThat(monitor.collectionAllowed()).isTrue();

            probe.usedPercent.set(80);
            assertThat(monitor.checkNow().state()).isEqualTo(CapacityState.WARNING);
            assertThat(monitor.collectionAllowed()).isTrue();

            probe.usedPercent.set(79);
            assertThat(monitor.checkNow().state()).isEqualTo(CapacityState.WARNING);

            probe.usedPercent.set(75);
            assertThat(monitor.checkNow().state()).isEqualTo(CapacityState.NORMAL);

            probe.usedPercent.set(90);
            assertThat(monitor.checkNow().state()).isEqualTo(CapacityState.CRITICAL);
            assertThat(monitor.collectionAllowed()).isFalse();

            probe.usedPercent.set(89);
            assertThat(monitor.checkNow().state()).isEqualTo(CapacityState.CRITICAL);

            probe.usedPercent.set(75);
            assertThat(monitor.checkNow().state()).isEqualTo(CapacityState.RECOVERING);
            assertThat(monitor.collectionAllowed()).isTrue();

            probe.usedPercent.set(76);
            assertThat(monitor.checkNow().state()).isEqualTo(CapacityState.RECOVERING);

            probe.usedPercent.set(75);
            assertThat(monitor.checkNow().state()).isEqualTo(CapacityState.NORMAL);
        }

        assertThat(warnings).anyMatch(message -> message.contains("WARNING"))
                .anyMatch(message -> message.contains("CRITICAL"));
    }

    @Test
    void restoresCriticalStateAfterRestartAndOnlyResumesAtResumeThreshold() throws Exception {
        Path root = Files.createDirectories(temp.resolve("restart/outbox"));
        Path stateFile = temp.resolve("restart/state").resolve(CapacityStateStore.FILE_NAME);
        MutableProbe probe = new MutableProbe(95);

        try (CapacityMonitor first = monitor(root, stateFile, probe, new ArrayList<>())) {
            assertThat(first.checkNow().state()).isEqualTo(CapacityState.CRITICAL);
        }

        probe.usedPercent.set(85);
        try (CapacityMonitor restarted = monitor(root, stateFile, probe, new ArrayList<>())) {
            assertThat(restarted.state()).isEqualTo(CapacityState.CRITICAL);
            assertThat(restarted.collectionAllowed()).isFalse();
            assertThat(restarted.checkNow().state()).isEqualTo(CapacityState.CRITICAL);

            probe.usedPercent.set(75);
            assertThat(restarted.checkNow().state()).isEqualTo(CapacityState.RECOVERING);
            assertThat(restarted.collectionAllowed()).isTrue();
        }

        CapacityStateStore reopened = new CapacityStateStore(stateFile);
        assertThat(reopened.load()).get()
                .extracting(CapacityStateStore.CapacitySnapshot::state)
                .isEqualTo(CapacityState.RECOVERING);
    }

    @Test
    void capacityProtectionNeverDeletesPendingRetryOrBlockedFiles() throws Exception {
        Path outbox = Files.createDirectories(temp.resolve("preserve/outbox"));
        Path pending = Files.createDirectories(outbox.resolve("pending/batch-pending"));
        Path blocked = Files.createDirectories(outbox.resolve("pending/batch-blocked"));
        Path retry = Files.createDirectories(outbox.resolve("retry/batch-retry"));
        Files.writeString(pending.resolve("batch.json"), "pending");
        Files.writeString(blocked.resolve("delivery.json"), "BLOCKED");
        Files.writeString(retry.resolve("batch.json"), "retry");

        try (CapacityMonitor monitor = monitor(outbox,
                temp.resolve("preserve/state/capacity-state.json"),
                new MutableProbe(99), new ArrayList<>())) {
            assertThat(monitor.checkNow().state()).isEqualTo(CapacityState.CRITICAL);
        }

        assertThat(pending.resolve("batch.json")).exists();
        assertThat(blocked.resolve("delivery.json")).exists();
        assertThat(retry.resolve("batch.json")).exists();
    }

    @Test
    void probeFailureFailsClosedWithoutDeletingData() throws Exception {
        Path outbox = Files.createDirectories(temp.resolve("probe-failure/outbox"));
        Path retained = Files.writeString(outbox.resolve("retained.batch"), "keep");
        CollectorConfig.StorageConfig storage = storage();
        CapacityStateStore store = new CapacityStateStore(
                temp.resolve("probe-failure/state/capacity-state.json"));
        DiskUsageProbe failing = ignored -> { throw new IOException("unavailable"); };
        CapacityMonitor monitor = new CapacityMonitor(storage, outbox, store, failing,
                Clock.fixed(NOW, ZoneOffset.UTC), Executors.newSingleThreadScheduledExecutor(),
                ignored -> { }, ignored -> { });

        try (monitor) {
            monitor.start();
            assertThat(monitor.state()).isEqualTo(CapacityState.CRITICAL);
            assertThat(monitor.collectionAllowed()).isFalse();
        }
        assertThat(retained).exists();
    }

    @Test
    void developmentRawDaemonDoesNotEnableProductionCapacityProtection() throws Exception {
        CollectorConfig config = new CollectorConfig();
        config.storage.dataRoot = temp.resolve("development").toString();
        config.runtime.deploymentMode = "DEVELOPMENT";
        config.storage.rawPersistence.enabled = true;
        config.arms.enabled = false;
        config.sls.enabled = false;

        try (CollectorDaemon daemon = new CollectorDaemon(config, new CollectorEngine(config))) {
            assertThat(daemon.capacityState()).isNull();
        }
    }

    private CapacityMonitor monitor(String name, MutableProbe probe, List<String> warnings) throws Exception {
        Path root = Files.createDirectories(temp.resolve(name).resolve("outbox"));
        return monitor(root, temp.resolve(name).resolve("state/capacity-state.json"), probe, warnings);
    }

    private CapacityMonitor monitor(Path root, Path stateFile, DiskUsageProbe probe,
                                    List<String> warnings) throws Exception {
        return new CapacityMonitor(storage(), root, new CapacityStateStore(stateFile), probe,
                Clock.fixed(NOW, ZoneOffset.UTC), Executors.newSingleThreadScheduledExecutor(),
                ignored -> { }, warnings::add);
    }

    private CollectorConfig.StorageConfig storage() {
        CollectorConfig.StorageConfig storage = new CollectorConfig.StorageConfig();
        storage.diskResumeWatermarkPercent = 75;
        storage.diskWarningWatermarkPercent = 80;
        storage.diskCriticalWatermarkPercent = 90;
        storage.diskCheckIntervalSeconds = 10;
        return storage;
    }

    private static final class MutableProbe implements DiskUsageProbe {
        private final AtomicInteger usedPercent;

        private MutableProbe(int initialUsedPercent) {
            this.usedPercent = new AtomicInteger(initialUsedPercent);
        }

        @Override
        public Usage measure(Path path) {
            int used = usedPercent.get();
            return new Usage(100, 100 - used);
        }
    }
}

package cn.kjky.collector.runtime;

import cn.kjky.collector.config.CollectorConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ProductionPollSchedulerCapacityTest {
    @TempDir
    Path temp;

    @Test
    void criticalSkipsNewQueryWindowAndResumeAllowsNextWindow() throws Exception {
        CollectorConfig config = config();
        AtomicInteger used = new AtomicInteger(95);
        CapacityMonitor capacity = monitor(config, used);
        capacity.checkNow();
        AtomicInteger collections = new AtomicInteger();
        List<String> info = new ArrayList<>();

        try (capacity; ProductionPollScheduler scheduler = new ProductionPollScheduler(config,
                (source, queryEnd, lookback) -> collections.incrementAndGet(),
                Clock.fixed(Instant.parse("2026-09-04T03:00:00Z"), ZoneOffset.UTC),
                Executors.newSingleThreadScheduledExecutor(), new Random(1),
                info::add, ignored -> { }, capacity::collectionAllowed)) {
            scheduler.pollOnce(CollectionSource.ARMS);
            assertThat(collections).hasValue(0);
            assertThat(info).anyMatch(message -> message.contains("CRITICAL") && message.contains("暂停"));

            used.set(85);
            capacity.checkNow();
            scheduler.pollOnce(CollectionSource.ARMS);
            assertThat(collections).hasValue(0);

            used.set(75);
            assertThat(capacity.checkNow().state()).isEqualTo(CapacityState.RECOVERING);
            scheduler.pollOnce(CollectionSource.ARMS);
            assertThat(collections).hasValue(1);
        }
    }

    private CapacityMonitor monitor(CollectorConfig config, AtomicInteger used) throws Exception {
        Path outbox = Files.createDirectories(temp.resolve("outbox"));
        CapacityStateStore store = new CapacityStateStore(temp.resolve("state/capacity-state.json"));
        DiskUsageProbe probe = ignored -> new DiskUsageProbe.Usage(100, 100 - used.get());
        return new CapacityMonitor(config.storage, outbox, store, probe,
                Clock.fixed(Instant.parse("2026-09-04T03:00:00Z"), ZoneOffset.UTC),
                Executors.newSingleThreadScheduledExecutor(), ignored -> { }, ignored -> { });
    }

    private CollectorConfig config() {
        CollectorConfig config = new CollectorConfig();
        config.runtime.deploymentMode = "PRODUCTION";
        config.storage.rawPersistence.enabled = false;
        config.storage.diskResumeWatermarkPercent = 75;
        config.storage.diskWarningWatermarkPercent = 80;
        config.storage.diskCriticalWatermarkPercent = 90;
        config.storage.diskCheckIntervalSeconds = 10;
        config.arms.enabled = true;
        config.sls.enabled = false;
        config.scheduler.jitterSeconds = 0;
        config.scheduler.shutdownGraceSeconds = 1;
        return config;
    }
}

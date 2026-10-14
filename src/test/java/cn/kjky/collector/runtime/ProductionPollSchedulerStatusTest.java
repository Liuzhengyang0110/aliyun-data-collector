package cn.kjky.collector.runtime;

import cn.kjky.collector.config.CollectorConfig;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Random;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

class ProductionPollSchedulerStatusTest {
    @Test
    void exposesThreadSafeSourceSuccessFailureAndConsecutiveFailureState() {
        CollectorConfig config = new CollectorConfig();
        config.arms.enabled = true;
        config.sls.enabled = true;
        Instant now = Instant.parse("2026-09-14T03:00:00Z");
        ProductionPollScheduler scheduler = new ProductionPollScheduler(config,
                (source, end, lookback) -> {
                    if (source == CollectionSource.ARMS) throw new java.io.IOException("mock failure");
                }, Clock.fixed(now, ZoneOffset.UTC), Executors.newScheduledThreadPool(2),
                new Random(1), value -> { }, value -> { }, source -> true);
        try {
            scheduler.pollOnce(CollectionSource.ARMS);
            scheduler.pollOnce(CollectionSource.ARMS);
            scheduler.pollOnce(CollectionSource.SLS);

            var snapshot = scheduler.runtimeStatusSnapshot();
            assertThat(snapshot.get(CollectionSource.ARMS).lastFailureAt()).isEqualTo(now);
            assertThat(snapshot.get(CollectionSource.ARMS).consecutiveFailures()).isEqualTo(2);
            assertThat(snapshot.get(CollectionSource.ARMS).running()).isFalse();
            assertThat(snapshot.get(CollectionSource.SLS).lastSuccessAt()).isEqualTo(now);
            assertThat(snapshot.get(CollectionSource.SLS).consecutiveFailures()).isZero();
            assertThat(snapshot.get(CollectionSource.SLS).running()).isFalse();
        } finally {
            scheduler.close();
        }
    }
}

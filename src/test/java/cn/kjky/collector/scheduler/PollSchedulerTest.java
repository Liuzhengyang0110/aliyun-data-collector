package cn.kjky.collector.scheduler;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.runtime.CollectionSource;
import cn.kjky.collector.runtime.PollingWatermarkStore;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class PollSchedulerTest {
    @Test
    void appliesVisibilityDelayAndContinuesFromSuccessfulWatermark() throws Exception {
        CollectorConfig config = config();
        config.scheduler.arms.visibilityDelaySeconds = 90;
        config.scheduler.arms.initialLookbackSeconds = 300;
        Instant now = Instant.parse("2026-08-27T02:00:00Z");
        List<Window> calls = new ArrayList<>();
        PollingWatermarkStore store = store();

        try (PollScheduler scheduler = scheduler(config, store, now,
                (source, start, end) -> calls.add(new Window(source, start, end)))) {
            scheduler.pollOnce(CollectionSource.ARMS);
        }

        Instant expectedEnd = now.minusSeconds(90);
        assertThat(calls).containsExactly(new Window(CollectionSource.ARMS,
                expectedEnd.minusSeconds(300), expectedEnd));
        assertThat(store.get(CollectionSource.ARMS)).contains(expectedEnd);
    }

    @Test
    void failedRoundDoesNotAdvanceWatermarkOrEscapeScheduler() throws Exception {
        CollectorConfig config = config();
        Instant now = Instant.parse("2026-08-27T02:00:00Z");
        AtomicInteger attempts = new AtomicInteger();
        PollingWatermarkStore store = store();

        try (PollScheduler scheduler = scheduler(config, store, now, (source, start, end) -> {
            if (attempts.incrementAndGet() == 1) throw new IllegalStateException("temporary failure");
        })) {
            scheduler.pollOnce(CollectionSource.ARMS);
            assertThat(store.get(CollectionSource.ARMS)).isEmpty();
            scheduler.pollOnce(CollectionSource.ARMS);
        }

        assertThat(attempts).hasValue(2);
        assertThat(store.get(CollectionSource.ARMS)).contains(now.minusSeconds(90));
    }

    @Test
    void doesNotMoveBackwardWhenClockIsBehindPersistedWatermark() throws Exception {
        CollectorConfig config = config();
        Instant watermark = Instant.parse("2026-08-27T02:00:00Z");
        PollingWatermarkStore store = store();
        store.advance(CollectionSource.ARMS, watermark);
        AtomicInteger calls = new AtomicInteger();

        try (PollScheduler scheduler = scheduler(config, store, watermark,
                (source, start, end) -> calls.incrementAndGet())) {
            scheduler.pollOnce(CollectionSource.ARMS);
        }

        assertThat(calls).hasValue(0);
        assertThat(store.get(CollectionSource.ARMS)).contains(watermark);
    }

    @Test
    void schedulesAnotherRoundAfterOneRoundFails() throws Exception {
        CollectorConfig config = config();
        config.scheduler.arms.pollIntervalSeconds = 1;
        Instant now = Instant.parse("2026-08-27T02:00:00Z");
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch twoRounds = new CountDownLatch(2);
        PollingWatermarkStore store = store();
        var executor = Executors.newSingleThreadScheduledExecutor();

        try (PollScheduler scheduler = new PollScheduler(config, (source, start, end) -> {
            int attempt = attempts.incrementAndGet();
            twoRounds.countDown();
            if (attempt == 1) throw new IllegalStateException("temporary failure");
        }, store, Clock.fixed(now, ZoneOffset.UTC), executor, new Random(1), value -> {}, value -> {})) {
            scheduler.start();
            assertThat(twoRounds.await(3, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(attempts.get()).isGreaterThanOrEqualTo(2);
        assertThat(store.get(CollectionSource.ARMS)).contains(now.minusSeconds(90));
    }

    @Test
    void firstFailedWindowKeepsItsStartAfterRestartAndClockAdvance() throws Exception {
        CollectorConfig config = config();
        Instant firstNow = Instant.parse("2026-08-27T02:00:00Z");
        Instant laterNow = firstNow.plusSeconds(30);
        PollingWatermarkStore store = store();

        try (PollScheduler first = scheduler(config, store, firstNow,
                (source, start, end) -> { throw new IllegalStateException("temporary failure"); })) {
            first.pollOnce(CollectionSource.ARMS);
        }

        List<Window> retried = new ArrayList<>();
        try (PollScheduler restarted = scheduler(config, store, laterNow,
                (source, start, end) -> retried.add(new Window(source, start, end)))) {
            restarted.pollOnce(CollectionSource.ARMS);
        }

        Instant firstEnd = firstNow.minusSeconds(90);
        assertThat(retried).containsExactly(new Window(CollectionSource.ARMS,
                firstEnd.minusSeconds(300), laterNow.minusSeconds(90)));
    }

    private PollScheduler scheduler(CollectorConfig config, PollingWatermarkStore store, Instant now,
                                    PollScheduler.SourceCollector collector) {
        return new PollScheduler(config, collector, store, Clock.fixed(now, ZoneOffset.UTC),
                Executors.newSingleThreadScheduledExecutor(), new Random(1), value -> {}, value -> {});
    }

    private PollingWatermarkStore store() throws Exception {
        Path root = Path.of("target/test-runtime", UUID.randomUUID().toString());
        return new PollingWatermarkStore(root.resolve("poll-watermarks.json"));
    }

    private CollectorConfig config() {
        CollectorConfig config = new CollectorConfig();
        config.arms.enabled = true;
        config.sls.enabled = false;
        config.scheduler.jitterSeconds = 0;
        config.scheduler.shutdownGraceSeconds = 1;
        return config;
    }

    private record Window(CollectionSource source, Instant start, Instant end) {}
}

package cn.kjky.collector.runtime;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PollingWatermarkStoreTest {
    @Test
    void persistsOnlyForwardProgressAndCanBeReopened() throws Exception {
        Path root = Path.of("target/test-runtime", UUID.randomUUID().toString());
        Path file = root.resolve("checkpoints/poll-watermarks.json");
        Instant first = Instant.parse("2026-08-27T01:00:00Z");
        Instant later = first.plusSeconds(30);

        PollingWatermarkStore store = new PollingWatermarkStore(file);
        assertThat(store.startFor(CollectionSource.ARMS, first.minusSeconds(60)))
                .isEqualTo(first.minusSeconds(60));
        store.advance(CollectionSource.ARMS, first);
        store.advance(CollectionSource.ARMS, first.minusSeconds(30));
        store.advance(CollectionSource.SLS, later);

        PollingWatermarkStore reopened = new PollingWatermarkStore(file);
        assertThat(reopened.get(CollectionSource.ARMS)).contains(first);
        assertThat(reopened.get(CollectionSource.SLS)).contains(later);
        assertThat(Files.exists(file.resolveSibling("poll-watermarks.json.partial"))).isFalse();
    }

    @Test
    void keepsInitialStartAcrossRestartUntilFirstRoundSucceeds() throws Exception {
        Path root = Path.of("target/test-runtime", UUID.randomUUID().toString());
        Path file = root.resolve("checkpoints/poll-watermarks.json");
        Instant original = Instant.parse("2026-08-27T01:00:00Z");

        PollingWatermarkStore firstProcess = new PollingWatermarkStore(file);
        assertThat(firstProcess.startFor(CollectionSource.SLS, original)).isEqualTo(original);

        PollingWatermarkStore restarted = new PollingWatermarkStore(file);
        assertThat(restarted.startFor(CollectionSource.SLS, original.plusSeconds(30))).isEqualTo(original);
        assertThat(restarted.get(CollectionSource.SLS)).isEmpty();
    }
}

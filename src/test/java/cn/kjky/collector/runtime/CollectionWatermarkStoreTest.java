package cn.kjky.collector.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** P0-04 生产任务级采集水位的原子性、独立性和失败关闭测试。 */
class CollectionWatermarkStoreTest {
    private static final Instant NOW = Instant.parse("2026-09-03T08:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    @TempDir Path temp;

    @Test
    void persistsIndependentTaskWatermarksAndAnchorsFirstStart() throws Exception {
        Path file = temp.resolve("state/collection-watermarks.json");
        CollectionWatermarkStore store = new CollectionWatermarkStore(file, CLOCK);
        String trace = "ARMS/TRACE/orders";
        String logstore = "SLS/LOG/payment/access";

        assertThat(store.startFor(trace, NOW.minusSeconds(300))).isEqualTo(NOW.minusSeconds(300));
        assertThat(store.startFor(trace, NOW.minusSeconds(60))).isEqualTo(NOW.minusSeconds(300));
        store.advance(trace, NOW.minusSeconds(30));
        store.advance(logstore, NOW.minusSeconds(10));

        CollectionWatermarkStore restarted = new CollectionWatermarkStore(file, CLOCK);
        assertThat(restarted.get(trace)).get().satisfies(value -> {
            assertThat(value.taskKey()).isEqualTo(trace);
            assertThat(value.windowEnd()).isEqualTo(NOW.minusSeconds(30));
            assertThat(value.lastSuccessAt()).isEqualTo(NOW);
        });
        assertThat(restarted.get(logstore)).get().extracting(CollectionWatermarkStore.Watermark::windowEnd)
                .isEqualTo(NOW.minusSeconds(10));
        assertThat(restarted.snapshot()).hasSize(2);
        assertThat(Files.exists(file.resolveSibling("collection-watermarks.json.partial"))).isFalse();
    }

    @Test
    void corruptOrUnfinishedWatermarkFileFailsClosed() throws Exception {
        Path file = temp.resolve("state/collection-watermarks.json");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "{not-json", StandardCharsets.UTF_8);

        assertThatThrownBy(() -> new CollectionWatermarkStore(file, CLOCK))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("采集水位文件损坏");
        assertThat(file).isRegularFile();

        Files.delete(file);
        Files.writeString(file.resolveSibling("collection-watermarks.json.partial"), "partial", StandardCharsets.UTF_8);
        assertThatThrownBy(() -> new CollectionWatermarkStore(file, CLOCK))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("未完成的采集水位临时文件");
    }
}

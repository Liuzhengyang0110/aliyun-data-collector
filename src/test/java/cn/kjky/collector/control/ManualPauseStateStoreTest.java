package cn.kjky.collector.control;

import cn.kjky.collector.runtime.CollectionSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ManualPauseStateStoreTest {
    @TempDir
    Path temp;

    @Test
    void firstManualStartPersistsGlobalPauseAtVersionZeroAndRestoresIt() throws Exception {
        Path file = temp.resolve("state/manual-pause-state.json");

        ManualPauseStateStore first = new ManualPauseStateStore(file, true, false);

        assertThat(first.snapshot().globalPaused()).isTrue();
        assertThat(first.snapshot().configVersion()).isZero();
        assertThat(first.paused(CollectionSource.ARMS)).isTrue();
        assertThat(Files.exists(file)).isTrue();
        assertThat(Files.exists(file.resolveSibling(file.getFileName() + ".partial"))).isFalse();

        ManualPauseStateStore restarted = new ManualPauseStateStore(file, true, true);
        assertThat(restarted.snapshot()).isEqualTo(first.snapshot());
    }

    @Test
    void ordinaryFirstStartRemainsUnpausedAndExistingValidStateIsPreserved() throws Exception {
        Path file = temp.resolve("ordinary/manual-pause-state.json");
        ManualPauseStateStore ordinary = new ManualPauseStateStore(file);
        assertThat(ordinary.snapshot().globalPaused()).isFalse();
        assertThat(Files.exists(file)).isFalse();

        ManualPauseStateStore initialized = new ManualPauseStateStore(file, true, false);
        var resumed = initialized.resume(null, 4, Instant.parse("2026-10-14T00:00:00Z"));
        ManualPauseStateStore restored = new ManualPauseStateStore(file, true, true);
        assertThat(restored.snapshot()).isEqualTo(resumed);
        assertThat(restored.snapshot().configVersion()).isEqualTo(4);
        assertThat(restored.snapshot().globalPaused()).isFalse();
    }

    @Test
    void missingPauseStateWithLegacyFilesFailsClosedWithoutCreatingVersionZero() throws Exception {
        Path state = Files.createDirectories(temp.resolve("legacy/state"));
        Path file = state.resolve(ManualPauseStateStore.FILE_NAME);
        Files.writeString(state.resolve("control-jobs.json"), "legacy task ledger");

        assertThatThrownBy(() -> new ManualPauseStateStore(file, true, true))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("旧任务/策略/采集状态")
                .hasMessageContaining("对账");
        assertThat(Files.exists(file)).isFalse();
    }

    @Test
    void partialAndCorruptPauseStateFailClosedEvenForManualStart() throws Exception {
        Path partial = temp.resolve("partial/state/manual-pause-state.json");
        Files.createDirectories(partial.getParent());
        Files.writeString(partial.resolveSibling(partial.getFileName() + ".partial"), "unfinished");
        assertThatThrownBy(() -> new ManualPauseStateStore(partial, true, false))
                .isInstanceOf(IOException.class).hasMessageContaining("失败关闭");

        Path corrupt = temp.resolve("corrupt/state/manual-pause-state.json");
        Files.createDirectories(corrupt.getParent());
        Files.writeString(corrupt, "{broken");
        assertThatThrownBy(() -> new ManualPauseStateStore(corrupt, true, false))
                .isInstanceOf(IOException.class).hasMessageContaining("损坏");
    }
}

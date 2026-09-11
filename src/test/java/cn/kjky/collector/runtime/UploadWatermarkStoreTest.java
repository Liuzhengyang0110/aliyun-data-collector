package cn.kjky.collector.runtime;

import cn.kjky.collector.config.CollectorConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UploadWatermarkStoreTest {
    private static final Instant NOW = Instant.parse("2026-09-03T06:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void processesSequentialAcknowledgements() throws Exception {
        UploadWatermarkStore store = store(file("sequential"));

        UploadWatermarkStore.PollWatermark first = store.acknowledge(ack("poll-a", 0, false, 'a'));
        assertThat(first.continuousSequence()).isZero();
        assertThat(first.sparseAcks()).isEmpty();
        assertThat(first.lastBatchSequence()).isNull();
        assertThat(first.completed()).isFalse();

        UploadWatermarkStore.PollWatermark second = store.acknowledge(ack("poll-a", 1, true, 'b'));
        assertThat(second.continuousSequence()).isEqualTo(1);
        assertThat(second.sparseAcks()).isEmpty();
        assertThat(second.lastBatchSequence()).isEqualTo(1);
        assertThat(second.completed()).isTrue();
        assertThat(second.updatedAt()).isEqualTo(NOW);
    }

    @Test
    void keepsOutOfOrderAckSparseUntilGapIsFilled() throws Exception {
        UploadWatermarkStore store = store(file("out-of-order"));

        UploadWatermarkStore.PollWatermark lastFirst = store.acknowledge(ack("poll-a", 2, true, 'c'));
        assertThat(lastFirst.continuousSequence()).isEqualTo(-1);
        assertThat(lastFirst.sparseAcks()).containsExactly(2);
        assertThat(lastFirst.lastBatchSequence()).isEqualTo(2);
        assertThat(lastFirst.completed()).isFalse();

        UploadWatermarkStore.PollWatermark withFirst = store.acknowledge(ack("poll-a", 0, false, 'a'));
        assertThat(withFirst.continuousSequence()).isZero();
        assertThat(withFirst.sparseAcks()).containsExactly(2);

        UploadWatermarkStore.PollWatermark completed = store.acknowledge(ack("poll-a", 1, false, 'b'));
        assertThat(completed.continuousSequence()).isEqualTo(2);
        assertThat(completed.sparseAcks()).isEmpty();
        assertThat(completed.completed()).isTrue();
    }

    @Test
    void duplicateAckIsIdempotentAndDoesNotRewriteState() throws Exception {
        Path file = file("duplicate");
        UploadWatermarkStore store = store(file);
        UploadAcknowledgement ack = ack("poll-a", 2, false, 'c');

        UploadWatermarkStore.PollWatermark first = store.acknowledge(ack);
        byte[] firstBytes = Files.readAllBytes(file);
        UploadWatermarkStore.PollWatermark duplicate = store.acknowledge(ack);

        assertThat(duplicate).isEqualTo(first);
        assertThat(Files.readAllBytes(file)).isEqualTo(firstBytes);
    }

    @Test
    void isolatesDifferentPollIds() throws Exception {
        UploadWatermarkStore store = store(file("poll-isolation"));

        store.acknowledge(ack("poll-a", 0, true, 'a'));
        store.acknowledge(ack("poll-b", 1, true, 'b'));

        assertThat(store.snapshot()).containsOnlyKeys("poll-a", "poll-b");
        assertThat(store.get("poll-a").orElseThrow().completed()).isTrue();
        assertThat(store.get("poll-a").orElseThrow().continuousSequence()).isZero();
        assertThat(store.get("poll-b").orElseThrow().completed()).isFalse();
        assertThat(store.get("poll-b").orElseThrow().sparseAcks()).containsExactly(1);
    }

    @Test
    void marksCompletedOnlyAfterKnownLastSequenceBecomesContinuous() throws Exception {
        Path file = file("completed");
        UploadWatermarkStore store = store(file);

        assertThat(store.acknowledge(ack("poll-a", 0, false, 'a')).completed()).isFalse();
        assertThat(store.acknowledge(ack("poll-a", 1, false, 'b')).completed()).isFalse();
        UploadWatermarkStore.PollWatermark completed = store.acknowledge(ack("poll-a", 2, true, 'c'));

        assertThat(completed.completed()).isTrue();
        assertThat(completed.lastBatchSequence()).isEqualTo(2);
        assertThat(completed.continuousSequence()).isEqualTo(2);
        assertThat(new String(Files.readAllBytes(file), StandardCharsets.UTF_8))
                .doesNotContain("batch-poll-a")
                .contains("\"sparse_acks\":[]");
    }

    @Test
    void canInferLastSequenceFromBatchCount() throws Exception {
        UploadWatermarkStore store = store(file("batch-count"));

        UploadWatermarkStore.PollWatermark lastFirst = store.acknowledge(
                new UploadAcknowledgement("poll-a", "batch-poll-a-2", 2, "c".repeat(64), false, 3));
        assertThat(lastFirst.lastBatchSequence()).isEqualTo(2);
        assertThat(lastFirst.completed()).isFalse();

        store.acknowledge(ack("poll-a", 0, false, 'a'));
        assertThat(store.acknowledge(ack("poll-a", 1, false, 'b')).completed()).isTrue();
    }

    @Test
    void foldsAllContiguousSparseAcksWithoutKnowingLastBatch() throws Exception {
        UploadWatermarkStore store = store(file("fold"));

        store.acknowledge(ack("poll-a", 3, false, 'd'));
        store.acknowledge(ack("poll-a", 2, false, 'c'));
        store.acknowledge(ack("poll-a", 0, false, 'a'));
        UploadWatermarkStore.PollWatermark folded = store.acknowledge(ack("poll-a", 1, false, 'b'));

        assertThat(folded.continuousSequence()).isEqualTo(3);
        assertThat(folded.sparseAcks()).isEmpty();
        assertThat(folded.lastBatchSequence()).isNull();
        assertThat(folded.completed()).isFalse();
    }

    @Test
    void restoresStateAfterRestart() throws Exception {
        Path file = file("restart");
        UploadWatermarkStore first = store(file);
        first.acknowledge(ack("poll-a", 2, true, 'c'));
        first.acknowledge(ack("poll-a", 0, false, 'a'));

        UploadWatermarkStore restarted = store(file);
        UploadWatermarkStore.PollWatermark restored = restarted.get("poll-a").orElseThrow();
        assertThat(restored.continuousSequence()).isZero();
        assertThat(restored.sparseAcks()).containsExactly(2);
        assertThat(restored.lastBatchSequence()).isEqualTo(2);

        assertThat(restarted.acknowledge(ack("poll-a", 1, false, 'b')).completed()).isTrue();
    }

    @Test
    void failsClosedForCorruptOrIncompatibleState() throws Exception {
        Path corrupt = file("corrupt");
        Files.createDirectories(corrupt.getParent());
        Files.writeString(corrupt, "{not-json", StandardCharsets.UTF_8);
        assertThatThrownBy(() -> new UploadWatermarkStore(corrupt))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("失败关闭");

        Path incompatible = file("schema");
        Files.createDirectories(incompatible.getParent());
        Files.writeString(incompatible, "{\"schema_version\":\"9.0\",\"polls\":{}}", StandardCharsets.UTF_8);
        assertThatThrownBy(() -> new UploadWatermarkStore(incompatible))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("失败关闭");
    }

    @Test
    void failsClosedWhenUnresolvedPartialExists() throws Exception {
        Path file = file("partial");
        Files.createDirectories(file.getParent());
        Files.writeString(file.resolveSibling(file.getFileName() + ".partial"), "unknown", StandardCharsets.UTF_8);

        assertThatThrownBy(() -> new UploadWatermarkStore(file))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("临时文件");
    }

    @Test
    void keepsOldStateReadableWhenAtomicReplacementFails() throws Exception {
        Path file = file("move-failure");
        UploadWatermarkStore initial = store(file);
        initial.acknowledge(ack("poll-a", 0, false, 'a'));
        byte[] oldState = Files.readAllBytes(file);

        UploadWatermarkStore failing = new UploadWatermarkStore(file, CLOCK,
                (source, target) -> { throw new IOException("模拟原子替换失败"); });
        assertThatThrownBy(() -> failing.acknowledge(ack("poll-a", 1, true, 'b')))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("模拟原子替换失败");

        assertThat(Files.readAllBytes(file)).isEqualTo(oldState);
        assertThat(Files.exists(file.resolveSibling(file.getFileName() + ".partial"))).isFalse();
        UploadWatermarkStore reopened = store(file);
        assertThat(reopened.get("poll-a").orElseThrow().continuousSequence()).isZero();
        assertThat(reopened.get("poll-a").orElseThrow().completed()).isFalse();
    }

    @Test
    void rejectsDifferentAckForSameSparseSequenceWithoutChangingState() throws Exception {
        Path file = file("conflict");
        UploadWatermarkStore store = store(file);
        store.acknowledge(ack("poll-a", 2, false, 'a'));
        byte[] oldState = Files.readAllBytes(file);

        assertThatThrownBy(() -> store.acknowledge(ack("poll-a", 2, false, 'b')))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("收到不同 ACK");
        assertThat(Files.readAllBytes(file)).isEqualTo(oldState);
    }

    @Test
    void rejectsDifferentAckForFoldedSequenceBeforePollCompletes() throws Exception {
        Path file = file("continuous-conflict");
        UploadWatermarkStore store = store(file);
        store.acknowledge(ack("poll-a", 0, false, 'a'));
        byte[] oldState = Files.readAllBytes(file);

        assertThatThrownBy(() -> store.acknowledge(ack("poll-a", 0, false, 'b')))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("收到不同 ACK");
        assertThat(Files.readAllBytes(file)).isEqualTo(oldState);
    }

    @Test
    void resolvesDefaultFileUnderConfiguredStateDirectory() throws Exception {
        Path root = Path.of("target/test-upload-watermark", UUID.randomUUID().toString());
        CollectorConfig config = new CollectorConfig();
        config.storage.dataRoot = root.toString();
        config.storage.stateDir = "state-data";

        UploadWatermarkStore store = new UploadWatermarkStore(config);

        assertThat(store.file()).isEqualTo(root.toAbsolutePath().normalize()
                .resolve("state-data").resolve("upload-watermarks.json"));
    }

    private UploadWatermarkStore store(Path file) throws Exception {
        return new UploadWatermarkStore(file, CLOCK);
    }

    private Path file(String scenario) {
        return Path.of("target/test-upload-watermark", scenario, UUID.randomUUID().toString(),
                "state", "upload-watermarks.json");
    }

    private UploadAcknowledgement ack(String pollId, int sequence, boolean last, char hashCharacter) {
        return new UploadAcknowledgement(pollId, "batch-" + pollId + "-" + sequence,
                sequence, Character.toString(hashCharacter).repeat(64), last);
    }
}

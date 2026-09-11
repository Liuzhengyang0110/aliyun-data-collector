package cn.kjky.collector.runtime;

import cn.kjky.collector.contract.BatchEnvelope;

import java.util.Locale;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.regex.Pattern;

/**
 * 已由上层确认为 COMMITTED 或 ALREADY_COMMITTED 的批次 ACK。
 * <p>
 * 本类型不接收 HTTP 状态；调用方只有在确认可靠提交后才能构造并交给
 * {@link UploadWatermarkStore#acknowledge(UploadAcknowledgement)}。
 * </p>
 */
public record UploadAcknowledgement(
        String pollId,
        String batchId,
        int batchSequence,
        String contentSha256,
        boolean lastBatch,
        Integer batchCount) {

    private static final Pattern SHA256 = Pattern.compile("[0-9a-fA-F]{64}");

    /** 使用 is_last_batch 判定末批；batch_count 未提供。 */
    public UploadAcknowledgement(String pollId, String batchId, int batchSequence,
                                 String contentSha256, boolean lastBatch) {
        this(pollId, batchId, batchSequence, contentSha256, lastBatch, null);
    }

    public UploadAcknowledgement {
        requireText(pollId, "poll_id");
        requireText(batchId, "batch_id");
        if (batchSequence < 0) throw new IllegalArgumentException("batch_sequence 不能小于 0");
        requireText(contentSha256, "content_sha256");
        if (!SHA256.matcher(contentSha256).matches()) {
            throw new IllegalArgumentException("content_sha256 必须是 64 位十六进制 SHA-256");
        }
        contentSha256 = contentSha256.toLowerCase(Locale.ROOT);
        if (batchCount != null) {
            if (batchCount < 1) throw new IllegalArgumentException("batch_count 必须大于 0");
            if (batchSequence >= batchCount) {
                throw new IllegalArgumentException("batch_sequence 必须小于 batch_count");
            }
            if (lastBatch && batchSequence != batchCount - 1) {
                throw new IllegalArgumentException("is_last_batch 与 batch_count 不一致");
            }
        }
    }

    /** 从不可变 Outbox 批次创建 ACK 参数；必须在上层确认可靠提交后调用。 */
    public static UploadAcknowledgement fromCommitted(BatchEnvelope batch) {
        Objects.requireNonNull(batch, "batch 不能为空");
        return new UploadAcknowledgement(batch.pollId(), batch.batchId(), batch.batchSequence(),
                batch.contentSha256(), batch.lastBatch());
    }

    /** 根据 is_last_batch 或 batch_count 推导最后批次序号。 */
    public OptionalInt inferredLastBatchSequence() {
        if (batchCount != null) return OptionalInt.of(batchCount - 1);
        return lastBatch ? OptionalInt.of(batchSequence) : OptionalInt.empty();
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " 不能为空");
    }
}

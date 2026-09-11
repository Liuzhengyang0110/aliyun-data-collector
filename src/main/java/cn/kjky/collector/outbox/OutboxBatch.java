package cn.kjky.collector.outbox;

import cn.kjky.collector.contract.BatchEnvelope;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.time.Instant;
import java.util.Objects;

/**
 * PVC Outbox 中不可变的批次描述。
 * <p>
 * 真正提交给 cc-ingest 的字节保存在同目录的 {@code batch.json}；本对象只描述
 * 这些不可变字节及其业务身份，不包含尝试次数、错误原因等可变投递状态。
 * </p>
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public record OutboxBatch(
        String outboxSchemaVersion,
        String batchSchemaVersion,
        String batchId,
        String pollId,
        int batchSequence,
        boolean lastBatch,
        String collectorId,
        String sourceType,
        String sourceKey,
        Instant windowStart,
        Instant windowEnd,
        int recordCount,
        String contentSha256,
        String serializedSha256,
        Instant createdAt,
        String parentBatchId) {

    public static final String SCHEMA_VERSION = "1.0";

    public OutboxBatch {
        requireText(outboxSchemaVersion, "outbox_schema_version");
        requireText(batchSchemaVersion, "batch_schema_version");
        requireText(batchId, "batch_id");
        requireText(pollId, "poll_id");
        if (batchSequence < 0) throw new IllegalArgumentException("batch_sequence 不能小于 0");
        requireText(collectorId, "collector_id");
        requireText(sourceType, "source_type");
        requireText(sourceKey, "source_key");
        Objects.requireNonNull(windowStart, "window_start 不能为空");
        Objects.requireNonNull(windowEnd, "window_end 不能为空");
        if (!windowStart.isBefore(windowEnd)) throw new IllegalArgumentException("window_start 必须早于 window_end");
        if (recordCount < 1) throw new IllegalArgumentException("record_count 必须大于 0");
        requireSha256(contentSha256, "content_sha256");
        requireSha256(serializedSha256, "serialized_sha256");
        Objects.requireNonNull(createdAt, "created_at 不能为空");
        if (parentBatchId != null && parentBatchId.isBlank()) parentBatchId = null;
    }

    /** 从已冻结的 Ingest 批次和精确序列化字节构造 Outbox 描述。 */
    public static OutboxBatch from(BatchEnvelope batch, byte[] serializedBytes, Instant createdAt,
                                   String parentBatchId) {
        Objects.requireNonNull(batch, "batch 不能为空");
        Objects.requireNonNull(serializedBytes, "serializedBytes 不能为空");
        return new OutboxBatch(SCHEMA_VERSION, batch.schemaVersion(), batch.batchId(), batch.pollId(),
                batch.batchSequence(), batch.lastBatch(), batch.collectorId(), batch.sourceType(),
                batch.sourceKey(), batch.windowStart(), batch.windowEnd(), batch.recordCount(),
                batch.contentSha256(), OutboxFiles.sha256(serializedBytes), createdAt, parentBatchId);
    }

    /** 校验描述与从 batch.json 解析出的上传批次完全对应。 */
    public void verify(BatchEnvelope batch, byte[] serializedBytes) {
        Objects.requireNonNull(batch, "batch 不能为空");
        if (!SCHEMA_VERSION.equals(outboxSchemaVersion)) {
            throw new IllegalStateException("不支持的 Outbox Schema: " + outboxSchemaVersion);
        }
        if (!batchSchemaVersion.equals(batch.schemaVersion())
                || !batchId.equals(batch.batchId())
                || !pollId.equals(batch.pollId())
                || batchSequence != batch.batchSequence()
                || lastBatch != batch.lastBatch()
                || !collectorId.equals(batch.collectorId())
                || !sourceType.equals(batch.sourceType())
                || !sourceKey.equals(batch.sourceKey())
                || !windowStart.equals(batch.windowStart())
                || !windowEnd.equals(batch.windowEnd())
                || recordCount != batch.recordCount()
                || !contentSha256.equalsIgnoreCase(batch.contentSha256())) {
            throw new IllegalStateException("Outbox 描述与 batch.json 内容不一致: " + batchId);
        }
        String actualSerializedHash = OutboxFiles.sha256(serializedBytes);
        if (!serializedSha256.equalsIgnoreCase(actualSerializedHash)) {
            throw new IllegalStateException("batch.json SHA-256 不一致: " + batchId);
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " 不能为空");
    }

    private static void requireSha256(String value, String name) {
        requireText(value, name);
        if (!value.matches("(?i)[0-9a-f]{64}")) throw new IllegalArgumentException(name + " 必须是 SHA-256 十六进制字符串");
    }
}

package cn.kjky.collector.batch;

import cn.kjky.collector.contract.IngestJson;
import cn.kjky.collector.contract.StandardEvent;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * 为查询窗口、普通批次和 413 拆分子批次生成稳定标识。
 * <p>
 * 标识只依赖稳定的采集上下文、序号和不可变事件内容，进程重启或重试不会改变结果。
 * </p>
 */
public final class BatchIdGenerator {
    private static final String POLL_ALGORITHM = "collector-poll-id-v1";
    private static final String BATCH_ALGORITHM = "collector-batch-id-v1";
    private static final String CHILD_ALGORITHM = "collector-child-batch-id-v1";

    /** 为同一采集器、数据源和查询窗口生成稳定 poll_id。 */
    public String pollId(String collectorId, String sourceType, String sourceKey,
                         Instant windowStart, Instant windowEnd) {
        requireText(collectorId, "collector_id");
        requireText(sourceType, "source_type");
        requireText(sourceKey, "source_key");
        requireWindow(windowStart, windowEnd);
        return "poll-" + IngestJson.sha256Canonical(new PollIdentity(
                POLL_ALGORITHM, collectorId, sourceType, sourceKey, windowStart, windowEnd));
    }

    /** 为正常组批生成稳定 batch_id；是否为尾批不参与 ID，避免结束标识影响重试身份。 */
    public String batchId(BatchBuilder.BatchContext context, int batchSequence,
                          List<StandardEvent> events) {
        Objects.requireNonNull(context, "batch context 不能为空");
        if (batchSequence < 0) throw new IllegalArgumentException("batch_sequence 不能小于 0");
        String contentSha256 = eventsSha256(events);
        return "batch-" + IngestJson.sha256Canonical(new BatchIdentity(
                BATCH_ALGORITHM, context.pollId(), batchSequence, context.collectorId(),
                context.siteId(), context.collectorType(), context.sourceType(), context.sourceKey(),
                context.windowStart(), context.windowEnd(), contentSha256));
    }

    /**
     * 为 413 拆分后的子批次生成稳定 ID。childIndex 从 0 开始，同一父批次、子序号和
     * 子事件内容始终得到相同 ID。
     */
    public String childBatchId(String parentBatchId, int childIndex, List<StandardEvent> events) {
        requireText(parentBatchId, "parent_batch_id");
        if (childIndex < 0) throw new IllegalArgumentException("child_index 不能小于 0");
        return "batch-child-" + IngestJson.sha256Canonical(new ChildIdentity(
                CHILD_ALGORITHM, parentBatchId, childIndex, eventsSha256(events)));
    }

    private String eventsSha256(List<StandardEvent> events) {
        if (events == null || events.isEmpty()) throw new IllegalArgumentException("events 不能为空");
        return IngestJson.eventsSha256(events);
    }

    private void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " 不能为空");
    }

    private void requireWindow(Instant windowStart, Instant windowEnd) {
        Objects.requireNonNull(windowStart, "window_start 不能为空");
        Objects.requireNonNull(windowEnd, "window_end 不能为空");
        if (!windowStart.isBefore(windowEnd)) throw new IllegalArgumentException("window_start 必须早于 window_end");
    }

    private record PollIdentity(String algorithm, String collectorId, String sourceType, String sourceKey,
                                Instant windowStart, Instant windowEnd) {}

    private record BatchIdentity(String algorithm, String pollId, int batchSequence,
                                 String collectorId, String siteId, String collectorType,
                                 String sourceType, String sourceKey,
                                 Instant windowStart, Instant windowEnd, String contentSha256) {}

    private record ChildIdentity(String algorithm, String parentBatchId, int childIndex,
                                 String contentSha256) {}
}

package cn.kjky.collector.batch;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.contract.BatchEnvelope;
import cn.kjky.collector.contract.IngestJson;
import cn.kjky.collector.contract.StandardEvent;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 流式构造 cc-ingest 批次。
 * <p>
 * 一个会话最多保留当前待封装批次的事件；批次达到记录数或字节上限时立即交给 sink，
 * 不会在内存中累积整轮采集结果。调用方必须在查询窗口完成时调用 {@link Session#finish()}，
 * 以发布不足阈值的尾批并标记最后一个批次。
 * </p>
 */
public final class BatchBuilder {
    private final BatchLimits limits;
    private final BatchIdGenerator idGenerator;
    private final Clock clock;

    /** 使用 YAML 中 upload 配置创建组批器。 */
    public BatchBuilder(CollectorConfig.UploadConfig upload) {
        this(new BatchLimits(requireUpload(upload).maxRecords, upload.maxCompressedBytes,
                upload.maxRequestBytes, upload.maxWaitMillis));
    }

    /** 使用显式边界创建组批器，便于测试和嵌入调用。 */
    public BatchBuilder(BatchLimits limits) {
        this(limits, new BatchIdGenerator(), Clock.systemUTC());
    }

    BatchBuilder(BatchLimits limits, BatchIdGenerator idGenerator, Clock clock) {
        this.limits = Objects.requireNonNull(limits, "limits 不能为空");
        this.idGenerator = Objects.requireNonNull(idGenerator, "idGenerator 不能为空");
        this.clock = Objects.requireNonNull(clock, "clock 不能为空");
    }

    /** 开始一个固定来源和查询窗口的流式组批会话。 */
    public Session open(BatchContext context, Consumer<BuiltBatch> sink) {
        return new Session(context, sink);
    }

    /**
     * 将收到 413 的批次按事件顺序二分为两个确定性子批次。
     * <p>
     * 子批次通过 {@link BuiltBatch#parentBatchId()} 关联父批次，供后续 Outbox 上传器写入
     * {@code parent_batch_id}。子批次保留父批次的 sequence：它们是一个逻辑批次的投递替代物，
     * P0-09 将以父批次完成情况推进上传水位，而不是把子批次当成新的采集窗口序号。
     * </p>
     */
    public List<BuiltBatch> splitForPayloadTooLarge(BuiltBatch parent) {
        Objects.requireNonNull(parent, "parent 批次不能为空");
        List<StandardEvent> events = parent.batch().events();
        if (events.size() < 2) {
            throw new BatchTooLargeException(events.get(0).eventId(), "单事件批次收到 413，无法继续拆分");
        }

        int middle = events.size() / 2;
        BatchContext context = BatchContext.from(parent.batch());
        String parentId = parent.batch().batchId();
        BuiltBatch first = measured(context, parent.batch().batchSequence(), false,
                events.subList(0, middle), parentId, 0);
        BuiltBatch second = measured(context, parent.batch().batchSequence(), parent.batch().lastBatch(),
                events.subList(middle, events.size()), parentId, 1);
        requireWithinLimits(first, first.batch().events().get(0));
        requireWithinLimits(second, second.batch().events().get(0));
        return List.of(first, second);
    }

    /** 组批的不可变输入上下文。 */
    public record BatchContext(
            String pollId,
            String collectorId,
            String siteId,
            String collectorType,
            String sourceType,
            String sourceKey,
            Instant windowStart,
            Instant windowEnd) {

        public BatchContext {
            requireText(pollId, "poll_id");
            requireText(collectorId, "collector_id");
            requireText(siteId, "site_id");
            requireText(collectorType, "collector_type");
            requireText(sourceType, "source_type");
            requireText(sourceKey, "source_key");
            Objects.requireNonNull(windowStart, "window_start 不能为空");
            Objects.requireNonNull(windowEnd, "window_end 不能为空");
            if (!windowStart.isBefore(windowEnd)) throw new IllegalArgumentException("window_start 必须早于 window_end");
        }

        /** 从已有批次恢复拆分子批次所需的上下文。 */
        static BatchContext from(BatchEnvelope batch) {
            return new BatchContext(batch.pollId(), batch.collectorId(), batch.siteId(), batch.collectorType(),
                    batch.sourceType(), batch.sourceKey(), batch.windowStart(), batch.windowEnd());
        }
    }

    /** 上传配置中控制单个批次的硬边界。 */
    public record BatchLimits(int maxRecords, long maxCompressedBytes, long maxRequestBytes, long maxWaitMillis) {
        public BatchLimits {
            if (maxRecords < 1) throw new IllegalArgumentException("maxRecords 必须大于 0");
            if (maxCompressedBytes < 1) throw new IllegalArgumentException("maxCompressedBytes 必须大于 0");
            if (maxRequestBytes < 1) throw new IllegalArgumentException("maxRequestBytes 必须大于 0");
            if (maxCompressedBytes > maxRequestBytes) {
                throw new IllegalArgumentException("maxCompressedBytes 不能大于 maxRequestBytes");
            }
            if (maxWaitMillis < 1) throw new IllegalArgumentException("maxWaitMillis 必须大于 0");
        }
    }

    /** 已冻结的批次及其精确请求大小；parentBatchId 仅在 413 子批次中非空。 */
    public record BuiltBatch(BatchEnvelope batch, String parentBatchId,
                             int uncompressedBytes, int gzipBytes) {
        public BuiltBatch {
            Objects.requireNonNull(batch, "batch 不能为空");
            if (parentBatchId != null && parentBatchId.isBlank()) parentBatchId = null;
            if (uncompressedBytes < 1) throw new IllegalArgumentException("uncompressedBytes 必须大于 0");
            if (gzipBytes < 1) throw new IllegalArgumentException("gzipBytes 必须大于 0");
        }
    }

    /** 单个事件无法在本地请求边界内构成一个有效批次时抛出。 */
    public static final class BatchTooLargeException extends IllegalArgumentException {
        private final String eventId;

        BatchTooLargeException(String eventId, String message) {
            super(message + "; event_id=" + eventId);
            this.eventId = eventId;
        }

        public String eventId() {
            return eventId;
        }
    }

    /**
     * 单个 poll 的流式会话。调用方可以在每产生一条标准事件时调用 {@link #accept(StandardEvent)}；
     * {@link #flushIfDue()} 供分页阻塞时由调度器调用，强制发布一个非尾批。
     */
    public final class Session {
        private final BatchContext context;
        private final Consumer<BuiltBatch> sink;
        private final List<StandardEvent> current = new ArrayList<>();
        private int nextSequence;
        private Instant openedAt;
        private boolean finished;

        private Session(BatchContext context, Consumer<BuiltBatch> sink) {
            this.context = Objects.requireNonNull(context, "context 不能为空");
            this.sink = Objects.requireNonNull(sink, "sink 不能为空");
        }

        /** 接收一条标准事件并按系统时钟判断批次的开始时间。 */
        public void accept(StandardEvent event) {
            accept(event, clock.instant());
        }

        /** 接收一条标准事件；显式传入时间便于调度器和测试控制等待阈值。 */
        public void accept(StandardEvent event, Instant now) {
            ensureOpen();
            Objects.requireNonNull(event, "event 不能为空");
            Objects.requireNonNull(now, "now 不能为空");
            if (current.isEmpty()) {
                requireWithinLimits(measured(context, nextSequence, false, List.of(event), null, -1), event);
                current.add(event);
                openedAt = now;
                return;
            }

            List<StandardEvent> candidate = new ArrayList<>(current.size() + 1);
            candidate.addAll(current);
            candidate.add(event);
            BuiltBatch measuredCandidate = measured(context, nextSequence, false, candidate, null, -1);
            if (withinLimits(measuredCandidate)) {
                current.add(event);
                return;
            }

            // 先确认新事件可独立组成下一批，再发布当前批次，避免超大单事件造成无意义的状态推进。
            requireWithinLimits(measured(context, nextSequence + 1, false, List.of(event), null, -1), event);
            emit(false);
            current.add(event);
            openedAt = now;
        }

        /**
         * 当当前批次等待达到 maxWaitMillis 时发布非尾批。返回 true 表示本次实际发布了批次。
         * 查询窗口结束时仍须调用 finish，以发布之后形成的尾批。
         */
        public boolean flushIfDue() {
            return flushIfDue(clock.instant());
        }

        /** 使用指定时刻判断是否达到最大等待时间，供调度器周期调用。 */
        public boolean flushIfDue(Instant now) {
            ensureOpen();
            Objects.requireNonNull(now, "now 不能为空");
            if (current.isEmpty() || now.isBefore(openedAt.plusMillis(limits.maxWaitMillis()))) return false;
            emit(false);
            return true;
        }

        /** 查询窗口结束时发布不足阈值的尾批，并将其标记为 is_last_batch=true。 */
        public void finish() {
            ensureOpen();
            if (!current.isEmpty()) emit(true);
            finished = true;
        }

        /** 返回当前内存中尚未发布的事件数量，始终不超过 maxRecords。 */
        public int bufferedEventCount() {
            return current.size();
        }

        private void emit(boolean lastBatch) {
            BuiltBatch built = measured(context, nextSequence, lastBatch, List.copyOf(current), null, -1);
            requireWithinLimits(built, current.get(0));
            sink.accept(built);
            current.clear();
            openedAt = null;
            nextSequence++;
        }

        private void ensureOpen() {
            if (finished) throw new IllegalStateException("组批会话已经结束");
        }
    }

    private BuiltBatch measured(BatchContext context, int sequence, boolean lastBatch,
                                List<StandardEvent> events, String parentBatchId, int childIndex) {
        try {
            List<StandardEvent> immutableEvents = List.copyOf(events);
            String batchId = parentBatchId == null
                    ? idGenerator.batchId(context, sequence, immutableEvents)
                    : idGenerator.childBatchId(parentBatchId, childIndex, immutableEvents);
            BatchEnvelope batch = BatchEnvelope.create(batchId, context.pollId(), sequence, lastBatch,
                    context.collectorId(), context.siteId(), context.collectorType(), context.sourceType(),
                    context.sourceKey(), context.windowStart(), context.windowEnd(), immutableEvents);
            byte[] json = IngestJson.writeBatch(batch);
            byte[] gzip = IngestJson.gzip(json);
            return new BuiltBatch(batch, parentBatchId, json.length, gzip.length);
        } catch (IOException e) {
            throw new IllegalStateException("无法序列化组批结果", e);
        }
    }

    private boolean withinLimits(BuiltBatch batch) {
        return batch.batch().recordCount() <= limits.maxRecords()
                && batch.uncompressedBytes() <= limits.maxRequestBytes()
                && batch.gzipBytes() <= limits.maxCompressedBytes();
    }

    private void requireWithinLimits(BuiltBatch batch, StandardEvent event) {
        if (batch.batch().recordCount() > limits.maxRecords()) {
            throw new BatchTooLargeException(event.eventId(), "单事件批次数量超过 maxRecords");
        }
        if (batch.uncompressedBytes() > limits.maxRequestBytes()) {
            throw new BatchTooLargeException(event.eventId(), "单事件批次未压缩字节超过 maxRequestBytes");
        }
        if (batch.gzipBytes() > limits.maxCompressedBytes()) {
            throw new BatchTooLargeException(event.eventId(), "单事件批次 gzip 字节超过 maxCompressedBytes");
        }
    }

    private static CollectorConfig.UploadConfig requireUpload(CollectorConfig.UploadConfig upload) {
        return Objects.requireNonNull(upload, "upload 配置不能为空");
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " 不能为空");
    }
}

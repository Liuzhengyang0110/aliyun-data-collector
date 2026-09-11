package cn.kjky.collector.pipeline;

import cn.kjky.collector.batch.BatchBuilder;
import cn.kjky.collector.batch.BatchIdGenerator;
import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.contract.StandardEvent;
import cn.kjky.collector.outbox.OutboxStore;
import cn.kjky.collector.runtime.CollectionWatermarkStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.util.Objects;

/**
 * 生产采集侧串联：标准事件 → 流式组批 → Outbox pending → 任务级采集水位。
 * <p>不包含 HTTP 上传、上传水位或任何删除 Outbox 批次的逻辑。</p>
 */
public final class ProductionEventSink implements CollectionEventSink {
    private final BatchBuilder batchBuilder;
    private final BatchIdGenerator ids;
    private final BatchWriter writer;
    private final CollectionWatermarkStore watermarks;
    private final CollectorConfig.CollectorIdentityConfig identity;
    private final Clock clock;
    private final CompletionHook completionHook;

    public ProductionEventSink(CollectorConfig config, OutboxStore outbox,
                               CollectionWatermarkStore watermarks) {
        this(config, new BatchBuilder(config.upload), new BatchIdGenerator(),
                built -> outbox.enqueue(built.batch(), built.parentBatchId()), watermarks,
                Clock.systemUTC(), task -> { });
    }

    ProductionEventSink(CollectorConfig config, BatchBuilder batchBuilder, BatchIdGenerator ids,
                        BatchWriter writer, CollectionWatermarkStore watermarks, Clock clock,
                        CompletionHook completionHook) {
        Objects.requireNonNull(config, "config 不能为空");
        this.identity = Objects.requireNonNull(config.collector, "collector 配置不能为空");
        require(identity.collectorId, "collector.collectorId");
        require(identity.siteId, "collector.siteId");
        require(identity.collectorType, "collector.collectorType");
        this.batchBuilder = Objects.requireNonNull(batchBuilder, "batchBuilder 不能为空");
        this.ids = Objects.requireNonNull(ids, "ids 不能为空");
        this.writer = Objects.requireNonNull(writer, "writer 不能为空");
        this.watermarks = Objects.requireNonNull(watermarks, "watermarks 不能为空");
        this.clock = Objects.requireNonNull(clock, "clock 不能为空");
        this.completionHook = Objects.requireNonNull(completionHook, "completionHook 不能为空");
    }

    @Override
    public TaskSession open(CollectionTask task) {
        Objects.requireNonNull(task, "task 不能为空");
        String pollId = ids.pollId(identity.collectorId, task.sourceType(), task.taskKey(),
                task.windowStart(), task.windowEnd());
        BatchBuilder.BatchContext context = new BatchBuilder.BatchContext(pollId, identity.collectorId,
                identity.siteId, identity.collectorType, task.sourceType(), task.taskKey(),
                task.windowStart(), task.windowEnd());
        BatchBuilder.Session batches = batchBuilder.open(context, built -> {
            try {
                writer.write(built);
            } catch (IOException e) {
                throw new UncheckedIOException("Outbox 写入失败", e);
            }
        });
        return new Session(task, batches);
    }

    private final class Session implements TaskSession {
        private final CollectionTask task;
        private final BatchBuilder.Session batches;
        private boolean finished;

        private Session(CollectionTask task, BatchBuilder.Session batches) {
            this.task = task;
            this.batches = batches;
        }

        @Override
        public void accept(StandardEvent event) throws IOException {
            ensureOpen();
            try {
                batches.accept(event);
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        }

        @Override
        public void finish() throws IOException {
            ensureOpen();
            try {
                batches.finish();
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
            // 此时尾批也已完成 writing → pending；测试 hook 模拟水位更新前崩溃。
            completionHook.afterPending(task);
            watermarks.advance(task.taskKey(), task.windowEnd(), clock.instant());
            finished = true;
        }

        private void ensureOpen() {
            if (finished) throw new IllegalStateException("生产采集任务会话已经结束");
        }
    }

    @FunctionalInterface
    interface BatchWriter {
        void write(BatchBuilder.BuiltBatch batch) throws IOException;
    }

    @FunctionalInterface
    interface CompletionHook {
        void afterPending(CollectionTask task) throws IOException;
    }

    private static void require(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " 不能为空");
    }
}

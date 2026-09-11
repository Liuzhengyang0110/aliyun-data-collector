package cn.kjky.collector.transport;

import cn.kjky.collector.batch.BatchBuilder;
import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.contract.BatchEnvelope;
import cn.kjky.collector.contract.IngestJson;
import cn.kjky.collector.outbox.OutboxClaim;
import cn.kjky.collector.outbox.OutboxDelivery;
import cn.kjky.collector.outbox.OutboxStore;
import cn.kjky.collector.outbox.OutboxStoredBatch;
import cn.kjky.collector.runtime.UploadAcknowledgement;
import cn.kjky.collector.runtime.UploadWatermarkStore;

import java.io.IOException;
import java.net.http.HttpTimeoutException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 从文件式 Outbox 认领批次并主动提交到 cc-ingest 的单轮上传器。
 * <p>
 * 本组件不创建常驻线程；调用方按需执行 {@link #uploadAvailable()}。内部工作池只限制当前
 * 调用所触发的实际在途 HTTP 请求。所有失败分支都保留原批次，只有可靠回执经过上传水位
 * 原子持久化后才允许删除本地批次。
 * </p>
 */
public final class BatchUploader implements AutoCloseable {
    private final OutboxStore outbox;
    private final IngestClient ingest;
    private final UploadWatermarkStore watermarks;
    private final UploadRetryPolicy retryPolicy;
    private final BatchBuilder batchBuilder;
    private final Clock clock;
    private final ExecutorService executor;
    private final int maxInFlight;
    private final CommitHook commitHook;
    private final AtomicReference<UploaderStatus> status = new AtomicReference<>(UploaderStatus.active());
    private final AtomicReference<UploadWarning> lastWarning = new AtomicReference<>();

    /** 使用配置创建 IngestClient，连接超时和请求超时均直接来自 upload 节点。 */
    public BatchUploader(CollectorConfig config, OutboxStore outbox, UploadWatermarkStore watermarks) {
        this(config, new IngestClient(config), outbox, watermarks);
    }

    /** 使用现有组件创建上传器；IngestClient 的连接/请求超时已经来自 upload 配置。 */
    public BatchUploader(CollectorConfig config, IngestClient ingest, OutboxStore outbox,
                         UploadWatermarkStore watermarks) {
        this(requireUpload(config), ingest, outbox, watermarks,
                new UploadRetryPolicy(config.upload), new BatchBuilder(config.upload),
                Clock.systemUTC(), batch -> { });
    }

    BatchUploader(CollectorConfig config, IngestClient ingest, OutboxStore outbox,
                  UploadWatermarkStore watermarks, UploadRetryPolicy retryPolicy,
                  BatchBuilder batchBuilder, Clock clock, CommitHook commitHook) {
        this(requireUpload(config), ingest, outbox, watermarks, retryPolicy, batchBuilder, clock, commitHook);
    }

    private BatchUploader(CollectorConfig.UploadConfig upload, IngestClient ingest, OutboxStore outbox,
                          UploadWatermarkStore watermarks, UploadRetryPolicy retryPolicy,
                          BatchBuilder batchBuilder, Clock clock, CommitHook commitHook) {
        if (!upload.enabled) throw new IllegalStateException("upload.enabled=false，不能创建 BatchUploader");
        if (upload.maxInFlightBatches < 1) throw new IllegalArgumentException("maxInFlightBatches 必须大于 0");
        this.ingest = Objects.requireNonNull(ingest, "IngestClient 不能为空");
        this.outbox = Objects.requireNonNull(outbox, "OutboxStore 不能为空");
        this.watermarks = Objects.requireNonNull(watermarks, "UploadWatermarkStore 不能为空");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "UploadRetryPolicy 不能为空");
        this.batchBuilder = Objects.requireNonNull(batchBuilder, "BatchBuilder 不能为空");
        this.clock = Objects.requireNonNull(clock, "clock 不能为空");
        this.commitHook = Objects.requireNonNull(commitHook, "commitHook 不能为空");
        this.maxInFlight = upload.maxInFlightBatches;
        this.executor = Executors.newFixedThreadPool(maxInFlight, daemonThreadFactory());
    }

    /**
     * 先收敛上次崩溃遗留的已完成拆分父批，再最多认领 maxInFlightBatches 个批次并等待本轮结束。
     */
    public UploadCycleResult uploadAvailable() throws IOException, InterruptedException {
        ensureOpen();
        finalizeCompletedSplits();
        if (status.get().state() == UploaderState.PAUSED_AUTHENTICATION) return UploadCycleResult.empty();

        List<OutboxClaim> claims = new ArrayList<>(maxInFlight);
        try {
            Instant now = clock.instant();
            for (int i = 0; i < maxInFlight; i++) {
                var next = outbox.claimNext(now);
                if (next.isEmpty()) break;
                claims.add(next.get());
            }
            if (claims.isEmpty()) return UploadCycleResult.empty();

            List<Callable<BatchOutcome>> tasks = claims.stream()
                    .<Callable<BatchOutcome>>map(claim -> () -> process(claim))
                    .toList();
            List<java.util.concurrent.Future<BatchOutcome>> futures = executor.invokeAll(tasks);
            UploadCycleResult result = UploadCycleResult.empty();
            for (var future : futures) result = result.plus(get(future));
            return result.withClaimed(claims.size());
        } finally {
            // invokeAll 被中断或任务在进入 process 前取消时，也必须释放已经认领的批次。
            claims.forEach(OutboxClaim::close);
        }
    }

    public UploaderStatus status() {
        return status.get();
    }

    public UploadWarning lastWarning() {
        return lastWarning.get();
    }

    /** 凭据修复后由外部明确恢复；不会修改或清空任何 Outbox 数据。 */
    public void resumeAfterAuthenticationRepair() {
        ensureOpen();
        status.set(UploaderStatus.active());
    }

    private BatchOutcome process(OutboxClaim claim) throws Exception {
        OutboxStoredBatch stored = claim.stored();
        IngestResult result;
        try {
            result = ingest.send(stored.batch());
        } catch (HttpTimeoutException e) {
            return scheduleRetry(claim, OutboxDelivery.ErrorCategory.TIMEOUT, null);
        } catch (IOException e) {
            return scheduleRetry(claim, OutboxDelivery.ErrorCategory.NETWORK, null);
        }

        return switch (result.disposition()) {
            case COMMITTED -> commit(claim);
            case RETRYABLE -> scheduleRetry(claim, retryCategory(result),
                    result.httpStatus() == 429 ? result.retryAfterSeconds() : null);
            case BLOCKED -> block(claim, OutboxDelivery.ErrorCategory.VALIDATION);
            case CONFLICT -> block(claim, OutboxDelivery.ErrorCategory.CONFLICT);
            case AUTHENTICATION_FAILED -> pauseAuthentication(result.httpStatus());
            case PAYLOAD_TOO_LARGE -> split(claim);
            case UNKNOWN_RESPONSE -> scheduleRetry(claim, OutboxDelivery.ErrorCategory.UNKNOWN, null);
        };
    }

    private BatchOutcome commit(OutboxClaim claim) throws Exception {
        OutboxStoredBatch stored = claim.stored();
        String parentBatchId = stored.descriptor().parentBatchId();
        if (parentBatchId == null) {
            watermarks.acknowledge(UploadAcknowledgement.fromCommitted(stored.batch()));
            commitHook.afterWatermark(stored.batch());
            outbox.deleteCommitted(claim);
            return BatchOutcome.COMMITTED;
        }

        OutboxStoredBatch parent = outbox.acknowledgeSplitChild(parentBatchId, stored.batch().batchId());
        outbox.deleteCommitted(claim);
        if (parent.delivery().splitComplete()) finalizeCompletedSplits();
        return BatchOutcome.COMMITTED;
    }

    private BatchOutcome scheduleRetry(OutboxClaim claim, OutboxDelivery.ErrorCategory category,
                                       Long retryAfterSeconds) throws IOException {
        int completedAttempts = claim.stored().delivery().attemptCount();
        UploadRetryPolicy.RetryDecision decision = retryPolicy.next(clock.instant(), completedAttempts,
                retryAfterSeconds);
        outbox.scheduleRetry(claim, category, decision.nextRetryAt());
        if (decision.warning()) {
            lastWarning.set(new UploadWarning("RETRY_ATTEMPTS_HIGH", claim.stored().batch().batchId(),
                    decision.attempt(), clock.instant()));
        }
        return BatchOutcome.RETRY_SCHEDULED;
    }

    private BatchOutcome block(OutboxClaim claim, OutboxDelivery.ErrorCategory category) throws IOException {
        outbox.markBlocked(claim, category);
        return BatchOutcome.BLOCKED;
    }

    private BatchOutcome pauseAuthentication(int httpStatus) {
        status.set(new UploaderStatus(UploaderState.PAUSED_AUTHENTICATION,
                "CC_INGEST_AUTHENTICATION_FAILED", ErrorPriority.HIGH, httpStatus, clock.instant()));
        // claim 由 uploadAvailable 的 finally 释放，批次内容和 delivery.json 都不修改。
        return BatchOutcome.AUTHENTICATION_PAUSED;
    }

    private BatchOutcome split(OutboxClaim claim) throws IOException {
        OutboxStoredBatch stored = claim.stored();
        try {
            byte[] gzip = IngestJson.gzip(stored.serializedBatch());
            BatchBuilder.BuiltBatch parent = new BatchBuilder.BuiltBatch(stored.batch(),
                    stored.descriptor().parentBatchId(), stored.serializedBatch().length, gzip.length);
            List<BatchBuilder.BuiltBatch> children = batchBuilder.splitForPayloadTooLarge(parent);
            outbox.markSplit(claim, children.stream().map(BatchBuilder.BuiltBatch::batch).toList());
            return BatchOutcome.SPLIT;
        } catch (BatchBuilder.BatchTooLargeException e) {
            outbox.markBlocked(claim, OutboxDelivery.ErrorCategory.PAYLOAD_TOO_LARGE);
            return BatchOutcome.BLOCKED;
        }
    }

    /**
     * 完成 413 的逻辑父批：根父批推进上传水位，嵌套父批向上一层确认；随后才删除父批确认信息。
     */
    private void finalizeCompletedSplits() throws IOException {
        boolean progressed;
        do {
            progressed = false;
            for (OutboxStoredBatch parent : outbox.completedSplitParents()) {
                String ancestor = parent.descriptor().parentBatchId();
                if (ancestor == null) {
                    watermarks.acknowledge(UploadAcknowledgement.fromCommitted(parent.batch()));
                    commitHook.afterWatermark(parent.batch());
                } else {
                    outbox.acknowledgeSplitChild(ancestor, parent.batch().batchId());
                }
                outbox.deleteCompletedSplit(parent.batch().batchId());
                progressed = true;
            }
        } while (progressed);
    }

    private OutboxDelivery.ErrorCategory retryCategory(IngestResult result) {
        if (result.httpStatus() == 429) return OutboxDelivery.ErrorCategory.RATE_LIMITED;
        if (result.httpStatus() == 408) return OutboxDelivery.ErrorCategory.TIMEOUT;
        return OutboxDelivery.ErrorCategory.SERVER_ERROR;
    }

    private BatchOutcome get(java.util.concurrent.Future<BatchOutcome> future)
            throws IOException, InterruptedException {
        try {
            return future.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) throw io;
            if (cause instanceof InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            }
            if (cause instanceof RuntimeException runtime) throw runtime;
            throw new IOException("上传工作线程失败，Outbox 批次已保留", cause);
        }
    }

    private void ensureOpen() {
        if (status.get().state() == UploaderState.CLOSED) throw new IllegalStateException("BatchUploader 已关闭");
    }

    @Override
    public void close() {
        UploaderStatus previous = status.getAndSet(new UploaderStatus(UploaderState.CLOSED,
                "UPLOADER_CLOSED", ErrorPriority.NONE, null, clock.instant()));
        if (previous.state() != UploaderState.CLOSED) {
            executor.shutdownNow();
            ingest.close();
        }
    }

    private static CollectorConfig.UploadConfig requireUpload(CollectorConfig config) {
        Objects.requireNonNull(config, "config 不能为空");
        return Objects.requireNonNull(config.upload, "upload 配置不能为空");
    }

    private static ThreadFactory daemonThreadFactory() {
        AtomicInteger sequence = new AtomicInteger();
        return task -> {
            Thread thread = new Thread(task, "cc-ingest-uploader-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    enum BatchOutcome {
        COMMITTED,
        RETRY_SCHEDULED,
        BLOCKED,
        SPLIT,
        AUTHENTICATION_PAUSED
    }

    public enum UploaderState {
        ACTIVE,
        PAUSED_AUTHENTICATION,
        CLOSED
    }

    public enum ErrorPriority {
        NONE,
        HIGH
    }

    /** 对外暴露的脱敏运行状态；不包含请求体、响应正文、payload、凭据或证书。 */
    public record UploaderStatus(UploaderState state, String code, ErrorPriority priority,
                                 Integer httpStatus, Instant occurredAt) {
        static UploaderStatus active() {
            return new UploaderStatus(UploaderState.ACTIVE, null, ErrorPriority.NONE, null, null);
        }
    }

    /** 达到单批告警尝试次数时记录有限元数据，不包含错误正文。 */
    public record UploadWarning(String code, String batchId, int attempts, Instant occurredAt) {
    }

    /** 单次 uploadAvailable 的处理统计。 */
    public record UploadCycleResult(int claimed, int committed, int retryScheduled, int blocked,
                                    int split, int authenticationPaused) {
        static UploadCycleResult empty() {
            return new UploadCycleResult(0, 0, 0, 0, 0, 0);
        }

        UploadCycleResult plus(BatchOutcome outcome) {
            return switch (outcome) {
                case COMMITTED -> new UploadCycleResult(claimed, committed + 1, retryScheduled, blocked, split, authenticationPaused);
                case RETRY_SCHEDULED -> new UploadCycleResult(claimed, committed, retryScheduled + 1, blocked, split, authenticationPaused);
                case BLOCKED -> new UploadCycleResult(claimed, committed, retryScheduled, blocked + 1, split, authenticationPaused);
                case SPLIT -> new UploadCycleResult(claimed, committed, retryScheduled, blocked, split + 1, authenticationPaused);
                case AUTHENTICATION_PAUSED -> new UploadCycleResult(claimed, committed, retryScheduled, blocked, split, authenticationPaused + 1);
            };
        }

        UploadCycleResult withClaimed(int value) {
            return new UploadCycleResult(value, committed, retryScheduled, blocked, split, authenticationPaused);
        }
    }

    @FunctionalInterface
    interface CommitHook {
        void afterWatermark(BatchEnvelope batch) throws IOException;
    }
}

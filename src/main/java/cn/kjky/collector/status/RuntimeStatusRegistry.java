package cn.kjky.collector.status;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.control.JobStateStore;
import cn.kjky.collector.control.ManualPauseStateStore;
import cn.kjky.collector.outbox.OutboxStore;
import cn.kjky.collector.runtime.CapacityMonitor;
import cn.kjky.collector.runtime.CapacityState;
import cn.kjky.collector.runtime.CollectionSource;
import cn.kjky.collector.runtime.CollectionWatermarkStore;
import cn.kjky.collector.runtime.OutboxQuotaManager;
import cn.kjky.collector.runtime.ProductionPollScheduler;
import cn.kjky.collector.runtime.UploadLoop;
import cn.kjky.collector.runtime.UploadWatermarkStore;
import cn.kjky.collector.transport.BatchUploader;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 汇总各运行组件的线程安全只读接口，并在单个同步入口生成不可变状态快照。
 * 快照不读取配置中的 Endpoint、Secret 路径或任何事件载荷。
 */
public final class RuntimeStatusRegistry {
    private static final String COLLECTOR_VERSION = "0.1.0";

    private final CollectorConfig config;
    private final Instant startedAt;
    private final ProductionPollScheduler scheduler;
    private final CollectionWatermarkStore collectionWatermarks;
    private final UploadLoop uploadLoop;
    private final UploadWatermarkStore uploadWatermarks;
    private final OutboxStore outbox;
    private final CapacityMonitor physicalCapacity;
    private final OutboxQuotaManager logicalQuota;
    private final ManualPauseStateStore pauseStore;
    private final JobStateStore jobs;
    private OutboxStatistics lastOutboxStatistics = new OutboxStatistics(
            new OutboxStatistics.Bucket(0, 0, 0),
            new OutboxStatistics.Bucket(0, 0, 0),
            new OutboxStatistics.Bucket(0, 0, 0));

    public RuntimeStatusRegistry(CollectorConfig config, Instant startedAt,
                                 ProductionPollScheduler scheduler,
                                 CollectionWatermarkStore collectionWatermarks,
                                 UploadLoop uploadLoop, UploadWatermarkStore uploadWatermarks,
                                 OutboxStore outbox, CapacityMonitor physicalCapacity,
                                 OutboxQuotaManager logicalQuota, ManualPauseStateStore pauseStore,
                                 JobStateStore jobs) {
        this.config = Objects.requireNonNull(config, "config 不能为空");
        this.startedAt = Objects.requireNonNull(startedAt, "startedAt 不能为空");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler 不能为空");
        this.collectionWatermarks = Objects.requireNonNull(collectionWatermarks, "collectionWatermarks 不能为空");
        this.uploadLoop = uploadLoop;
        this.uploadWatermarks = uploadWatermarks;
        this.outbox = Objects.requireNonNull(outbox, "outbox 不能为空");
        this.physicalCapacity = Objects.requireNonNull(physicalCapacity, "physicalCapacity 不能为空");
        this.logicalQuota = Objects.requireNonNull(logicalQuota, "logicalQuota 不能为空");
        this.pauseStore = Objects.requireNonNull(pauseStore, "pauseStore 不能为空");
        this.jobs = Objects.requireNonNull(jobs, "jobs 不能为空");
    }

    /** 轻量变更令牌。状态线程用它发现变化，不会为每次检测扫描 Outbox 文件。 */
    public ChangeToken changeToken() {
        var pause = pauseStore.snapshot();
        var physical = physicalCapacity.snapshot();
        var quota = logicalQuota.snapshot();
        var upload = uploadLoop == null ? null : uploadLoop.status();
        var job = jobs.latestSummary().orElse(null);
        return new ChangeToken(pause.configVersion(), pause.globalPaused(), pause.pausedSources(),
                physical.state(), physical.reason(), quota.state(), quota.reason(),
                quota.configVersion(), quota.requestedQuotaBytes(), quota.effectiveQuotaBytes(),
                upload == null ? null : upload.state(), outbox.changeVersion(),
                job == null ? null : job.jobId(), job == null ? null : job.status(),
                scheduler.runtimeStatusSnapshot());
    }

    /** 以同一 sent_at 汇总全部组件；返回后内容不会再被业务线程修改。 */
    public synchronized CollectorStatusSnapshot snapshot(Instant sentAt) {
        Objects.requireNonNull(sentAt, "sentAt 不能为空");
        boolean statisticsFailed = false;
        OutboxQuotaManager.Snapshot quota;
        synchronized (outbox) {
            try {
                lastOutboxStatistics = OutboxStatistics.capture(outbox, sentAt);
            } catch (IOException | RuntimeException e) {
                statisticsFailed = true;
            }
            // Outbox 变更在同一锁内提交逻辑配额差量；两者必须取自同一观察点。
            quota = logicalQuota.snapshot();
        }
        var pause = pauseStore.snapshot();
        var physical = physicalCapacity.snapshot();
        var sourceRuntime = scheduler.runtimeStatusSnapshot();
        Map<String, CollectionWatermarkStore.Watermark> collection = collectionWatermarks.snapshot();

        EnumSet<CollectorStatusSnapshot.StatusReason> reasons =
                EnumSet.noneOf(CollectorStatusSnapshot.StatusReason.class);
        boolean manualPause = pause.globalPaused() || !pause.pausedSources().isEmpty();
        if (manualPause) reasons.add(CollectorStatusSnapshot.StatusReason.MANUAL_PAUSE);
        if (physical.state() == CapacityState.CRITICAL) {
            reasons.add(CollectorStatusSnapshot.StatusReason.PHYSICAL_CAPACITY_CRITICAL);
        }
        if (logicalQuota.state() == CapacityState.CRITICAL) {
            reasons.add(CollectorStatusSnapshot.StatusReason.LOGICAL_QUOTA_CRITICAL);
        }
        if ("CAPACITY_CHECK_FAILED".equals(physical.reason())
                || "OUTBOX_QUOTA_CHECK_FAILED".equals(quota.reason())) {
            reasons.add(CollectorStatusSnapshot.StatusReason.CAPACITY_CHECK_FAILED);
        }
        boolean authenticationPause = uploadLoop != null
                && uploadLoop.status().state() == BatchUploader.UploaderState.PAUSED_AUTHENTICATION;
        if (authenticationPause) {
            reasons.add(CollectorStatusSnapshot.StatusReason.UPLOAD_AUTHENTICATION_PAUSED);
        }
        boolean collectionDegraded = sourceRuntime.values().stream()
                .anyMatch(value -> value.consecutiveFailures() > 0);
        if (collectionDegraded) reasons.add(CollectorStatusSnapshot.StatusReason.COLLECTION_DEGRADED);
        boolean uploadDegraded = statisticsFailed || lastOutboxStatistics.blocked().count() > 0
                || lastOutboxStatistics.retry().count() > 0;
        if (uploadDegraded) reasons.add(CollectorStatusSnapshot.StatusReason.UPLOAD_DEGRADED);

        List<CollectorStatusSnapshot.SourceStatus> sources = new ArrayList<>();
        for (CollectionSource source : CollectionSource.values()) {
            boolean enabled = enabled(source);
            var runtime = sourceRuntime.getOrDefault(source,
                    new ProductionPollScheduler.SourceRuntimeStatus(false, null, null, 0));
            List<CollectorStatusSnapshot.CollectionWatermarkStatus> watermarks = collection.entrySet().stream()
                    .filter(entry -> belongsTo(source, entry.getKey()))
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> new CollectorStatusSnapshot.CollectionWatermarkStatus(entry.getKey(),
                            entry.getValue().windowEnd(), entry.getValue().lastSuccessAt()))
                    .toList();
            Instant lastWatermarkSuccess = watermarks.stream()
                    .map(CollectorStatusSnapshot.CollectionWatermarkStatus::updatedAt)
                    .max(Comparator.naturalOrder()).orElse(null);
            Instant lastSuccess = later(runtime.lastSuccessAt(), lastWatermarkSuccess);
            boolean sourcePaused = enabled && (pause.globalPaused() || pause.pausedSources().contains(source)
                    || physical.state() == CapacityState.CRITICAL || logicalQuota.state() == CapacityState.CRITICAL);
            CollectorStatusSnapshot.ComponentState state = !enabled
                    ? CollectorStatusSnapshot.ComponentState.DISABLED
                    : sourcePaused ? CollectorStatusSnapshot.ComponentState.PAUSED
                    : runtime.consecutiveFailures() > 0 ? CollectorStatusSnapshot.ComponentState.DEGRADED
                    : CollectorStatusSnapshot.ComponentState.RUNNING;
            long lag = !enabled ? 0
                    : watermarks.isEmpty() ? Math.max(0, Duration.between(startedAt, sentAt).getSeconds())
                    : watermarks.stream().mapToLong(watermark ->
                            Math.max(0, Duration.between(watermark.windowEnd(), sentAt).getSeconds())).max().orElse(0);
            sources.add(new CollectorStatusSnapshot.SourceStatus(source.name(), enabled, state,
                    lastSuccess, runtime.lastFailureAt(), runtime.consecutiveFailures(), lag, watermarks));
        }

        CollectorStatusSnapshot.UploadStatus upload = uploadStatus(authenticationPause, statisticsFailed);
        CollectorStatusSnapshot.CapacityStatus capacity = capacityStatus(physical, quota);
        CollectorStatusSnapshot.OverallStatus overall = hasPauseReason(reasons)
                ? CollectorStatusSnapshot.OverallStatus.PAUSED
                : (!reasons.isEmpty() || physical.state() != CapacityState.NORMAL
                    || logicalQuota.state() != CapacityState.NORMAL)
                ? CollectorStatusSnapshot.OverallStatus.DEGRADED
                : CollectorStatusSnapshot.OverallStatus.RUNNING;

        long configVersion = Math.max(jobs.lastAppliedConfigVersion(),
                Math.max(pause.configVersion(), quota.configVersion()));
        CollectorStatusSnapshot.JobSummary lastJob = jobs.latestSummary().map(record ->
                new CollectorStatusSnapshot.JobSummary(record.jobId(), record.commandType(),
                        record.configVersion(), record.status(), record.occurredAt(),
                        record.rejectReason())).orElse(null);

        return new CollectorStatusSnapshot(CollectorStatusSnapshot.SCHEMA_VERSION,
                config.collector.collectorId, config.collector.siteId, config.collector.collectorType,
                COLLECTOR_VERSION, CollectorStatusSnapshot.DeploymentMode.valueOf(
                        config.runtime.deploymentMode.toUpperCase(java.util.Locale.ROOT)),
                startedAt, sentAt, configVersion, overall, List.copyOf(reasons),
                sources, upload, capacity, lastJob);
    }

    private CollectorStatusSnapshot.UploadStatus uploadStatus(boolean authPause, boolean statisticsFailed) {
        CollectorStatusSnapshot.ComponentState state = uploadLoop == null
                ? CollectorStatusSnapshot.ComponentState.DISABLED
                : authPause ? CollectorStatusSnapshot.ComponentState.PAUSED
                : statisticsFailed || lastOutboxStatistics.blocked().count() > 0
                    || lastOutboxStatistics.retry().count() > 0
                ? CollectorStatusSnapshot.ComponentState.DEGRADED
                : CollectorStatusSnapshot.ComponentState.RUNNING;
        List<CollectorStatusSnapshot.UploadWatermarkStatus> watermarks = uploadWatermarks == null ? List.of()
                : uploadWatermarks.snapshot().values().stream()
                .sorted(Comparator.comparing(UploadWatermarkStore.PollWatermark::pollId))
                .map(value -> new CollectorStatusSnapshot.UploadWatermarkStatus(value.pollId(),
                        value.continuousSequence(), value.completed(), value.updatedAt()))
                .toList();
        return new CollectorStatusSnapshot.UploadStatus(state,
                uploadLoop == null ? null : uploadLoop.lastSuccessAt(),
                uploadLoop == null ? 0 : uploadLoop.inFlightBatches(),
                backlog(lastOutboxStatistics.pending()), backlog(lastOutboxStatistics.retry()),
                backlog(lastOutboxStatistics.blocked()), watermarks);
    }

    private CollectorStatusSnapshot.CapacityStatus capacityStatus(
            cn.kjky.collector.runtime.CapacityStateStore.CapacitySnapshot physical,
            OutboxQuotaManager.Snapshot quota) {
        EnumSet<CollectorStatusSnapshot.StatusReason> reasons =
                EnumSet.noneOf(CollectorStatusSnapshot.StatusReason.class);
        if (physical.state() == CapacityState.CRITICAL) {
            reasons.add(CollectorStatusSnapshot.StatusReason.PHYSICAL_CAPACITY_CRITICAL);
        }
        if (quota.state() == CapacityState.CRITICAL) {
            reasons.add(CollectorStatusSnapshot.StatusReason.LOGICAL_QUOTA_CRITICAL);
        }
        if ("CAPACITY_CHECK_FAILED".equals(physical.reason())
                || "OUTBOX_QUOTA_CHECK_FAILED".equals(quota.reason())) {
            reasons.add(CollectorStatusSnapshot.StatusReason.CAPACITY_CHECK_FAILED);
        }
        CapacityState unified = worst(physical.state(), quota.state());
        long used = Math.max(0, physical.totalBytes() - physical.usableBytes());
        return new CollectorStatusSnapshot.CapacityStatus(
                CollectorStatusSnapshot.CapacityState.valueOf(unified.name()), List.copyOf(reasons),
                physical.totalBytes(), physical.usableBytes(), used, physical.usedPercent(),
                quota.outboxUsedBytes(), quota.requestedQuotaBytes(), quota.effectiveQuotaBytes(),
                quota.localMinBytes(), quota.localMaxBytes(), quota.reservedFreeBytes(),
                Math.max(0, quota.quotaUsedPercent()));
    }

    private CollectorStatusSnapshot.BacklogStatus backlog(OutboxStatistics.Bucket value) {
        return new CollectorStatusSnapshot.BacklogStatus(value.count(), value.bytes(), value.oldestAgeSeconds());
    }

    private boolean enabled(CollectionSource source) {
        return source == CollectionSource.ARMS ? config.arms.enabled : config.sls.enabled;
    }

    private boolean belongsTo(CollectionSource source, String taskKey) {
        String upper = taskKey.toUpperCase(java.util.Locale.ROOT);
        return upper.startsWith(source.name() + "/") || upper.startsWith(source.name() + ":");
    }

    private Instant later(Instant first, Instant second) {
        if (first == null) return second;
        if (second == null) return first;
        return first.isAfter(second) ? first : second;
    }

    private boolean hasPauseReason(EnumSet<CollectorStatusSnapshot.StatusReason> reasons) {
        return reasons.contains(CollectorStatusSnapshot.StatusReason.MANUAL_PAUSE)
                || reasons.contains(CollectorStatusSnapshot.StatusReason.PHYSICAL_CAPACITY_CRITICAL)
                || reasons.contains(CollectorStatusSnapshot.StatusReason.LOGICAL_QUOTA_CRITICAL)
                || reasons.contains(CollectorStatusSnapshot.StatusReason.UPLOAD_AUTHENTICATION_PAUSED);
    }

    private CapacityState worst(CapacityState first, CapacityState second) {
        if (first == CapacityState.CRITICAL || second == CapacityState.CRITICAL) return CapacityState.CRITICAL;
        if (first == CapacityState.WARNING || second == CapacityState.WARNING) return CapacityState.WARNING;
        if (first == CapacityState.RECOVERING || second == CapacityState.RECOVERING) return CapacityState.RECOVERING;
        return CapacityState.NORMAL;
    }

    public record ChangeToken(long pauseConfigVersion, boolean globalPaused,
                              java.util.Set<CollectionSource> pausedSources,
                              CapacityState physicalState, String physicalReason,
                              CapacityState logicalState, String logicalReason,
                              long quotaConfigVersion, long requestedQuotaBytes,
                              long effectiveQuotaBytes, BatchUploader.UploaderState uploaderState,
                              long outboxChangeVersion, String lastJobId,
                              String lastJobStatus,
                              Map<CollectionSource, ProductionPollScheduler.SourceRuntimeStatus> sources) {
        public ChangeToken {
            pausedSources = java.util.Set.copyOf(pausedSources);
            sources = Map.copyOf(sources);
        }
    }
}

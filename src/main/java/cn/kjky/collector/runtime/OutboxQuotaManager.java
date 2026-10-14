package cn.kjky.collector.runtime;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.outbox.OutboxSpaceGuard;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Outbox 逻辑配额状态机。
 * <p>启动时扫描 writing/pending/retry，运行时接受 OutboxStore 的精确差量，
 * 并周期性全量校正。该组件从不修改 PVC，也从不删除 Outbox 数据。</p>
 */
public final class OutboxQuotaManager implements DaemonLoop, OutboxSpaceGuard {
    public static final String STATE_FILE_NAME = "outbox-quota-state.json";

    private final Path outboxRoot;
    private final long localMinBytes;
    private final long localMaxBytes;
    private final long reservedFreeBytes;
    private final int warningPercent;
    private final int criticalPercent;
    private final int resumePercent;
    private final int checkIntervalSeconds;
    private final RuntimePolicyStore policyStore;
    private final CapacityStateStore stateStore;
    private final DiskUsageProbe diskProbe;
    private final OutboxUsageProbe usageProbe;
    private final Clock clock;
    private final ScheduledExecutorService executor;
    private final Consumer<String> info;
    private final Consumer<String> error;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean accepting = new AtomicBoolean(true);

    private long requestedQuotaBytes;
    private long configVersion;
    private boolean remotePolicy;
    private long reservedBytes;
    private Snapshot current;

    public OutboxQuotaManager(CollectorConfig config, Path outboxRoot, Path stateRoot) throws IOException {
        this(requireQuota(config), outboxRoot,
                new RuntimePolicyStore(stateRoot.resolve(RuntimePolicyStore.FILE_NAME)),
                new CapacityStateStore(stateRoot.resolve(STATE_FILE_NAME)),
                new FileStoreDiskUsageProbe(), new OutboxUsageProbe(), Clock.systemUTC(),
                Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "collector-outbox-quota-monitor");
                    thread.setDaemon(false);
                    return thread;
                }), System.out::println, System.err::println);
    }

    OutboxQuotaManager(CollectorConfig.OutboxQuotaConfig quota, Path outboxRoot,
                       RuntimePolicyStore policyStore, CapacityStateStore stateStore,
                       DiskUsageProbe diskProbe, OutboxUsageProbe usageProbe, Clock clock,
                       ScheduledExecutorService executor, Consumer<String> info,
                       Consumer<String> error) throws IOException {
        Objects.requireNonNull(quota, "outboxQuota 不能为空");
        this.outboxRoot = Objects.requireNonNull(outboxRoot, "outboxRoot 不能为空")
                .toAbsolutePath().normalize();
        this.localMinBytes = quota.minBytes;
        this.localMaxBytes = quota.maxBytes;
        this.reservedFreeBytes = quota.reservedFreeBytes;
        this.warningPercent = quota.warningPercent;
        this.criticalPercent = quota.criticalPercent;
        this.resumePercent = quota.resumePercent;
        this.checkIntervalSeconds = quota.checkIntervalSeconds;
        this.policyStore = Objects.requireNonNull(policyStore, "policyStore 不能为空");
        this.stateStore = Objects.requireNonNull(stateStore, "stateStore 不能为空");
        this.diskProbe = Objects.requireNonNull(diskProbe, "diskProbe 不能为空");
        this.usageProbe = Objects.requireNonNull(usageProbe, "usageProbe 不能为空");
        this.clock = Objects.requireNonNull(clock, "clock 不能为空");
        this.executor = Objects.requireNonNull(executor, "executor 不能为空");
        this.info = Objects.requireNonNull(info, "info 不能为空");
        this.error = Objects.requireNonNull(error, "error 不能为空");

        var persistedPolicy = policyStore.load();
        this.remotePolicy = persistedPolicy.isPresent();
        this.requestedQuotaBytes = persistedPolicy.map(RuntimePolicyStore.RuntimePolicy::requestedQuotaBytes)
                .orElse(quota.defaultBytes);
        this.configVersion = persistedPolicy.map(RuntimePolicyStore.RuntimePolicy::configVersion).orElse(0L);
        CapacityState restored = stateStore.load().map(CapacityStateStore.CapacitySnapshot::state)
                .orElse(CapacityState.NORMAL);
        this.current = new Snapshot(restored, 0, 0, requestedQuotaBytes, 0,
                localMinBytes, localMaxBytes, reservedFreeBytes, 0, 0,
                100.0d, clock.instant(), "NOT_CHECKED", configVersion);
        try {
            checkNow();
        } catch (Exception ignored) {
            // checkNow 已经持久化 CRITICAL；构造器保留可运行的失败关闭实例。
        }
    }

    /** 构造阶段已完成首轮同步扫描；start 只启动周期全量校正。 */
    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) return;
        executor.scheduleWithFixedDelay(this::checkSafely,
                checkIntervalSeconds, checkIntervalSeconds, TimeUnit.SECONDS);
        info.accept("Outbox 逻辑配额监控已启动，状态=" + state()
                + "，实际配额=" + snapshot().effectiveQuotaBytes());
    }

    /** 全量扫描实际 Outbox 字节，同时复核 PVC 总容量和安全预留上限。 */
    public synchronized Snapshot checkNow() throws IOException {
        try {
            DiskUsageProbe.Usage disk = diskProbe.measure(outboxRoot);
            // 有 writing 预留时全量扫描会看到尚未 commit 的临时文件，延后校正可避免重复计数。
            long used = reservedBytes == 0 ? usageProbe.measure(outboxRoot) : current.outboxUsedBytes();
            updateSnapshot(used, disk, false);
            return current;
        } catch (IOException | RuntimeException failure) {
            failClosed(failure);
            if (failure instanceof IOException io) throw io;
            throw new IOException("Outbox 逻辑配额全量检查失败", failure);
        }
    }

    /**
     * 供后续 SET_OUTBOX_QUOTA 命令处理器直接调用。这里只执行本地校验、原子落盘和生效，
     * 不包含 HTTP 任务领取或状态回传。
     */
    public synchronized PolicyApplyResult applyRequestedQuota(long requestedBytes, long newConfigVersion)
            throws IOException, QuotaPolicyRejectedException {
        if (requestedBytes <= 0) {
            throw rejected(QuotaPolicyRejectedException.Reason.INVALID_PARAMETERS, "requested_quota_bytes 必须为正数");
        }
        if (newConfigVersion <= configVersion) {
            throw rejected(QuotaPolicyRejectedException.Reason.STALE_CONFIG_VERSION,
                    "config_version 必须大于最后生效版本 " + configVersion);
        }
        if (requestedBytes < localMinBytes) {
            throw rejected(QuotaPolicyRejectedException.Reason.QUOTA_BELOW_LOCAL_MIN,
                    "请求配额低于本地最小值");
        }
        if (requestedBytes > localMaxBytes) {
            throw rejected(QuotaPolicyRejectedException.Reason.QUOTA_ABOVE_LOCAL_MAX,
                    "请求配额高于本地最大值");
        }
        DiskUsageProbe.Usage disk;
        try {
            disk = diskProbe.measure(outboxRoot);
        } catch (IOException | RuntimeException failure) {
            failClosed(failure);
            if (failure instanceof IOException io) throw io;
            throw new IOException("无法确认 PVC 物理安全上限，逻辑配额未生效", failure);
        }
        long safeLimit = safeLimit(disk.totalBytes());
        if (safeLimit < requestedBytes) {
            throw rejected(QuotaPolicyRejectedException.Reason.PVC_SAFETY_LIMIT,
                    "请求配额超过 PVC 总容量扣除安全预留后的上限");
        }

        Instant appliedAt = clock.instant();
        RuntimePolicyStore.RuntimePolicy policy = RuntimePolicyStore.RuntimePolicy.quota(
                newConfigVersion, requestedBytes, appliedAt);
        policyStore.save(policy);
        requestedQuotaBytes = requestedBytes;
        configVersion = newConfigVersion;
        remotePolicy = true;
        updateSnapshot(current.outboxUsedBytes(), disk, true);
        return new PolicyApplyResult(requestedBytes, current.effectiveQuotaBytes(),
                localMinBytes, localMaxBytes, newConfigVersion, current.state());
    }

    @Override
    public synchronized Reservation reserveNewBatch(long peakBytes) throws IOException {
        requireNonNegative(peakBytes, "peakBytes");
        if (!collectionAllowed()) {
            throw new OutboxQuotaExceededException("Outbox 逻辑配额处于 CRITICAL，拒绝创建新采集批次");
        }
        long projected;
        try {
            projected = Math.addExact(Math.addExact(current.outboxUsedBytes(), reservedBytes), peakBytes);
        } catch (ArithmeticException e) {
            throw new OutboxQuotaExceededException("Outbox 空间预留发生整数溢出");
        }
        if (projected > current.effectiveQuotaBytes()) {
            throw new OutboxQuotaExceededException("Outbox 剩余逻辑配额不足，所需=" + peakBytes
                    + "，当前使用=" + current.outboxUsedBytes()
                    + "，已预留=" + reservedBytes
                    + "，配额=" + current.effectiveQuotaBytes());
        }
        reservedBytes = Math.addExact(reservedBytes, peakBytes);
        return new ManagedReservation(peakBytes);
    }

    /** 上传元数据和 413 拆分属于排空路径，不因 CRITICAL 被拒绝。 */
    @Override
    public synchronized Reservation reserveMaintenance(long peakBytes) throws IOException {
        requireNonNegative(peakBytes, "peakBytes");
        try {
            reservedBytes = Math.addExact(reservedBytes, peakBytes);
        } catch (ArithmeticException e) {
            throw new IOException("Outbox 维护空间预留发生整数溢出", e);
        }
        return new ManagedReservation(peakBytes);
    }

    /** 删除或无临时文件的持久化变更使用精确差量更新计数。 */
    @Override
    public synchronized void adjustUsage(long deltaBytes) throws IOException {
        applyUsageDelta(deltaBytes);
    }

    @Override
    public void reconcileNow() throws IOException {
        checkNow();
    }

    /** CRITICAL 或硬配额已经没有剩余空间时禁止新采集；上传不读取此门禁。 */
    @Override
    public synchronized boolean collectionAllowed() {
        if (current.state() == CapacityState.CRITICAL || current.effectiveQuotaBytes() < localMinBytes) return false;
        try {
            return Math.addExact(current.outboxUsedBytes(), reservedBytes) < current.effectiveQuotaBytes();
        } catch (ArithmeticException e) {
            return false;
        }
    }

    public synchronized CapacityState state() {
        return current.state();
    }

    public synchronized Snapshot snapshot() {
        if (current.reservedBytes() == reservedBytes) return current;
        return new Snapshot(current.state(), current.outboxUsedBytes(), reservedBytes,
                current.requestedQuotaBytes(), current.effectiveQuotaBytes(), current.localMinBytes(),
                current.localMaxBytes(), current.reservedFreeBytes(), current.pvcTotalBytes(),
                current.pvcUsableBytes(), current.quotaUsedPercent(), current.checkedAt(),
                current.reason(), current.configVersion());
    }

    public synchronized long lastAppliedConfigVersion() {
        return configVersion;
    }

    private void updateSnapshot(long used, DiskUsageProbe.Usage disk, boolean forcePersist) throws IOException {
        if (used < 0) throw new IOException("Outbox 使用量不能为负数");
        long safeLimit = safeLimit(disk.totalBytes());
        long effective = minimum(requestedQuotaBytes, localMaxBytes, safeLimit);
        double percent = effective > 0 ? used * 100.0d / effective : 100.0d;
        String hardFailure = hardFailure(safeLimit, effective);
        CapacityState before = current.state();
        CapacityState next = hardFailure == null ? transition(before, percent) : CapacityState.CRITICAL;
        String reason = hardFailure != null ? hardFailure
                : next == CapacityState.CRITICAL ? "OUTBOX_QUOTA_CRITICAL" : null;
        current = new Snapshot(next, used, reservedBytes, requestedQuotaBytes, effective,
                localMinBytes, localMaxBytes, reservedFreeBytes, disk.totalBytes(), disk.usableBytes(),
                percent, clock.instant(), reason, configVersion);
        if (forcePersist || before != next || stateStore.load().isEmpty()) persistState();
        if (before != next) logTransition(before, current);
    }

    private String hardFailure(long safeLimit, long effective) {
        if (requestedQuotaBytes <= 0 || requestedQuotaBytes < localMinBytes || requestedQuotaBytes > localMaxBytes) {
            return "POLICY_OUTSIDE_LOCAL_BOUNDS";
        }
        if (safeLimit < localMinBytes || effective < localMinBytes) return "PVC_SAFETY_LIMIT";
        if (remotePolicy && effective < requestedQuotaBytes) return "PVC_SAFETY_LIMIT";
        return null;
    }

    private CapacityState transition(CapacityState state, double usedPercent) {
        return switch (state) {
            case NORMAL -> usedPercent >= criticalPercent ? CapacityState.CRITICAL
                    : usedPercent >= warningPercent ? CapacityState.WARNING : CapacityState.NORMAL;
            case WARNING -> usedPercent >= criticalPercent ? CapacityState.CRITICAL
                    : usedPercent <= resumePercent ? CapacityState.NORMAL : CapacityState.WARNING;
            case CRITICAL -> usedPercent <= resumePercent ? CapacityState.RECOVERING : CapacityState.CRITICAL;
            case RECOVERING -> usedPercent >= criticalPercent ? CapacityState.CRITICAL
                    : usedPercent >= warningPercent ? CapacityState.WARNING
                    : usedPercent <= resumePercent ? CapacityState.NORMAL : CapacityState.RECOVERING;
        };
    }

    private void persistState() throws IOException {
        long effective = current.effectiveQuotaBytes();
        long usable = effective <= 0 ? 0 : Math.max(0, effective - Math.min(effective, current.outboxUsedBytes()));
        double persistedPercent = Math.max(0.0d, Math.min(100.0d, current.quotaUsedPercent()));
        stateStore.save(new CapacityStateStore.CapacitySnapshot(CapacityStateStore.SCHEMA_VERSION,
                current.state(), persistedPercent, Math.max(0, effective), usable,
                current.checkedAt(), current.reason()));
    }

    private void applyUsageDelta(long deltaBytes) throws IOException {
        long used;
        try {
            used = Math.addExact(current.outboxUsedBytes(), deltaBytes);
        } catch (ArithmeticException e) {
            throw new IOException("Outbox 使用量差量发生整数溢出", e);
        }
        if (used < 0) {
            // 计数漂移时宁可重新扫描，也不能用负数放宽配额。
            checkNow();
            return;
        }
        DiskUsageProbe.Usage disk = current.pvcTotalBytes() > 0
                ? new DiskUsageProbe.Usage(current.pvcTotalBytes(), current.pvcUsableBytes())
                : diskProbe.measure(outboxRoot);
        updateSnapshot(used, disk, false);
    }

    private synchronized void finishReservation(ManagedReservation reservation,
                                                long permanentDeltaBytes) throws IOException {
        if (reservation.finished) throw new IllegalStateException("Outbox 空间预留已经结束");
        reservation.finished = true;
        reservedBytes -= reservation.peakBytes;
        applyUsageDelta(permanentDeltaBytes);
    }

    private synchronized void cancelReservation(ManagedReservation reservation) {
        if (reservation.finished) return;
        reservation.finished = true;
        reservedBytes -= reservation.peakBytes;
    }

    private void checkSafely() {
        if (!accepting.get()) return;
        try {
            checkNow();
        } catch (Exception ignored) {
            // checkNow 已经失败关闭并记录有限错误摘要。
        }
    }

    private synchronized void failClosed(Exception failure) {
        Snapshot before = current;
        current = new Snapshot(CapacityState.CRITICAL, before.outboxUsedBytes(), reservedBytes,
                requestedQuotaBytes, before.effectiveQuotaBytes(), localMinBytes, localMaxBytes,
                reservedFreeBytes, before.pvcTotalBytes(), before.pvcUsableBytes(),
                before.quotaUsedPercent(), clock.instant(), "OUTBOX_QUOTA_CHECK_FAILED", configVersion);
        try {
            persistState();
        } catch (IOException persistFailure) {
            error.accept("逻辑配额检查失败且 CRITICAL 状态无法持久化，采集保持暂停: " + safe(persistFailure));
        }
        error.accept("Outbox 逻辑配额检查失败，按 CRITICAL 暂停新采集；上传继续运行: " + safe(failure));
    }

    private void logTransition(CapacityState before, Snapshot after) {
        String usage = String.format(java.util.Locale.ROOT, "%.2f%%", after.quotaUsedPercent());
        switch (after.state()) {
            case WARNING -> error.accept("Outbox 逻辑配额进入 WARNING，使用率=" + usage + "；继续采集和上传");
            case CRITICAL -> error.accept("Outbox 逻辑配额进入 CRITICAL，使用率=" + usage
                    + "，原因=" + after.reason() + "；暂停新采集，上传继续运行");
            case RECOVERING -> info.accept("Outbox 使用量下降到恢复水位，状态 " + before
                    + " -> RECOVERING，使用率=" + usage + "；恢复采集");
            case NORMAL -> info.accept("Outbox 逻辑配额恢复 NORMAL，使用率=" + usage);
        }
    }

    private QuotaPolicyRejectedException rejected(QuotaPolicyRejectedException.Reason reason, String message) {
        return new QuotaPolicyRejectedException(reason, message);
    }

    private long safeLimit(long totalBytes) {
        return totalBytes > reservedFreeBytes ? totalBytes - reservedFreeBytes : 0;
    }

    private long minimum(long first, long second, long third) {
        return Math.max(0, Math.min(first, Math.min(second, third)));
    }

    private void requireNonNegative(long value, String name) {
        if (value < 0) throw new IllegalArgumentException(name + " 不能为负数");
    }

    private String safe(Exception e) {
        String message = e.getMessage() == null ? "" : e.getMessage();
        return e.getClass().getSimpleName() + ": "
                + message.replaceAll("(?i)(accesskey|secret|password|token)[^ ,;]*", "[REDACTED]");
    }

    @Override
    public void close() {
        if (!accepting.compareAndSet(true, false)) return;
        executor.shutdownNow();
    }

    private static CollectorConfig.OutboxQuotaConfig requireQuota(CollectorConfig config) {
        Objects.requireNonNull(config, "config 不能为空");
        CollectorConfig.StorageConfig storage = Objects.requireNonNull(config.storage, "storage 不能为空");
        return Objects.requireNonNull(storage.outboxQuota, "storage.outboxQuota 不能为空");
    }

    private final class ManagedReservation implements Reservation {
        private final long peakBytes;
        private boolean finished;

        private ManagedReservation(long peakBytes) {
            this.peakBytes = peakBytes;
        }

        @Override
        public void commit(long permanentDeltaBytes) throws IOException {
            finishReservation(this, permanentDeltaBytes);
        }

        @Override
        public void close() {
            cancelReservation(this);
        }
    }

    public record Snapshot(CapacityState state, long outboxUsedBytes, long reservedBytes,
                           long requestedQuotaBytes, long effectiveQuotaBytes,
                           long localMinBytes, long localMaxBytes, long reservedFreeBytes,
                           long pvcTotalBytes, long pvcUsableBytes, double quotaUsedPercent,
                           Instant checkedAt, String reason, long configVersion) {
    }

    public record PolicyApplyResult(long requestedQuotaBytes, long effectiveQuotaBytes,
                                    long localMinBytes, long localMaxBytes, long configVersion,
                                    CapacityState state) {
    }

    public static final class OutboxQuotaExceededException extends IOException {
        public OutboxQuotaExceededException(String message) {
            super(message);
        }
    }
}

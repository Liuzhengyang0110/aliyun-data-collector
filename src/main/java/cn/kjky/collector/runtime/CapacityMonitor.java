package cn.kjky.collector.runtime;

import cn.kjky.collector.config.CollectorConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * 生产 Outbox/PVC 容量状态机。
 * <p>只控制新采集窗口是否可以启动，不接触、删除或移动 pending/retry/BLOCKED 批次，
 * 也不暂停独立的 UploadLoop。</p>
 */
public final class CapacityMonitor implements DaemonLoop {
    private final Path monitoredPath;
    private final int warningPercent;
    private final int criticalPercent;
    private final int resumePercent;
    private final int checkIntervalSeconds;
    private final CapacityStateStore store;
    private final DiskUsageProbe probe;
    private final Clock clock;
    private final ScheduledExecutorService executor;
    private final Consumer<String> info;
    private final Consumer<String> error;
    private final AtomicReference<CapacityStateStore.CapacitySnapshot> current;
    private final AtomicBoolean persisted = new AtomicBoolean();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean accepting = new AtomicBoolean(true);

    /** 使用 Outbox 所在文件系统和 state/capacity-state.json 创建生产容量监控。 */
    public CapacityMonitor(CollectorConfig config, Path monitoredPath, Path stateRoot) throws IOException {
        this(requireStorage(config), monitoredPath,
                new CapacityStateStore(stateRoot.resolve(CapacityStateStore.FILE_NAME)),
                new FileStoreDiskUsageProbe(), Clock.systemUTC(),
                Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "collector-capacity-monitor");
                    thread.setDaemon(false);
                    return thread;
                }), System.out::println, System.err::println);
    }

    /** 可注入探针、时钟和执行器的构造器，用于确定性测试。 */
    CapacityMonitor(CollectorConfig.StorageConfig storage, Path monitoredPath, CapacityStateStore store,
                    DiskUsageProbe probe, Clock clock, ScheduledExecutorService executor,
                    Consumer<String> info, Consumer<String> error) {
        this.monitoredPath = Objects.requireNonNull(monitoredPath, "monitoredPath 不能为空")
                .toAbsolutePath().normalize();
        this.warningPercent = storage.diskWarningWatermarkPercent;
        this.criticalPercent = storage.diskCriticalWatermarkPercent;
        this.resumePercent = storage.diskResumeWatermarkPercent;
        this.checkIntervalSeconds = storage.diskCheckIntervalSeconds;
        this.store = Objects.requireNonNull(store, "store 不能为空");
        this.probe = Objects.requireNonNull(probe, "probe 不能为空");
        this.clock = Objects.requireNonNull(clock, "clock 不能为空");
        this.executor = Objects.requireNonNull(executor, "executor 不能为空");
        this.info = Objects.requireNonNull(info, "info 不能为空");
        this.error = Objects.requireNonNull(error, "error 不能为空");
        CapacityStateStore.CapacitySnapshot restored = store.load().orElseGet(() ->
                new CapacityStateStore.CapacitySnapshot(CapacityStateStore.SCHEMA_VERSION,
                        CapacityState.NORMAL, 0, 0, 0, Instant.EPOCH, "NOT_CHECKED"));
        this.current = new AtomicReference<>(restored);
        this.persisted.set(store.load().isPresent());
    }

    /** 启动时同步检查一次，确保采集调度器启动前已经得到当前容量状态。 */
    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) return;
        checkSafely();
        executor.scheduleWithFixedDelay(this::checkSafely,
                checkIntervalSeconds, checkIntervalSeconds, TimeUnit.SECONDS);
        info.accept("Outbox/PVC 容量监控已启动，状态=" + state());
    }

    /** 执行一次确定性容量检查；状态变化和首次快照会原子持久化。 */
    public synchronized CapacityStateStore.CapacitySnapshot checkNow() throws IOException {
        DiskUsageProbe.Usage usage = probe.measure(monitoredPath);
        CapacityStateStore.CapacitySnapshot before = current.get();
        CapacityState next = transition(before.state(), usage.usedPercent());
        CapacityStateStore.CapacitySnapshot after = CapacityStateStore.CapacitySnapshot.measured(
                next, usage, clock.instant());
        if (!persisted.get() || before.state() != next) {
            store.save(after);
            persisted.set(true);
        }
        current.set(after);
        if (before.state() != next) logTransition(before.state(), after);
        return after;
    }

    /** CRITICAL 是唯一暂停采集的状态；WARNING 和 RECOVERING 均允许上传和采集。 */
    public boolean collectionAllowed() {
        return current.get().state() != CapacityState.CRITICAL;
    }

    public CapacityState state() {
        return current.get().state();
    }

    public CapacityStateStore.CapacitySnapshot snapshot() {
        return current.get();
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

    private void checkSafely() {
        if (!accepting.get()) return;
        try {
            checkNow();
        } catch (Exception e) {
            failClosed(e);
        }
    }

    /** 探针或状态落盘失败时按 CRITICAL 失败关闭，避免容量未知时继续创建数据。 */
    private synchronized void failClosed(Exception failure) {
        CapacityStateStore.CapacitySnapshot before = current.get();
        double used = before.totalBytes() > 0 ? before.usedPercent() : 100.0d;
        CapacityStateStore.CapacitySnapshot failed = new CapacityStateStore.CapacitySnapshot(
                CapacityStateStore.SCHEMA_VERSION, CapacityState.CRITICAL, used,
                before.totalBytes(), before.usableBytes(), clock.instant(), "CAPACITY_CHECK_FAILED");
        current.set(failed);
        try {
            store.save(failed);
            persisted.set(true);
        } catch (IOException persistFailure) {
            error.accept("容量检测失败且 CRITICAL 状态无法持久化，采集保持暂停: "
                    + safe(persistFailure));
        }
        error.accept("Outbox/PVC 容量检测失败，按 CRITICAL 暂停新采集窗口；上传继续运行: "
                + safe(failure));
    }

    private void logTransition(CapacityState before, CapacityStateStore.CapacitySnapshot after) {
        String usage = String.format(java.util.Locale.ROOT, "%.2f%%", after.usedPercent());
        switch (after.state()) {
            case WARNING -> error.accept("Outbox/PVC 容量进入 WARNING，使用率=" + usage
                    + "；继续采集和上传");
            case CRITICAL -> error.accept("Outbox/PVC 容量进入 CRITICAL，使用率=" + usage
                    + "；暂停新采集窗口，上传继续运行");
            case RECOVERING -> info.accept("Outbox/PVC 容量下降到恢复水位，状态 " + before
                    + " -> RECOVERING，使用率=" + usage + "；恢复采集");
            case NORMAL -> info.accept("Outbox/PVC 容量恢复 NORMAL，使用率=" + usage);
        }
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

    private static CollectorConfig.StorageConfig requireStorage(CollectorConfig config) {
        Objects.requireNonNull(config, "config 不能为空");
        return Objects.requireNonNull(config.storage, "storage 不能为空");
    }
}

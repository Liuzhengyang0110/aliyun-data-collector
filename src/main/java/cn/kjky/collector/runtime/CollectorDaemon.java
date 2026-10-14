package cn.kjky.collector.runtime;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.config.ConfigValidator;
import cn.kjky.collector.control.CollectionGate;
import cn.kjky.collector.control.ControlClient;
import cn.kjky.collector.control.JobExecutor;
import cn.kjky.collector.control.JobPoller;
import cn.kjky.collector.control.JobStateStore;
import cn.kjky.collector.control.ManualPauseStateStore;
import cn.kjky.collector.outbox.OutboxStore;
import cn.kjky.collector.scheduler.PollScheduler;
import cn.kjky.collector.service.CollectorEngine;
import cn.kjky.collector.transport.BatchUploader;
import cn.kjky.collector.status.RuntimeStatusRegistry;
import cn.kjky.collector.status.StatusClient;
import cn.kjky.collector.status.StatusReporter;

import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/** 管理常驻轮询的生命周期、停止信号和优雅退出。 */
public final class CollectorDaemon implements AutoCloseable {
    private final DaemonLoop collectionLoop;
    private final UploadLoop uploadLoop;
    private final CapacityMonitor capacityMonitor;
    private final OutboxQuotaManager outboxQuotaManager;
    private final DaemonLoop controlLoop;
    private final DaemonLoop statusReporter;
    private final CollectionGate collectionGate;
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    /** 根据运行模式创建开发过渡调度器或生产任务级水位调度器。 */
    public CollectorDaemon(CollectorConfig config, CollectorEngine engine) throws Exception {
        OutboxStore outbox = null;
        ProductionPollScheduler productionScheduler = null;
        ManualPauseStateStore pauseStore = null;
        JobStateStore jobStore = null;
        if (config.rawPersistenceEnabled()) {
            if (config.control.enabled) {
                throw new IllegalStateException("启用中控控制时必须关闭 raw 持久化并使用生产 Outbox 路径");
            }
            this.capacityMonitor = null;
            this.outboxQuotaManager = null;
            this.controlLoop = null;
            this.collectionGate = null;
            Path watermark = Path.of(config.effectiveDataRoot()).toAbsolutePath().normalize()
                    .resolve(config.effectiveStateDirectory()).resolve("poll-watermarks.json");
            this.collectionLoop = new PollScheduler(config, engine::collectSource,
                    new PollingWatermarkStore(watermark));
        } else {
            outbox = engine.daemonOutbox();
            this.capacityMonitor = new CapacityMonitor(config, outbox.outboxRoot(), outbox.stateRoot());
            this.outboxQuotaManager = new OutboxQuotaManager(config, outbox.outboxRoot(), outbox.stateRoot());
            outbox.attachSpaceGuard(outboxQuotaManager);
            pauseStore = new ManualPauseStateStore(
                    outbox.stateRoot().resolve(ManualPauseStateStore.FILE_NAME));
            boolean localConfigValid = new ConfigValidator().validate(config, false).isEmpty();
            this.collectionGate = new CollectionGate(pauseStore, outboxQuotaManager::collectionAllowed,
                    capacityMonitor::collectionAllowed, () -> localConfigValid);
            engine.configureProductionCollectionGate(collectionGate::collectionAllowed);
            productionScheduler = new ProductionPollScheduler(config, engine::collectProductionSource,
                    collectionGate::collectionAllowed);
            this.collectionLoop = productionScheduler;
            if (config.control.enabled) {
                jobStore = new JobStateStore(outbox.stateRoot().resolve(JobStateStore.FILE_NAME));
                JobExecutor jobExecutor = new JobExecutor(config, jobStore, pauseStore, outboxQuotaManager);
                this.controlLoop = new JobPoller(config, new ControlClient(config), jobExecutor, jobStore);
            } else {
                this.controlLoop = null;
            }
        }
        UploadWatermarkStore uploadWatermarks = null;
        if (config.upload.enabled) {
            if (outbox == null) outbox = engine.daemonOutbox();
            uploadWatermarks = new UploadWatermarkStore(
                    outbox.stateRoot().resolve(UploadWatermarkStore.FILE_NAME));
            this.uploadLoop = new UploadLoop(config,
                    new BatchUploader(config, outbox, uploadWatermarks), outbox);
        } else {
            this.uploadLoop = null;
        }
        if (!config.rawPersistenceEnabled() && config.control.enabled) {
            RuntimeStatusRegistry registry = new RuntimeStatusRegistry(config, Instant.now(),
                    productionScheduler, engine.productionWatermarks(), uploadLoop, uploadWatermarks,
                    outbox, capacityMonitor, outboxQuotaManager, pauseStore, jobStore);
            this.statusReporter = new StatusReporter(config, registry, new StatusClient(config));
        } else {
            this.statusReporter = null;
        }
    }

    /** 允许测试注入两个完全独立的循环。 */
    CollectorDaemon(DaemonLoop collectionLoop, UploadLoop uploadLoop) {
        this(null, null, null, null, null, collectionLoop, uploadLoop);
    }

    /** 允许测试同时注入容量、采集和上传三个独立循环。 */
    CollectorDaemon(CapacityMonitor capacityMonitor, DaemonLoop collectionLoop, UploadLoop uploadLoop) {
        this(capacityMonitor, null, null, null, null, collectionLoop, uploadLoop);
    }

    /** 允许测试同时注入物理与逻辑容量保护。 */
    CollectorDaemon(CapacityMonitor capacityMonitor, OutboxQuotaManager outboxQuotaManager,
                    DaemonLoop collectionLoop, UploadLoop uploadLoop) {
        this(capacityMonitor, outboxQuotaManager, null, null, null, collectionLoop, uploadLoop);
    }

    /** 允许测试验证控制、采集、上传和容量循环的隔离关闭。 */
    CollectorDaemon(CapacityMonitor capacityMonitor, OutboxQuotaManager outboxQuotaManager,
                    CollectionGate collectionGate, DaemonLoop controlLoop,
                    DaemonLoop collectionLoop, UploadLoop uploadLoop) {
        this(capacityMonitor, outboxQuotaManager, collectionGate, controlLoop, null,
                collectionLoop, uploadLoop);
    }

    /** 允许测试验证状态、控制、采集、上传和容量循环的隔离关闭。 */
    CollectorDaemon(CapacityMonitor capacityMonitor, OutboxQuotaManager outboxQuotaManager,
                    CollectionGate collectionGate, DaemonLoop controlLoop,
                    DaemonLoop statusReporter, DaemonLoop collectionLoop, UploadLoop uploadLoop) {
        this.capacityMonitor = capacityMonitor;
        this.outboxQuotaManager = outboxQuotaManager;
        this.collectionGate = collectionGate;
        this.controlLoop = controlLoop;
        this.statusReporter = statusReporter;
        this.collectionLoop = java.util.Objects.requireNonNull(collectionLoop, "collectionLoop 不能为空");
        this.uploadLoop = uploadLoop;
    }

    /** 非阻塞启动采集循环和可选上传循环；一个循环的运行失败不影响另一个。 */
    public void start() {
        if (!started.compareAndSet(false, true)) return;
        if (capacityMonitor != null) capacityMonitor.start();
        if (outboxQuotaManager != null) outboxQuotaManager.start();
        if (uploadLoop != null) uploadLoop.start();
        if (controlLoop != null) controlLoop.start();
        collectionLoop.start();
        if (statusReporter != null) statusReporter.start();
    }

    /** 启动调度并阻塞，直到 JVM 停止信号或当前线程被中断。 */
    public void runBlocking() {
        Thread hook = new Thread(this::close, "collector-daemon-shutdown");
        Runtime.getRuntime().addShutdownHook(hook);
        start();
        try {
            stopped.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            close();
        } finally {
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException ignored) {
                // JVM 已经进入关闭阶段，当前 hook 正在执行。
            }
        }
    }

    /** 停止接收新轮询并等待调度器安全结束。 */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        if (statusReporter != null) statusReporter.close();
        if (controlLoop != null) controlLoop.close();
        collectionLoop.close();
        if (uploadLoop != null) uploadLoop.close();
        if (outboxQuotaManager != null) outboxQuotaManager.close();
        if (capacityMonitor != null) capacityMonitor.close();
        stopped.countDown();
    }

    /** 401/403 的凭据问题修复后显式恢复上传；采集循环在暂停期间始终继续。 */
    public void resumeUploadAfterAuthenticationRepair() {
        if (uploadLoop == null) throw new IllegalStateException("upload.enabled=false，上传循环未启用");
        uploadLoop.resumeAfterAuthenticationRepair();
    }

    public BatchUploader.UploaderStatus uploadStatus() {
        return uploadLoop == null ? null : uploadLoop.status();
    }

    /** 开发 raw 模式未启用生产 PVC 保护时返回 null。 */
    public CapacityState capacityState() {
        return capacityMonitor == null ? null : capacityMonitor.state();
    }

    /** 开发 raw 模式未启用逻辑 Outbox 配额时返回 null。 */
    public CapacityState outboxQuotaState() {
        return outboxQuotaManager == null ? null : outboxQuotaManager.state();
    }

    /** 供后续本地控制命令处理器调用，不负责 HTTP 领取或上报。 */
    public OutboxQuotaManager outboxQuotaManager() {
        return outboxQuotaManager;
    }

    /** 开发 raw 模式未启用生产组合门禁时返回 null。 */
    public CollectionGate collectionGate() {
        return collectionGate;
    }
}

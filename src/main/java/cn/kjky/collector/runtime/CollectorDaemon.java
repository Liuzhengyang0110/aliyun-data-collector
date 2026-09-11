package cn.kjky.collector.runtime;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.outbox.OutboxStore;
import cn.kjky.collector.scheduler.PollScheduler;
import cn.kjky.collector.service.CollectorEngine;
import cn.kjky.collector.transport.BatchUploader;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/** 管理常驻轮询的生命周期、停止信号和优雅退出。 */
public final class CollectorDaemon implements AutoCloseable {
    private final DaemonLoop collectionLoop;
    private final UploadLoop uploadLoop;
    private final CapacityMonitor capacityMonitor;
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    /** 根据运行模式创建开发过渡调度器或生产任务级水位调度器。 */
    public CollectorDaemon(CollectorConfig config, CollectorEngine engine) throws Exception {
        OutboxStore outbox = null;
        if (config.rawPersistenceEnabled()) {
            this.capacityMonitor = null;
            Path watermark = Path.of(config.effectiveDataRoot()).toAbsolutePath().normalize()
                    .resolve(config.effectiveStateDirectory()).resolve("poll-watermarks.json");
            this.collectionLoop = new PollScheduler(config, engine::collectSource,
                    new PollingWatermarkStore(watermark));
        } else {
            outbox = engine.daemonOutbox();
            this.capacityMonitor = new CapacityMonitor(config, outbox.outboxRoot(), outbox.stateRoot());
            this.collectionLoop = new ProductionPollScheduler(config, engine::collectProductionSource,
                    capacityMonitor::collectionAllowed);
        }
        if (config.upload.enabled) {
            if (outbox == null) outbox = engine.daemonOutbox();
            UploadWatermarkStore watermarks = new UploadWatermarkStore(
                    outbox.stateRoot().resolve(UploadWatermarkStore.FILE_NAME));
            this.uploadLoop = new UploadLoop(config, new BatchUploader(config, outbox, watermarks), outbox);
        } else {
            this.uploadLoop = null;
        }
    }

    /** 允许测试注入两个完全独立的循环。 */
    CollectorDaemon(DaemonLoop collectionLoop, UploadLoop uploadLoop) {
        this(null, collectionLoop, uploadLoop);
    }

    /** 允许测试同时注入容量、采集和上传三个独立循环。 */
    CollectorDaemon(CapacityMonitor capacityMonitor, DaemonLoop collectionLoop, UploadLoop uploadLoop) {
        this.capacityMonitor = capacityMonitor;
        this.collectionLoop = java.util.Objects.requireNonNull(collectionLoop, "collectionLoop 不能为空");
        this.uploadLoop = uploadLoop;
    }

    /** 非阻塞启动采集循环和可选上传循环；一个循环的运行失败不影响另一个。 */
    public void start() {
        if (!started.compareAndSet(false, true)) return;
        if (capacityMonitor != null) capacityMonitor.start();
        collectionLoop.start();
        if (uploadLoop != null) uploadLoop.start();
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
        collectionLoop.close();
        if (capacityMonitor != null) capacityMonitor.close();
        if (uploadLoop != null) uploadLoop.close();
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
}

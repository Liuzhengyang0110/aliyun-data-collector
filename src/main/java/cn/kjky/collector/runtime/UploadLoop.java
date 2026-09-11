package cn.kjky.collector.runtime;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.outbox.OutboxStore;
import cn.kjky.collector.transport.BatchUploader;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * 独立于云端采集调度器的常驻上传循环。
 * <p>
 * 循环只消费 Outbox pending/到期 retry，并把单轮工作交给 BatchUploader。队列为空时
 * 等待 Outbox 变更；只有存在未来 retry 时才按 next_retry_at 定时唤醒，不进行忙轮询。
 * 采集失败、HTTP 失败或单批 BLOCKED 均不会结束该循环或 CollectorDaemon。
 * </p>
 */
public final class UploadLoop implements DaemonLoop {
    private static final Duration IDLE_SAFETY_RECHECK = Duration.ofMinutes(5);

    private final BatchUploader uploader;
    private final OutboxStore outbox;
    private final Clock clock;
    private final Duration failureDelay;
    private final int shutdownGraceSeconds;
    private final Consumer<String> info;
    private final Consumer<String> error;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final CountDownLatch terminated = new CountDownLatch(1);
    private final Thread thread;

    /** 使用 upload 重试间隔和 scheduler 关闭宽限期创建常驻上传循环。 */
    public UploadLoop(CollectorConfig config, BatchUploader uploader, OutboxStore outbox) {
        this(uploader, outbox, Clock.systemUTC(),
                Duration.ofMillis(config.upload.retryInitialDelayMillis),
                config.scheduler.shutdownGraceSeconds, System.out::println, System.err::println);
    }

    UploadLoop(BatchUploader uploader, OutboxStore outbox, Clock clock, Duration failureDelay,
               int shutdownGraceSeconds, Consumer<String> info, Consumer<String> error) {
        this.uploader = Objects.requireNonNull(uploader, "uploader 不能为空");
        this.outbox = Objects.requireNonNull(outbox, "outbox 不能为空");
        this.clock = Objects.requireNonNull(clock, "clock 不能为空");
        this.failureDelay = Objects.requireNonNull(failureDelay, "failureDelay 不能为空");
        if (failureDelay.isNegative() || failureDelay.isZero()) {
            throw new IllegalArgumentException("failureDelay 必须大于 0");
        }
        if (shutdownGraceSeconds < 1) throw new IllegalArgumentException("shutdownGraceSeconds 必须大于 0");
        this.shutdownGraceSeconds = shutdownGraceSeconds;
        this.info = Objects.requireNonNull(info, "info 不能为空");
        this.error = Objects.requireNonNull(error, "error 不能为空");
        this.thread = new Thread(this::run, "collector-upload-loop");
        this.thread.setDaemon(false);
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) return;
        thread.start();
        info.accept("cc-ingest 常驻上传循环已启动");
    }

    private void run() {
        try {
            while (accepting.get()) {
                long observedVersion = outbox.changeVersion();
                if (uploader.status().state() == BatchUploader.UploaderState.PAUSED_AUTHENTICATION) {
                    awaitChange(observedVersion, IDLE_SAFETY_RECHECK);
                    continue;
                }

                try {
                    BatchUploader.UploadCycleResult result = uploader.uploadAvailable();
                    if (result.authenticationPaused() > 0) {
                        error.accept("cc-ingest 返回 401/403，上传循环已暂停；采集循环继续运行");
                    }
                    if (result.claimed() > 0) continue;
                    awaitChange(observedVersion, waitUntilNextRetry());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    error.accept("上传循环单轮失败，批次保持在 Outbox，后续继续尝试: " + safe(e));
                    awaitChange(observedVersion, failureDelay);
                }
            }
        } finally {
            terminated.countDown();
        }
    }

    private Duration waitUntilNextRetry() throws IOException {
        Instant now = clock.instant();
        return outbox.nextRetryAt()
                .map(next -> next.isAfter(now) ? Duration.between(now, next) : Duration.ofMillis(1))
                .map(wait -> wait.compareTo(IDLE_SAFETY_RECHECK) > 0 ? IDLE_SAFETY_RECHECK : wait)
                .orElse(IDLE_SAFETY_RECHECK);
    }

    private void awaitChange(long observedVersion, Duration wait) {
        if (!accepting.get()) return;
        try {
            outbox.awaitChange(observedVersion, wait);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            accepting.set(false);
        }
    }

    /** 凭据修复后显式恢复上传并立即唤醒循环；不修改任何批次。 */
    public void resumeAfterAuthenticationRepair() {
        if (!accepting.get()) throw new IllegalStateException("上传循环已经关闭");
        uploader.resumeAfterAuthenticationRepair();
        outbox.signalAvailabilityChange();
    }

    public BatchUploader.UploaderStatus status() {
        return uploader.status();
    }

    public boolean running() {
        return started.get() && terminated.getCount() != 0;
    }

    boolean awaitTermination(Duration timeout) throws InterruptedException {
        return terminated.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        if (!accepting.compareAndSet(true, false)) return;
        outbox.signalAvailabilityChange();
        if (!started.get()) terminated.countDown();
        if (started.get() && Thread.currentThread() != thread) {
            try {
                if (!terminated.await(shutdownGraceSeconds, TimeUnit.SECONDS)) {
                    error.accept("等待上传安全结束超时，将中断在途上传");
                    thread.interrupt();
                    terminated.await(Math.min(shutdownGraceSeconds, 5), TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                thread.interrupt();
                Thread.currentThread().interrupt();
            }
        }
        uploader.close();
    }

    private String safe(Exception e) {
        String message = e.getMessage() == null ? "" : e.getMessage();
        return e.getClass().getSimpleName() + ": "
                + message.replaceAll("(?i)(accesskey|secret|password|token)[^ ,;]*", "[REDACTED]");
    }
}

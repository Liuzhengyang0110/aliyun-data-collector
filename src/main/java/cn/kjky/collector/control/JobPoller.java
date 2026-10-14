package cn.kjky.collector.control;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.runtime.DaemonLoop;

import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** 单线程主动领取控制任务；网络失败仅触发有界退避，不影响采集和上传循环。 */
public final class JobPoller implements DaemonLoop {
    private static final String COLLECTOR_VERSION = "0.1.0";

    private final CollectorConfig config;
    private final ControlClient client;
    private final JobExecutor executor;
    private final JobStateStore jobs;
    private final ScheduledExecutorService scheduler;
    private final Random random;
    private final Clock clock;
    private final Consumer<String> info;
    private final Consumer<String> error;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private int consecutiveFailures;

    public JobPoller(CollectorConfig config, ControlClient client,
                     JobExecutor executor, JobStateStore jobs) {
        this(config, client, executor, jobs,
                Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "collector-control-job-poller");
                    thread.setDaemon(false);
                    return thread;
                }), new Random(), Clock.systemUTC(), System.out::println, System.err::println);
    }

    JobPoller(CollectorConfig config, ControlClient client, JobExecutor executor, JobStateStore jobs,
              ScheduledExecutorService scheduler, Random random, Clock clock,
              Consumer<String> info, Consumer<String> error) {
        this.config = Objects.requireNonNull(config, "config 不能为空");
        this.client = Objects.requireNonNull(client, "client 不能为空");
        this.executor = Objects.requireNonNull(executor, "executor 不能为空");
        this.jobs = Objects.requireNonNull(jobs, "jobs 不能为空");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler 不能为空");
        this.random = Objects.requireNonNull(random, "random 不能为空");
        this.clock = Objects.requireNonNull(clock, "clock 不能为空");
        this.info = Objects.requireNonNull(info, "info 不能为空");
        this.error = Objects.requireNonNull(error, "error 不能为空");
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) return;
        schedule(initialDelayMillis());
        info.accept("中控任务主动领取已启动；采集端未暴露入站控制接口");
    }

    /** 单次循环供确定性测试调用。 */
    void pollOnce() {
        if (!accepting.get()) return;
        Long retryAfter = null;
        boolean failed = false;
        try {
            flushPendingReports();
            ControlProtocol.ClaimRequest request = new ControlProtocol.ClaimRequest(
                    ControlProtocol.SCHEMA_VERSION, config.collector.collectorId,
                    config.collector.siteId, COLLECTOR_VERSION,
                    executor.lastAppliedConfigVersion(), executor.lastCompletedJobId());
            var claimed = client.claim(request);
            if (claimed.isPresent()) executor.execute(claimed.get(), this::report);
            consecutiveFailures = 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failed = true;
        } catch (ControlClient.ControlException e) {
            failed = true;
            retryAfter = e.retryAfterMillis();
            error.accept(safe(e));
        } catch (Exception e) {
            failed = true;
            error.accept("中控控制循环失败，采集和上传继续运行: " + safe(e));
        } finally {
            if (accepting.get()) {
                long delay = failed ? retryDelayMillis(retryAfter) : normalDelayMillis();
                schedule(delay);
            }
        }
    }

    private void flushPendingReports() throws IOException, InterruptedException {
        List<JobStateStore.JobRecord> pending = jobs.pendingReports();
        for (JobStateStore.JobRecord record : pending) {
            ControlProtocol.StatusUpdate update = new ControlProtocol.StatusUpdate(
                    ControlProtocol.SCHEMA_VERSION, record.jobId(), record.collectorId(), record.siteId(),
                    record.commandType(), record.configVersion(), record.status(), record.occurredAt(),
                    record.requestedQuotaBytes(), record.effectiveQuotaBytes(), record.localMinBytes(),
                    record.localMaxBytes(), record.rejectReason(), record.message());
            try {
                client.report(update);
                jobs.markReported(record.jobId(), record.status(), record.occurredAt());
            } catch (ControlClient.ControlException e) {
                if (e.kind() == ControlClient.FailureKind.NOT_FOUND
                        || e.kind() == ControlClient.FailureKind.CONFLICT
                        || e.kind() == ControlClient.FailureKind.PERMANENT) {
                    jobs.markReported(record.jobId(), record.status(), record.occurredAt());
                    error.accept("停止重试无法被中控接受的任务状态 " + record.jobId() + ": " + safe(e));
                    continue;
                }
                throw e;
            }
        }
    }

    private boolean report(ControlProtocol.StatusUpdate update) {
        if (!accepting.get()) return false;
        try {
            client.report(update);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ControlClient.ControlException e) {
            if (e.kind() == ControlClient.FailureKind.NOT_FOUND
                    || e.kind() == ControlClient.FailureKind.CONFLICT
                    || e.kind() == ControlClient.FailureKind.PERMANENT) {
                error.accept("任务状态未被中控接受，将停止该状态的重试: " + safe(e));
                return true;
            }
            error.accept("任务状态回传暂时失败，将幂等重试: " + safe(e));
            return false;
        } catch (IOException e) {
            error.accept("任务状态回传网络失败，将幂等重试: " + safe(e));
            return false;
        }
    }

    private void schedule(long delayMillis) {
        if (!accepting.get()) return;
        try {
            scheduler.schedule(this::pollOnce, Math.max(0, delayMillis), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            if (accepting.get()) error.accept("控制任务调度被拒绝: " + safe(e));
        }
    }

    private long initialDelayMillis() {
        int jitter = config.control.taskPollJitterSeconds;
        return jitter == 0 ? 0 : random.nextInt(jitter * 1000 + 1);
    }

    private long normalDelayMillis() {
        long base = config.control.taskPollIntervalSeconds * 1000L;
        long jitter = config.control.taskPollJitterSeconds * 1000L;
        if (jitter == 0) return base;
        long offset = (long) (random.nextDouble() * (jitter * 2 + 1)) - jitter;
        return Math.max(1000, base + offset);
    }

    private long retryDelayMillis(Long serverDelay) {
        consecutiveFailures = Math.min(30, consecutiveFailures + 1);
        long base;
        if (serverDelay != null) {
            base = serverDelay;
        } else {
            int shift = Math.min(20, consecutiveFailures - 1);
            long factor = 1L << shift;
            try { base = Math.multiplyExact(config.control.retryInitialDelayMillis, factor); }
            catch (ArithmeticException ignored) { base = config.control.retryMaxDelayMillis; }
        }
        base = Math.min(config.control.retryMaxDelayMillis, Math.max(1, base));
        double spread = base * config.control.retryJitterRatio;
        long jitter = spread == 0 ? 0 : (long) ((random.nextDouble() * 2 - 1) * spread);
        return Math.max(1, Math.min(config.control.retryMaxDelayMillis, base + jitter));
    }

    private String safe(Exception e) {
        String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        return message.replaceAll("(?i)(accesskey|secret|password|token|private[_ -]?key)[^ ,;]*", "[REDACTED]");
    }

    @Override
    public void close() {
        if (!accepting.compareAndSet(true, false)) return;
        scheduler.shutdownNow();
        client.close();
        try {
            scheduler.awaitTermination(Math.min(5, config.control.requestTimeoutSeconds), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

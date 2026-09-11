package cn.kjky.collector.scheduler;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.runtime.CollectionSource;
import cn.kjky.collector.runtime.PollingWatermarkStore;
import cn.kjky.collector.runtime.DaemonLoop;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.EnumMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** 为 ARMS 和 SLS 建立相互独立、失败隔离的周期轮询。 */
public final class PollScheduler implements DaemonLoop {
    /** 便于生产编排器和测试替身共同接入的最小采集接口。 */
    @FunctionalInterface
    public interface SourceCollector {
        void collect(CollectionSource source, Instant start, Instant end) throws Exception;
    }

    private final CollectorConfig config;
    private final SourceCollector collector;
    private final PollingWatermarkStore watermarks;
    private final Clock clock;
    private final ScheduledExecutorService executor;
    private final Random random;
    private final Consumer<String> info;
    private final Consumer<String> error;
    private final Map<CollectionSource, AtomicBoolean> running = new EnumMap<>(CollectionSource.class);
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean accepting = new AtomicBoolean(true);

    /** 使用系统 UTC 时钟和两个后台线程创建生产调度器。 */
    public PollScheduler(CollectorConfig config, SourceCollector collector, PollingWatermarkStore watermarks) {
        this(config, collector, watermarks, Clock.systemUTC(),
                Executors.newScheduledThreadPool(2, runnable -> {
                    Thread thread = new Thread(runnable, "collector-poll-scheduler");
                    thread.setDaemon(false);
                    return thread;
                }), new Random(), System.out::println, System.err::println);
    }

    /** 可注入时钟、执行器和随机源的构造器，主要用于确定性测试。 */
    PollScheduler(CollectorConfig config, SourceCollector collector, PollingWatermarkStore watermarks,
                  Clock clock, ScheduledExecutorService executor, Random random,
                  Consumer<String> info, Consumer<String> error) {
        this.config = config;
        this.collector = collector;
        this.watermarks = watermarks;
        this.clock = clock;
        this.executor = executor;
        this.random = random;
        this.info = info;
        this.error = error;
        for (CollectionSource source : CollectionSource.values()) running.put(source, new AtomicBoolean());
    }

    /** 启动所有已启用数据源；重复调用不会重复创建任务。 */
    public void start() {
        if (!started.compareAndSet(false, true)) return;
        if (config.arms.enabled) scheduleNext(CollectionSource.ARMS, true);
        if (config.sls.enabled) scheduleNext(CollectionSource.SLS, true);
        info.accept("常驻轮询已启动");
    }

    /** 执行一轮数据源查询。异常只记录，调度线程会继续安排下一轮。 */
    void pollOnce(CollectionSource source) {
        if (!accepting.get() || !enabled(source)) return;
        AtomicBoolean sourceRunning = running.get(source);
        if (!sourceRunning.compareAndSet(false, true)) {
            info.accept(source + " 上一轮尚未结束，本轮跳过");
            return;
        }
        try {
            CollectorConfig.SourceScheduleConfig schedule = schedule(source);
            Instant queryEnd = clock.instant().truncatedTo(ChronoUnit.SECONDS)
                    .minusSeconds(schedule.visibilityDelaySeconds);
            Instant queryStart = watermarks.startFor(source,
                    queryEnd.minusSeconds(schedule.initialLookbackSeconds));
            if (!queryStart.isBefore(queryEnd)) {
                info.accept(source + " 当前查询终点未超过水位，等待系统时间追平");
                return;
            }
            collector.collect(source, queryStart, queryEnd);
            watermarks.advance(source, queryEnd);
            info.accept(source + " 轮询成功: " + queryStart + " - " + queryEnd);
        } catch (Exception e) {
            error.accept(source + " 轮询失败，水位未推进，后续轮次将重试: " + safe(e));
        } finally {
            sourceRunning.set(false);
        }
    }

    /** 安排下一次单次任务；每轮结束后重新计算随机抖动。 */
    private void scheduleNext(CollectionSource source, boolean initial) {
        if (!accepting.get()) return;
        long delay = initial ? initialDelay() : nextDelay(schedule(source).pollIntervalSeconds);
        try {
            executor.schedule(() -> {
                try {
                    pollOnce(source);
                } finally {
                    scheduleNext(source, false);
                }
            }, delay, TimeUnit.SECONDS);
        } catch (RejectedExecutionException e) {
            if (accepting.get()) error.accept(source + " 调度任务被拒绝: " + safe(e));
        }
    }

    /** 首轮只增加 0..jitter 的随机延迟，避免多个实例同时启动请求。 */
    private long initialDelay() {
        int jitter = config.scheduler.jitterSeconds;
        return jitter == 0 ? 0 : random.nextInt(jitter + 1);
    }

    /** 后续轮次在基础间隔两侧增加随机抖动，且保证至少等待 1 秒。 */
    private long nextDelay(int baseSeconds) {
        int jitter = config.scheduler.jitterSeconds;
        int offset = jitter == 0 ? 0 : random.nextInt(jitter * 2 + 1) - jitter;
        return Math.max(1, (long) baseSeconds + offset);
    }

    private boolean enabled(CollectionSource source) {
        return source == CollectionSource.ARMS ? config.arms.enabled : config.sls.enabled;
    }

    private CollectorConfig.SourceScheduleConfig schedule(CollectionSource source) {
        return source == CollectionSource.ARMS ? config.scheduler.arms : config.scheduler.sls;
    }

    private String safe(Exception e) {
        String message = e.getMessage() == null ? "" : e.getMessage();
        return e.getClass().getSimpleName() + ": "
                + message.replaceAll("(?i)(accesskey|secret|password|token)[^ ,;]*", "[REDACTED]");
    }

    /** 停止创建新轮询，并等待正在执行的采集在宽限期内完成。 */
    @Override
    public void close() {
        if (!accepting.compareAndSet(true, false)) return;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(config.scheduler.shutdownGraceSeconds, TimeUnit.SECONDS)) {
                error.accept("等待采集安全结束超时，将中断剩余调度线程");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

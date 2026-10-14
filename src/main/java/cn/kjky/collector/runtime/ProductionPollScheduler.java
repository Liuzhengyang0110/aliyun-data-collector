package cn.kjky.collector.runtime;

import cn.kjky.collector.config.CollectorConfig;

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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

/**
 * 生产模式常驻调度器。
 * <p>它只计算各数据源的可见查询终点；每个任务自己的起点由 CollectionWatermarkStore 决定，
 * 因而不会再使用过渡性 PollingWatermarkStore 的整源水位。</p>
 */
public final class ProductionPollScheduler implements DaemonLoop {
    @FunctionalInterface
    public interface SourceCollector {
        void collect(CollectionSource source, Instant queryEnd, int initialLookbackSeconds) throws Exception;
    }

    private final CollectorConfig config;
    private final SourceCollector collector;
    private final Clock clock;
    private final ScheduledExecutorService executor;
    private final Random random;
    private final Consumer<String> info;
    private final Consumer<String> error;
    private final Predicate<CollectionSource> collectionAllowed;
    private final Map<CollectionSource, AtomicBoolean> running = new EnumMap<>(CollectionSource.class);
    private final Map<CollectionSource, AtomicReference<SourceRuntimeStatus>> runtime =
            new EnumMap<>(CollectionSource.class);
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean accepting = new AtomicBoolean(true);

    public ProductionPollScheduler(CollectorConfig config, SourceCollector collector) {
        this(config, collector, () -> true);
    }

    /** 创建带 Outbox/PVC 容量门控的生产调度器。 */
    public ProductionPollScheduler(CollectorConfig config, SourceCollector collector,
                                   BooleanSupplier collectionAllowed) {
        this(config, collector, ignored -> collectionAllowed.getAsBoolean());
    }

    /** 创建带按数据源组合门控的生产调度器。 */
    public ProductionPollScheduler(CollectorConfig config, SourceCollector collector,
                                   Predicate<CollectionSource> collectionAllowed) {
        this(config, collector, Clock.systemUTC(), Executors.newScheduledThreadPool(2, runnable -> {
            Thread thread = new Thread(runnable, "collector-production-poll-scheduler");
            thread.setDaemon(false);
            return thread;
        }), new Random(), System.out::println, System.err::println, collectionAllowed);
    }

    ProductionPollScheduler(CollectorConfig config, SourceCollector collector, Clock clock,
                            ScheduledExecutorService executor, Random random,
                            Consumer<String> info, Consumer<String> error) {
        this(config, collector, clock, executor, random, info, error, () -> true);
    }

    ProductionPollScheduler(CollectorConfig config, SourceCollector collector, Clock clock,
                            ScheduledExecutorService executor, Random random,
                            Consumer<String> info, Consumer<String> error,
                            BooleanSupplier collectionAllowed) {
        this(config, collector, clock, executor, random, info, error,
                ignored -> collectionAllowed.getAsBoolean());
    }

    ProductionPollScheduler(CollectorConfig config, SourceCollector collector, Clock clock,
                            ScheduledExecutorService executor, Random random,
                            Consumer<String> info, Consumer<String> error,
                            Predicate<CollectionSource> collectionAllowed) {
        this.config = config;
        this.collector = collector;
        this.clock = clock;
        this.executor = executor;
        this.random = random;
        this.info = info;
        this.error = error;
        this.collectionAllowed = java.util.Objects.requireNonNull(collectionAllowed,
                "collectionAllowed 不能为空");
        for (CollectionSource source : CollectionSource.values()) {
            running.put(source, new AtomicBoolean());
            runtime.put(source, new AtomicReference<>(SourceRuntimeStatus.initial()));
        }
    }

    public void start() {
        if (!started.compareAndSet(false, true)) return;
        if (config.arms.enabled) scheduleNext(CollectionSource.ARMS, true);
        if (config.sls.enabled) scheduleNext(CollectionSource.SLS, true);
        info.accept("生产模式常驻轮询已启动（任务级采集水位）");
    }

    void pollOnce(CollectionSource source) {
        if (!accepting.get() || !enabled(source)) return;
        AtomicBoolean sourceRunning = running.get(source);
        if (!sourceRunning.compareAndSet(false, true)) {
            info.accept(source + " 上一轮尚未结束，本轮跳过");
            return;
        }
        try {
            updateRunning(source, true);
            if (!collectionAllowed.test(source)) {
                info.accept(source + " 因人工暂停、容量 CRITICAL 或本地配置无效，未开始新查询窗口；上传继续运行");
                return;
            }
            CollectorConfig.SourceScheduleConfig schedule = schedule(source);
            Instant queryEnd = clock.instant().truncatedTo(ChronoUnit.SECONDS)
                    .minusSeconds(schedule.visibilityDelaySeconds);
            collector.collect(source, queryEnd, schedule.initialLookbackSeconds);
            runtime.get(source).set(new SourceRuntimeStatus(false, clock.instant(), null, 0));
            info.accept(source + " 生产轮询完成，任务水位已按 pending 批次分别推进至 " + queryEnd);
        } catch (Exception e) {
            SourceRuntimeStatus before = runtime.get(source).get();
            runtime.get(source).set(new SourceRuntimeStatus(false, before.lastSuccessAt(), clock.instant(),
                    Math.min(Integer.MAX_VALUE, before.consecutiveFailures() + 1)));
            error.accept(source + " 生产轮询失败，受影响任务水位未推进，后续轮次将重试: " + safe(e));
        } finally {
            sourceRunning.set(false);
            SourceRuntimeStatus before = runtime.get(source).get();
            if (before.running()) updateRunning(source, false);
        }
    }

    private void updateRunning(CollectionSource source, boolean value) {
        runtime.get(source).updateAndGet(before -> new SourceRuntimeStatus(value,
                before.lastSuccessAt(), before.lastFailureAt(), before.consecutiveFailures()));
    }

    /** 供状态上报线程读取的不可变、线程安全数据源运行摘要。 */
    public Map<CollectionSource, SourceRuntimeStatus> runtimeStatusSnapshot() {
        Map<CollectionSource, SourceRuntimeStatus> result = new EnumMap<>(CollectionSource.class);
        runtime.forEach((source, value) -> result.put(source, value.get()));
        return Map.copyOf(result);
    }

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

    private long initialDelay() {
        int jitter = config.scheduler.jitterSeconds;
        return jitter == 0 ? 0 : random.nextInt(jitter + 1);
    }

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

    @Override
    public void close() {
        if (!accepting.compareAndSet(true, false)) return;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(config.scheduler.shutdownGraceSeconds, TimeUnit.SECONDS)) {
                error.accept("等待生产采集安全结束超时，将中断剩余调度线程");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public record SourceRuntimeStatus(boolean running, Instant lastSuccessAt,
                                      Instant lastFailureAt, int consecutiveFailures) {
        static SourceRuntimeStatus initial() { return new SourceRuntimeStatus(false, null, null, 0); }
    }
}

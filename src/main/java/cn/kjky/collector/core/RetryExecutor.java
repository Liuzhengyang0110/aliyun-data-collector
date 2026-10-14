package cn.kjky.collector.core;

import cn.kjky.collector.config.CollectorConfig;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

/** 云 API 查询专用重试器；不复用上传、控制或状态上报的策略及状态。 */
public final class RetryExecutor {
    private static final Logger LOGGER = Logger.getLogger(RetryExecutor.class.getName());

    @FunctionalInterface public interface Sleeper { void sleep(Duration duration) throws InterruptedException; }
    @FunctionalInterface public interface JitterSource { double nextDouble(); }
    @FunctionalInterface public interface RetryObserver { void onFailure(RetryEvent event); }

    /** 测试及安全日志可观察的脱敏事件。 */
    public record RetryEvent(Instant timestamp, String system, String task, int attempt,
                             QueryErrorCategory category, String safeReason,
                             long nextWaitMillis, boolean willRetry) {}

    private final String system;
    private final int maxRetries;
    private final long baseMillis;
    private final long maxDelayMillis;
    private final double jitterRatio;
    private final Clock clock;
    private final Sleeper sleeper;
    private final JitterSource jitter;
    private final RetryObserver observer;
    private final CloudQueryErrorClassifier classifier;

    /** 兼容原有测试和调用；生产代码应使用 {@link #forCloud(String, CollectorConfig.RuntimeConfig)}。 */
    public RetryExecutor(int maxRetries, long baseMillis) {
        this("CLOUD", maxRetries, baseMillis, 30_000L, 0d, Clock.systemUTC(),
                duration -> Thread.sleep(duration.toMillis()), () -> ThreadLocalRandom.current().nextDouble(),
                RetryExecutor::logEvent, new CloudQueryErrorClassifier());
    }

    public RetryExecutor(String system, int maxRetries, long baseMillis, long maxDelayMillis,
                         double jitterRatio, Clock clock, Sleeper sleeper, JitterSource jitter,
                         RetryObserver observer, CloudQueryErrorClassifier classifier) {
        if (maxRetries < 0) throw new IllegalArgumentException("maxRetries 不能小于 0");
        if (baseMillis < 0 || maxDelayMillis < 0 || maxDelayMillis < baseMillis) {
            throw new IllegalArgumentException("退避时间边界无效");
        }
        if (jitterRatio < 0d || jitterRatio > 1d) throw new IllegalArgumentException("jitterRatio 必须在 0..1");
        this.system = Objects.requireNonNull(system, "system");
        this.maxRetries = maxRetries;
        this.baseMillis = baseMillis;
        this.maxDelayMillis = maxDelayMillis;
        this.jitterRatio = jitterRatio;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
        this.jitter = Objects.requireNonNull(jitter, "jitter");
        this.observer = Objects.requireNonNull(observer, "observer");
        this.classifier = Objects.requireNonNull(classifier, "classifier");
    }

    /** 为一个外部云系统创建独立、无共享可变状态的重试器。 */
    public static RetryExecutor forCloud(String system, CollectorConfig.RuntimeConfig runtime) {
        return new RetryExecutor(system, runtime.maxRetries, runtime.retryBaseMillis,
                runtime.retryMaxDelayMillis, runtime.retryJitterRatio, Clock.systemUTC(),
                duration -> Thread.sleep(duration.toMillis()), () -> ThreadLocalRandom.current().nextDouble(),
                RetryExecutor::logEvent, new CloudQueryErrorClassifier());
    }

    /** 达到上限或遇到不可重试/鉴权错误时原样抛出，由窗口层阻止水位推进。 */
    public <T> T execute(String task, Callable<T> call) throws Exception {
        for (int attempt = 1; attempt <= maxRetries + 1; attempt++) {
            try {
                return call.call();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (Exception failure) {
                CloudQueryErrorClassifier.Classification classified = classifier.classify(failure);
                boolean retry = classified.retryable() && attempt <= maxRetries;
                long wait = retry ? delayMillis(attempt, classified.retryAfter()) : 0L;
                observer.onFailure(new RetryEvent(clock.instant(), system, task, attempt,
                        classified.category(), classified.safeReason(), wait, retry));
                if (!retry) throw failure;
                sleeper.sleep(Duration.ofMillis(wait));
            }
        }
        throw new IllegalStateException("查询重试循环异常退出");
    }

    /** 第 n 次失败后的等待：min(max, base*2^(n-1))，随后施加对称抖动。 */
    public long delayMillis(int failedAttempt, String retryAfter) {
        if (failedAttempt < 1) throw new IllegalArgumentException("failedAttempt 必须大于 0");
        Long serverDelay = parseRetryAfterMillis(retryAfter);
        if (serverDelay != null) return Math.min(maxDelayMillis, Math.max(0L, serverDelay));
        long exponential = cappedExponential(failedAttempt);
        if (exponential == 0L || jitterRatio == 0d) return exponential;
        double random = Math.max(0d, Math.min(1d, jitter.nextDouble()));
        double factor = 1d - jitterRatio + random * (2d * jitterRatio);
        return Math.min(maxDelayMillis, Math.max(0L, Math.round(exponential * factor)));
    }

    private long cappedExponential(int failedAttempt) {
        long delay = baseMillis;
        for (int i = 1; i < failedAttempt && delay < maxDelayMillis; i++) {
            delay = delay > maxDelayMillis / 2 ? maxDelayMillis : delay * 2;
        }
        return Math.min(delay, maxDelayMillis);
    }

    private Long parseRetryAfterMillis(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return Math.multiplyExact(Math.max(0L, Long.parseLong(value.trim())), 1000L);
        } catch (NumberFormatException | ArithmeticException ignored) {
            try {
                Instant retryAt = ZonedDateTime.parse(value.trim(), DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
                return Math.max(0L, Duration.between(clock.instant(), retryAt).toMillis());
            } catch (DateTimeParseException ignoredDate) {
                return null;
            }
        }
    }

    private static void logEvent(RetryEvent event) {
        Level level = event.willRetry() ? Level.WARNING : Level.SEVERE;
        LOGGER.log(level, () -> "cloud_query_failure system=" + event.system()
                + " task=" + safeTask(event.task()) + " attempt=" + event.attempt()
                + " category=" + event.category() + " reason=" + event.safeReason()
                + " next_wait_ms=" + event.nextWaitMillis());
    }

    private static String safeTask(String task) {
        if (task == null) return "unknown";
        String sanitized = task.replaceAll("[^A-Za-z0-9_.:/ -]", "_");
        return sanitized.length() <= 160 ? sanitized : sanitized.substring(0, 160);
    }
}

package cn.kjky.collector.transport;

import cn.kjky.collector.config.CollectorConfig;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * cc-ingest 上传重试策略。
 * <p>
 * 普通暂时性故障使用有上限的带抖动指数退避；429 返回有效 Retry-After 时严格采用
 * 服务端给出的整数秒。达到告警次数只产生告警标志，不停止重试或删除批次。
 * </p>
 */
public final class UploadRetryPolicy {
    private final long initialDelayMillis;
    private final long maxDelayMillis;
    private final double jitterRatio;
    private final int warningAttempts;
    private final DoubleSupplier random;

    public UploadRetryPolicy(CollectorConfig.UploadConfig config) {
        this(require(config).retryInitialDelayMillis, config.retryMaxDelayMillis,
                config.retryJitterRatio, config.retryWarningAttempts,
                () -> ThreadLocalRandom.current().nextDouble());
    }

    /** 允许测试注入稳定随机数。random 必须返回 [0,1)。 */
    UploadRetryPolicy(long initialDelayMillis, long maxDelayMillis, double jitterRatio,
                      int warningAttempts, DoubleSupplier random) {
        if (initialDelayMillis < 1) throw new IllegalArgumentException("initialDelayMillis 必须大于 0");
        if (maxDelayMillis < initialDelayMillis) {
            throw new IllegalArgumentException("maxDelayMillis 不能小于 initialDelayMillis");
        }
        if (!Double.isFinite(jitterRatio) || jitterRatio < 0.0d || jitterRatio > 1.0d) {
            throw new IllegalArgumentException("jitterRatio 必须在 0..1");
        }
        if (warningAttempts < 1) throw new IllegalArgumentException("warningAttempts 必须大于 0");
        this.initialDelayMillis = initialDelayMillis;
        this.maxDelayMillis = maxDelayMillis;
        this.jitterRatio = jitterRatio;
        this.warningAttempts = warningAttempts;
        this.random = Objects.requireNonNull(random, "random 不能为空");
    }

    /**
     * 计算本次失败应写入 delivery.json 的下次重试时间。
     *
     * @param now 当前时间
     * @param completedAttempts 当前批次已经保存的失败尝试次数
     * @param retryAfterSeconds 429 的整数秒 Retry-After；其他情况传 null
     */
    public RetryDecision next(Instant now, int completedAttempts, Long retryAfterSeconds) {
        Objects.requireNonNull(now, "now 不能为空");
        if (completedAttempts < 0) throw new IllegalArgumentException("completedAttempts 不能小于 0");
        int nextAttempt = completedAttempts == Integer.MAX_VALUE ? Integer.MAX_VALUE : completedAttempts + 1;

        long delayMillis;
        boolean fromRetryAfter = retryAfterSeconds != null && retryAfterSeconds >= 0;
        if (fromRetryAfter) {
            try {
                delayMillis = Math.multiplyExact(retryAfterSeconds, 1000L);
            } catch (ArithmeticException e) {
                delayMillis = Long.MAX_VALUE;
            }
        } else {
            long exponential = initialDelayMillis;
            for (int i = 1; i < nextAttempt && exponential < maxDelayMillis; i++) {
                exponential = exponential > maxDelayMillis / 2 ? maxDelayMillis : exponential * 2;
            }
            exponential = Math.min(exponential, maxDelayMillis);
            double sample = random.getAsDouble();
            if (!(sample >= 0.0d && sample < 1.0d)) {
                throw new IllegalStateException("random 必须返回 [0,1)");
            }
            double multiplier = 1.0d - jitterRatio + (2.0d * jitterRatio * sample);
            delayMillis = Math.max(1L, Math.min(maxDelayMillis, Math.round(exponential * multiplier)));
        }

        Duration delay = Duration.ofMillis(delayMillis);
        Instant nextRetryAt;
        try {
            nextRetryAt = now.plus(delay);
        } catch (RuntimeException e) {
            nextRetryAt = Instant.MAX;
        }
        return new RetryDecision(nextAttempt, delay, nextRetryAt,
                nextAttempt >= warningAttempts, fromRetryAfter);
    }

    private static CollectorConfig.UploadConfig require(CollectorConfig.UploadConfig config) {
        return Objects.requireNonNull(config, "upload 配置不能为空");
    }

    /** 单次重试调度结果；warning 不会终止后续重试。 */
    public record RetryDecision(
            int attempt,
            Duration delay,
            Instant nextRetryAt,
            boolean warning,
            boolean retryAfterApplied) {
    }
}

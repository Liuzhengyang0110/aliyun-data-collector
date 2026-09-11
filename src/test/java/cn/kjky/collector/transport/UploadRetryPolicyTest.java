package cn.kjky.collector.transport;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class UploadRetryPolicyTest {
    private static final Instant NOW = Instant.parse("2026-09-03T06:00:00Z");

    @Test
    void appliesCappedExponentialBackoffWithDeterministicJitter() {
        UploadRetryPolicy policy = new UploadRetryPolicy(1000, 5000, 0.2, 3, () -> 0.5);

        assertThat(policy.next(NOW, 0, null).delay().toMillis()).isEqualTo(1000);
        assertThat(policy.next(NOW, 1, null).delay().toMillis()).isEqualTo(2000);
        UploadRetryPolicy.RetryDecision third = policy.next(NOW, 2, null);
        assertThat(third.delay().toMillis()).isEqualTo(4000);
        assertThat(third.warning()).isTrue();
        assertThat(policy.next(NOW, 20, null).delay().toMillis()).isEqualTo(5000);
    }

    @Test
    void retryAfterOverridesBackoffAndDoesNotDisableWarning() {
        UploadRetryPolicy policy = new UploadRetryPolicy(1000, 5000, 0.2, 1, () -> 0.0);

        UploadRetryPolicy.RetryDecision decision = policy.next(NOW, 0, 17L);

        assertThat(decision.retryAfterApplied()).isTrue();
        assertThat(decision.delay().toSeconds()).isEqualTo(17);
        assertThat(decision.nextRetryAt()).isEqualTo(NOW.plusSeconds(17));
        assertThat(decision.warning()).isTrue();
    }

    @Test
    void jitterMovesDelayWithinConfiguredRange() {
        UploadRetryPolicy lower = new UploadRetryPolicy(1000, 5000, 0.2, 10, () -> 0.0d);
        UploadRetryPolicy upper = new UploadRetryPolicy(1000, 5000, 0.2, 10, () -> 0.999999d);

        assertThat(lower.next(NOW, 0, null).delay().toMillis()).isEqualTo(800);
        assertThat(upper.next(NOW, 0, null).delay().toMillis()).isEqualTo(1200);
    }
}

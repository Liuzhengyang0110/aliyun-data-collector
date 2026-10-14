package cn.kjky.collector.core;

import com.aliyun.openservices.log.exception.LogException;
import com.aliyuncs.exceptions.ClientException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.http.HttpTimeoutException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryExecutorTest {
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-09-19T10:00:00Z"), ZoneOffset.UTC);

    @Test
    void calculatesCappedExponentialBackoffWithoutRealSleep() throws Exception {
        Harness h = harness("ARMS_RPC", 4, 100, 250, 0d, 0.5d);
        AtomicInteger calls = new AtomicInteger();

        String result = h.executor.execute("metric/cpu", () -> {
            if (calls.getAndIncrement() < 4) throw new IOException("temporary");
            return "ok";
        });

        assertThat(result).isEqualTo("ok");
        assertThat(h.sleeps).extracting(Duration::toMillis).containsExactly(100L, 200L, 250L, 250L);
        assertThat(h.events).extracting(RetryExecutor.RetryEvent::attempt).containsExactly(1, 2, 3, 4);
    }

    @Test
    void jitterStaysInsideConfiguredRangeAndSpreadsRecoveringTasks() {
        Harness low = harness("ARMS_RPC", 1, 1000, 5000, 0.2d, 0d);
        Harness high = harness("ARMS_RPC", 1, 1000, 5000, 0.2d, 1d);

        long lowDelay = low.executor.delayMillis(1, null);
        long highDelay = high.executor.delayMillis(1, null);
        assertThat(lowDelay).isEqualTo(800);
        assertThat(highDelay).isEqualTo(1200);
        assertThat(lowDelay).isNotEqualTo(highDelay);
    }

    @Test
    void rateLimitUsesRetryAfterSecondsAndHttpDateButKeepsMaximumBound() throws Exception {
        Harness h = harness("ARMS_DATASET", 2, 100, 10_000, 0.2d, 0d);
        AtomicInteger calls = new AtomicInteger();
        h.executor.execute("dataset/orders", () -> {
            if (calls.getAndIncrement() == 0) throw CloudQueryException.forHttpStatus(429, "7");
            return "ok";
        });

        assertThat(h.sleeps).extracting(Duration::toMillis).containsExactly(7000L);
        assertThat(h.events.get(0).category()).isEqualTo(QueryErrorCategory.RATE_LIMITED);
        assertThat(h.executor.delayMillis(1, "Sat, 19 Sep 2026 10:00:30 GMT")).isEqualTo(10_000L);
    }

    @Test
    void retries408AndSupportedServerErrors() throws Exception {
        for (int status : List.of(408, 500, 502, 503, 504)) {
            Harness h = harness("ARMS_DATASET", 1, 10, 100, 0d, 0.5d);
            AtomicInteger calls = new AtomicInteger();
            h.executor.execute("query", () -> {
                if (calls.getAndIncrement() == 0) throw CloudQueryException.forHttpStatus(status, null);
                return "ok";
            });
            assertThat(calls).hasValue(2);
            assertThat(h.events.get(0).category()).isEqualTo(QueryErrorCategory.RETRYABLE);
        }
    }

    @Test
    void doesNotRetry400404OrInvalidArguments() {
        for (Exception failure : List.of(
                CloudQueryException.forHttpStatus(400, null),
                CloudQueryException.forHttpStatus(404, null),
                new IllegalArgumentException("bad parameter"))) {
            Harness h = harness("ARMS_DATASET", 3, 10, 100, 0d, 0.5d);
            AtomicInteger calls = new AtomicInteger();
            assertThatThrownBy(() -> h.executor.execute("query", () -> {
                calls.incrementAndGet();
                throw failure;
            })).isSameAs(failure);
            assertThat(calls).hasValue(1);
            assertThat(h.sleeps).isEmpty();
            assertThat(h.events.get(0).category()).isEqualTo(QueryErrorCategory.NON_RETRYABLE);
        }
    }

    @Test
    void authenticationFailuresDoNotRetryRapidly() {
        for (int status : List.of(401, 403)) {
            Harness h = harness("ARMS_DATASET", 5, 10, 100, 0d, 0.5d);
            AtomicInteger calls = new AtomicInteger();
            assertThatThrownBy(() -> h.executor.execute("query", () -> {
                calls.incrementAndGet();
                throw CloudQueryException.forHttpStatus(status, null);
            })).isInstanceOf(CloudQueryException.class);
            assertThat(calls).hasValue(1);
            assertThat(h.sleeps).isEmpty();
            assertThat(h.events.get(0).category()).isEqualTo(QueryErrorCategory.AUTHENTICATION);
        }
    }

    @Test
    void classifiesArmsAndSlsSdkAuthenticationAndRateLimitErrors() {
        CloudQueryErrorClassifier classifier = new CloudQueryErrorClassifier();
        ClientException rpcAuth = new ClientException("InvalidAccessKeyId.NotFound", "secret message");
        ClientException rpcRate = new ClientException("Throttling.User", "secret message");
        LogException slsAuth = new LogException(403, "Unauthorized", "secret body", "request-id");
        LogException slsRate = new LogException(429, "Throttling", "secret body", "request-id");
        LogException slsTemporary = new LogException(503, "ServiceUnavailable", "secret body", "request-id");
        ClientException invalidParameter = new ClientException("InvalidParameter", "secret message");
        ClientException wrappedNetwork = new ClientException("SDK.ClientError", "safe", new ConnectException("refused"));

        assertThat(classifier.classify(rpcAuth).category()).isEqualTo(QueryErrorCategory.AUTHENTICATION);
        assertThat(classifier.classify(rpcRate).category()).isEqualTo(QueryErrorCategory.RATE_LIMITED);
        assertThat(classifier.classify(slsAuth).category()).isEqualTo(QueryErrorCategory.AUTHENTICATION);
        assertThat(classifier.classify(slsRate).category()).isEqualTo(QueryErrorCategory.RATE_LIMITED);
        assertThat(classifier.classify(slsTemporary).category()).isEqualTo(QueryErrorCategory.RETRYABLE);
        assertThat(classifier.classify(invalidParameter).category()).isEqualTo(QueryErrorCategory.NON_RETRYABLE);
        assertThat(classifier.classify(wrappedNetwork).category()).isEqualTo(QueryErrorCategory.RETRYABLE);
    }

    @Test
    void retriesConnectionsResetsAndTimeouts() throws Exception {
        for (IOException failure : List.of(
                new ConnectException("refused"),
                new java.net.SocketException("connection reset"),
                new SocketTimeoutException("timeout"),
                new HttpTimeoutException("timeout"))) {
            Harness h = harness("SLS", 1, 1, 10, 0d, 0.5d);
            AtomicInteger calls = new AtomicInteger();
            h.executor.execute("logs", () -> {
                if (calls.getAndIncrement() == 0) throw failure;
                return "ok";
            });
            assertThat(calls).hasValue(2);
        }
    }

    @Test
    void stopsAtMaximumAttemptsAndAllowsLaterInvocation() throws Exception {
        Harness h = harness("SLS", 2, 1, 10, 0d, 0.5d);
        AtomicInteger calls = new AtomicInteger();
        assertThatThrownBy(() -> h.executor.execute("logs", () -> {
            calls.incrementAndGet();
            throw new IOException("down");
        })).isInstanceOf(IOException.class);

        assertThat(calls).hasValue(3);
        assertThat(h.sleeps).hasSize(2);
        assertThat(h.events.get(2).willRetry()).isFalse();
        assertThat(h.executor.execute("logs", () -> "next-cycle-ok")).isEqualTo("next-cycle-ok");
    }

    @Test
    void separateSystemsKeepSleepsEventsAndJitterIndependent() throws Exception {
        Harness arms = harness("ARMS_RPC", 1, 100, 1000, 0.2d, 0d);
        Harness sls = harness("SLS", 1, 100, 1000, 0.2d, 1d);
        AtomicInteger armsCalls = new AtomicInteger();
        AtomicInteger slsCalls = new AtomicInteger();

        arms.executor.execute("arms-task", () -> {
            if (armsCalls.getAndIncrement() == 0) throw new IOException("arms down");
            return null;
        });
        sls.executor.execute("sls-task", () -> {
            if (slsCalls.getAndIncrement() == 0) throw new IOException("sls down");
            return null;
        });

        assertThat(arms.sleeps).extracting(Duration::toMillis).containsExactly(80L);
        assertThat(sls.sleeps).extracting(Duration::toMillis).containsExactly(120L);
        assertThat(arms.events).extracting(RetryExecutor.RetryEvent::system).containsOnly("ARMS_RPC");
        assertThat(sls.events).extracting(RetryExecutor.RetryEvent::system).containsOnly("SLS");
    }

    @Test
    void observerReceivesSanitizedClassificationNotSecretsOrResponseBody() {
        Harness h = harness("SLS", 0, 1, 10, 0d, 0.5d);
        LogException failure = new LogException(403, "Unauthorized",
                "AccessKey=AK_SECRET Token=TOKEN_SECRET full raw event", "request-id");

        assertThatThrownBy(() -> h.executor.execute("safe-task", () -> { throw failure; }))
                .isSameAs(failure);

        String event = h.events.get(0).toString();
        assertThat(event).doesNotContain("AK_SECRET", "TOKEN_SECRET", "full raw event", "request-id");
        assertThat(h.events.get(0).safeReason()).isEqualTo("unauthorized");
        assertThat(h.events.get(0).timestamp()).isEqualTo(CLOCK.instant());
    }

    private Harness harness(String system, int retries, long base, long maximum,
                            double jitterRatio, double... randomValues) {
        List<Duration> sleeps = new ArrayList<>();
        List<RetryExecutor.RetryEvent> events = new ArrayList<>();
        ArrayDeque<Double> values = new ArrayDeque<>();
        for (double value : randomValues) values.add(value);
        RetryExecutor executor = new RetryExecutor(system, retries, base, maximum, jitterRatio, CLOCK,
                sleeps::add, () -> values.isEmpty() ? 0.5d : values.removeFirst(), events::add,
                new CloudQueryErrorClassifier());
        return new Harness(executor, sleeps, events);
    }

    private record Harness(RetryExecutor executor, List<Duration> sleeps,
                           List<RetryExecutor.RetryEvent> events) {}
}

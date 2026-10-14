package cn.kjky.collector.status;

import cn.kjky.collector.contract.BatchEnvelope;
import cn.kjky.collector.contract.StandardEvent;
import cn.kjky.collector.outbox.OutboxDelivery;
import cn.kjky.collector.outbox.OutboxStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxStatisticsTest {
    private static final Instant NOW = Instant.parse("2026-09-14T01:00:00Z");

    @TempDir Path temp;

    @Test
    void capturesPendingRetryAndBlockedInOneConsistentSnapshot() throws Exception {
        OutboxStore outbox = new OutboxStore(temp, "outbox", "state",
                Clock.fixed(NOW, ZoneOffset.UTC));
        outbox.enqueue(batch("retry"));
        var retry = outbox.claimNext(NOW).orElseThrow();
        outbox.scheduleRetry(retry, OutboxDelivery.ErrorCategory.NETWORK, NOW.plusSeconds(30));
        outbox.enqueue(batch("blocked"));
        var blocked = outbox.claimNext(NOW).orElseThrow();
        outbox.markBlocked(blocked, OutboxDelivery.ErrorCategory.VALIDATION);
        outbox.enqueue(batch("pending"));

        OutboxStatistics statistics = OutboxStatistics.capture(outbox, NOW.plusSeconds(120));

        assertThat(statistics.pending().count()).isEqualTo(1);
        assertThat(statistics.retry().count()).isEqualTo(1);
        assertThat(statistics.blocked().count()).isEqualTo(1);
        assertThat(statistics.pending().bytes()).isPositive();
        assertThat(statistics.retry().bytes()).isPositive();
        assertThat(statistics.blocked().bytes()).isPositive();
        assertThat(statistics.pending().oldestAgeSeconds()).isEqualTo(120);
        assertThat(statistics.retry().oldestAgeSeconds()).isEqualTo(120);
        assertThat(statistics.blocked().oldestAgeSeconds()).isEqualTo(120);
    }

    private BatchEnvelope batch(String id) {
        var payload = new ObjectMapper().createObjectNode().put("secret_payload", id);
        StandardEvent event = StandardEvent.create("event-" + id, "flow_record", NOW,
                null, null, "service-a", "instance-a", "sls/project/logstore", payload);
        return BatchEnvelope.create("batch-" + id, "poll-001", 0, true,
                "collector-001", "site-001", "ALIYUN", "SLS", "project/logstore",
                NOW.minusSeconds(60), NOW, List.of(event));
    }
}

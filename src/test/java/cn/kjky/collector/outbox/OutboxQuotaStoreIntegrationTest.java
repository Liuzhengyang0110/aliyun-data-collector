package cn.kjky.collector.outbox;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.contract.BatchEnvelope;
import cn.kjky.collector.contract.StandardEvent;
import cn.kjky.collector.runtime.CapacityState;
import cn.kjky.collector.runtime.OutboxQuotaManager;
import cn.kjky.collector.runtime.OutboxUsageProbe;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxQuotaStoreIntegrationTest {
    private static final Instant NOW = Instant.parse("2026-09-11T08:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @TempDir
    Path temp;

    @Test
    void storeMaintainsExactUsageAcrossPendingRetryBlockedAndDeletion() throws Exception {
        CollectorConfig config = config(temp.resolve("store"));
        OutboxStore store = new OutboxStore(Path.of(config.storage.dataRoot), "outbox", "state", CLOCK);
        try (OutboxQuotaManager quota = new OutboxQuotaManager(config, store.outboxRoot(), store.stateRoot())) {
            store.attachSpaceGuard(quota);
            OutboxUsageProbe probe = new OutboxUsageProbe();

            store.enqueue(batch("blocked", "first"));
            assertThat(quota.snapshot().outboxUsedBytes()).isEqualTo(probe.measure(store.outboxRoot()));

            try (OutboxClaim claim = store.claimNext(NOW).orElseThrow()) {
                store.scheduleRetry(claim, OutboxDelivery.ErrorCategory.TIMEOUT, NOW);
            }
            assertThat(quota.snapshot().outboxUsedBytes()).isEqualTo(probe.measure(store.outboxRoot()));

            try (OutboxClaim claim = store.claimNext(NOW).orElseThrow()) {
                store.markBlocked(claim, OutboxDelivery.ErrorCategory.VALIDATION);
            }
            assertThat(store.pendingBatches()).singleElement()
                    .extracting(item -> item.delivery().state()).isEqualTo(OutboxDelivery.State.BLOCKED);
            assertThat(quota.snapshot().outboxUsedBytes()).isEqualTo(probe.measure(store.outboxRoot()));

            store.enqueue(batch("committed", "second"));
            long beforeDelete = quota.snapshot().outboxUsedBytes();
            try (OutboxClaim claim = store.claimNext(NOW).orElseThrow()) {
                assertThat(claim.stored().batch().batchId()).isEqualTo("committed");
                store.deleteCommitted(claim);
            }
            assertThat(quota.snapshot().outboxUsedBytes()).isLessThan(beforeDelete)
                    .isEqualTo(probe.measure(store.outboxRoot()));
            assertThat(store.pendingBatches()).singleElement()
                    .extracting(item -> item.delivery().state()).isEqualTo(OutboxDelivery.State.BLOCKED);
        }
    }

    @Test
    void deletingCommittedBatchReleasesQuotaAndRecoversCollection() throws Exception {
        CollectorConfig config = config(temp.resolve("release"));
        OutboxStore store = new OutboxStore(Path.of(config.storage.dataRoot), "outbox", "state", CLOCK);
        OutboxStoredBatch stored = store.enqueue(batch("release-me", "payload"));
        long used = new OutboxUsageProbe().measure(store.outboxRoot());
        config.storage.outboxQuota.minBytes = 1;
        config.storage.outboxQuota.defaultBytes = used;
        config.storage.outboxQuota.maxBytes = used * 2;

        try (OutboxQuotaManager quota = new OutboxQuotaManager(config, store.outboxRoot(), store.stateRoot())) {
            store.attachSpaceGuard(quota);
            assertThat(quota.state()).isEqualTo(CapacityState.CRITICAL);
            assertThat(quota.collectionAllowed()).isFalse();

            try (OutboxClaim claim = store.claimNext(NOW).orElseThrow()) {
                store.deleteCommitted(claim);
            }
            assertThat(stored.directory()).doesNotExist();
            assertThat(quota.snapshot().outboxUsedBytes()).isZero();
            assertThat(quota.state()).isEqualTo(CapacityState.RECOVERING);
            assertThat(quota.collectionAllowed()).isTrue();
        }
    }

    private CollectorConfig config(Path root) {
        CollectorConfig config = new CollectorConfig();
        config.storage.dataRoot = root.toString();
        config.storage.outboxQuota.defaultBytes = 10 * 1024 * 1024;
        config.storage.outboxQuota.minBytes = 1;
        config.storage.outboxQuota.maxBytes = 20 * 1024 * 1024;
        config.storage.outboxQuota.reservedFreeBytes = 1;
        return config;
    }

    private BatchEnvelope batch(String id, String value) {
        var payload = new ObjectMapper().createObjectNode().put("value", value);
        StandardEvent event = StandardEvent.create("event-" + id, "flow_record", NOW,
                null, null, "service-a", "instance-a", "sls/project/logstore", payload);
        return BatchEnvelope.create(id, "poll-001", 0, true,
                "collector-001", "site-001", "ALIYUN", "SLS", "project/logstore",
                NOW.minusSeconds(60), NOW, List.of(event));
    }
}

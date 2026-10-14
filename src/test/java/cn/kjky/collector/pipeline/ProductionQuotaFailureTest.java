package cn.kjky.collector.pipeline;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.contract.BatchEnvelope;
import cn.kjky.collector.contract.StandardEvent;
import cn.kjky.collector.outbox.OutboxStore;
import cn.kjky.collector.runtime.CollectionWatermarkStore;
import cn.kjky.collector.runtime.OutboxQuotaManager;
import cn.kjky.collector.runtime.OutboxUsageProbe;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProductionQuotaFailureTest {
    private static final Instant START = Instant.parse("2026-09-11T07:59:00Z");
    private static final Instant END = Instant.parse("2026-09-11T08:00:00Z");

    @TempDir
    Path temp;

    @Test
    void insufficientQuotaKeepsExistingPendingAndDoesNotAdvanceCollectionWatermark() throws Exception {
        CollectorConfig config = config();
        config.storage.dataRoot = temp.resolve("pipeline").toString();
        OutboxStore outbox = new OutboxStore(config);
        OutboxStore seed = outbox;
        BatchEnvelope retained = batch("retained", "keep");
        seed.enqueue(retained);
        long used = new OutboxUsageProbe().measure(outbox.outboxRoot());
        config.storage.outboxQuota.minBytes = 1;
        config.storage.outboxQuota.defaultBytes = used;
        config.storage.outboxQuota.maxBytes = used * 2;
        config.storage.outboxQuota.reservedFreeBytes = 1;

        CollectionWatermarkStore watermarks = new CollectionWatermarkStore(
                outbox.stateRoot().resolve("collection-watermarks.json"));
        try (OutboxQuotaManager quota = new OutboxQuotaManager(config, outbox.outboxRoot(), outbox.stateRoot())) {
            outbox.attachSpaceGuard(quota);
            ProductionEventSink sink = new ProductionEventSink(config, outbox, watermarks);
            CollectionEventSink.TaskSession session = sink.open(
                    new CollectionTask("SLS/LOG/project/logstore", "SLS", "flow_record", START, END));
            session.accept(event("new-event", "new-data"));

            assertThatThrownBy(session::finish)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Outbox");
            assertThat(watermarks.snapshot()).isEmpty();
            assertThat(outbox.pendingBatches()).extracting(item -> item.batch().batchId())
                    .containsExactly(retained.batchId());
            assertThat(outbox.outboxRoot().resolve("writing").toFile().list()).isEmpty();
        }
    }

    private CollectorConfig config() {
        CollectorConfig config = new CollectorConfig();
        config.collector.collectorId = "collector-001";
        config.collector.siteId = "site-001";
        config.collector.collectorType = "ALIYUN";
        config.storage.rawPersistence.enabled = false;
        return config;
    }

    private BatchEnvelope batch(String id, String value) {
        return BatchEnvelope.create(id, "poll-seed", 0, true,
                "collector-001", "site-001", "ALIYUN", "SLS", "project/logstore",
                START, END, List.of(event("event-" + id, value)));
    }

    private StandardEvent event(String id, String value) {
        var payload = new ObjectMapper().createObjectNode().put("value", value);
        return StandardEvent.create(id, "flow_record", END, null, null,
                "service-a", "instance-a", "sls/project/logstore", payload);
    }
}

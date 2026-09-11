package cn.kjky.collector.outbox;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.contract.BatchEnvelope;
import cn.kjky.collector.contract.IngestJson;
import cn.kjky.collector.contract.StandardEvent;
import cn.kjky.collector.output.OutputSession;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxStoreTest {
    private static final Instant NOW = Instant.parse("2026-09-02T12:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @TempDir
    Path temp;

    @Test
    void productionLayoutContainsOnlyOutboxAndStateWhileDevelopmentCanStillCreateRaw() throws Exception {
        CollectorConfig production = new CollectorConfig();
        production.runtime.deploymentMode = "PRODUCTION";
        production.storage.rawPersistence.enabled = false;
        production.storage.dataRoot = temp.resolve("production").toString();

        OutboxStore store = new OutboxStore(production);

        assertThat(store.outboxRoot().resolve("writing")).isDirectory();
        assertThat(store.outboxRoot().resolve("pending")).isDirectory();
        assertThat(store.outboxRoot().resolve("retry")).isDirectory();
        assertThat(store.stateRoot()).isDirectory();
        assertThat(temp.resolve("production/raw")).doesNotExist();
        assertThat(temp.resolve("production/manifest")).doesNotExist();
        assertThat(temp.resolve("production/committed")).doesNotExist();
        assertThat(temp.resolve("production/quarantine")).doesNotExist();

        CollectorConfig development = new CollectorConfig();
        development.storage.dataRoot = temp.resolve("development").toString();
        new OutputSession(development, NOW, NOW.plusSeconds(60));
        assertThat(temp.resolve("development/raw/arms")).isDirectory();
        assertThat(temp.resolve("development/raw/sls")).isDirectory();
        assertThat(temp.resolve("development/manifest")).isDirectory();
    }

    @Test
    void enqueuePublishesOneCompleteImmutableDirectoryAtomically() throws Exception {
        OutboxStore store = store(temp.resolve("atomic"));
        BatchEnvelope batch = batch("batch-atomic", 0, true, "order-created");

        OutboxStoredBatch stored = store.enqueue(batch, "parent-001");

        assertThat(store.outboxRoot().resolve("writing")).isEmptyDirectory();
        assertThat(store.pendingBatches()).hasSize(1);
        assertThat(stored.directory()).isDirectory();
        assertThat(stored.directory().resolve(OutboxFiles.BATCH_FILE)).isRegularFile();
        assertThat(stored.directory().resolve(OutboxFiles.DESCRIPTOR_FILE)).isRegularFile();
        assertThat(stored.directory().resolve(OutboxFiles.DELIVERY_FILE)).isRegularFile();
        assertThat(stored.descriptor().outboxSchemaVersion()).isEqualTo("1.0");
        assertThat(stored.descriptor().parentBatchId()).isEqualTo("parent-001");
        assertThat(stored.descriptor().serializedSha256())
                .isEqualTo(OutboxFiles.sha256(stored.serializedBatch()));
        assertThat(stored.serializedBatch()).isEqualTo(IngestJson.writeBatch(batch));
        try (var files = Files.walk(store.outboxRoot())) {
            assertThat(files.noneMatch(path -> path.getFileName().toString().endsWith(".partial"))).isTrue();
        }
    }

    @Test
    void enqueueIsIdempotentForSameContentAndRejectsSameIdWithDifferentContent() throws Exception {
        OutboxStore store = store(temp.resolve("idempotent"));
        BatchEnvelope first = batch("same-batch", 0, true, "first");
        BatchEnvelope different = batch("same-batch", 0, true, "different");

        Path firstPath = store.enqueue(first).directory();
        Path secondPath = store.enqueue(first).directory();

        assertThat(secondPath).isEqualTo(firstPath);
        assertThat(store.pendingBatches()).hasSize(1);
        assertThatThrownBy(() -> store.enqueue(different))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("同一 batch_id");
    }

    @Test
    void retryMetadataDoesNotChangeImmutableBytesAndBlockedBatchStaysInPending() throws Exception {
        OutboxStore store = store(temp.resolve("delivery"));
        OutboxStoredBatch original = store.enqueue(batch("batch-retry", 0, true, "retry"));
        byte[] immutableBytes = original.serializedBatch();
        String immutableHash = original.descriptor().serializedSha256();

        OutboxClaim firstClaim = store.claimNext(NOW).orElseThrow();
        store.scheduleRetry(firstClaim, OutboxDelivery.ErrorCategory.SERVER_ERROR, NOW.plusSeconds(30));

        assertThat(store.pendingBatches()).isEmpty();
        OutboxStoredBatch retry = store.retryBatches().get(0);
        assertThat(retry.delivery().state()).isEqualTo(OutboxDelivery.State.RETRY);
        assertThat(retry.delivery().attemptCount()).isEqualTo(1);
        assertThat(retry.delivery().lastErrorCategory()).isEqualTo(OutboxDelivery.ErrorCategory.SERVER_ERROR);
        assertThat(retry.delivery().nextRetryAt()).isEqualTo(NOW.plusSeconds(30));
        assertThat(Files.readString(retry.directory().resolve(OutboxFiles.DELIVERY_FILE)))
                .contains("\"attempt_count\":1", "\"last_error\":\"SERVER_ERROR\"", "\"next_retry_at\"")
                .doesNotContain("last_error_category");
        assertThat(retry.serializedBatch()).isEqualTo(immutableBytes);
        assertThat(retry.descriptor().serializedSha256()).isEqualTo(immutableHash);
        assertThat(store.claimNext(NOW.plusSeconds(29))).isEmpty();

        OutboxClaim retryClaim = store.claimNext(NOW.plusSeconds(30)).orElseThrow();
        store.markBlocked(retryClaim, OutboxDelivery.ErrorCategory.VALIDATION);

        assertThat(store.retryBatches()).isEmpty();
        OutboxStoredBatch blocked = store.pendingBatches().get(0);
        assertThat(blocked.delivery().state()).isEqualTo(OutboxDelivery.State.BLOCKED);
        assertThat(blocked.delivery().attemptCount()).isEqualTo(2);
        assertThat(blocked.serializedBatch()).isEqualTo(immutableBytes);
        assertThat(store.claimNext(NOW.plusSeconds(3600))).isEmpty();
        assertThat(store.outboxRoot().resolve("quarantine")).doesNotExist();
    }

    @Test
    void concurrentWorkersNeverClaimTheSameBatch() throws Exception {
        OutboxStore store = store(temp.resolve("claims"));
        for (int i = 0; i < 12; i++) store.enqueue(batch("batch-" + i, i, i == 11, "event-" + i));

        var executor = Executors.newFixedThreadPool(6);
        List<OutboxClaim> claims = new ArrayList<>();
        try {
            List<Callable<OutboxClaim>> tasks = new ArrayList<>();
            for (int i = 0; i < 12; i++) tasks.add(() -> store.claimNext(NOW).orElseThrow());
            for (var future : executor.invokeAll(tasks)) claims.add(future.get());

            Set<String> batchIds = new HashSet<>();
            claims.forEach(claim -> batchIds.add(claim.stored().batch().batchId()));
            assertThat(batchIds).hasSize(12);
            assertThat(store.claimNext(NOW)).isEmpty();
        } finally {
            claims.forEach(OutboxClaim::close);
            executor.shutdownNow();
        }
        assertThat(store.claimNext(NOW)).isPresent().get().satisfies(OutboxClaim::close);
    }

    @Test
    void pendingAndDueRetryAreInterleavedWithoutStarvation() throws Exception {
        OutboxStore store = store(temp.resolve("fairness"));
        store.enqueue(batch("retry-item", 0, true, "retry"));
        OutboxClaim retrySource = store.claimNext(NOW).orElseThrow();
        store.scheduleRetry(retrySource, OutboxDelivery.ErrorCategory.NETWORK, NOW);
        store.enqueue(batch("pending-item", 1, true, "pending"));

        OutboxClaim first = store.claimNext(NOW).orElseThrow();
        assertThat(first.stored().batch().batchId()).isEqualTo("pending-item");
        first.close();

        OutboxClaim second = store.claimNext(NOW).orElseThrow();
        assertThat(second.stored().batch().batchId()).isEqualTo("retry-item");
        second.close();
    }

    @Test
    void restartRecoversCompleteWritingDeletesIncompleteAndReconcilesRetryState() throws Exception {
        Path root = temp.resolve("recovery");
        OutboxStore first = store(root);
        OutboxStoredBatch complete = first.enqueue(batch("complete-writing", 0, true, "complete"));
        Path writingResidue = first.outboxRoot().resolve("writing")
                .resolve(complete.directory().getFileName() + ".partial");
        Files.move(complete.directory(), writingResidue, StandardCopyOption.ATOMIC_MOVE);

        Path incomplete = first.outboxRoot().resolve("writing")
                .resolve("batch-" + "a".repeat(64) + ".partial");
        Files.createDirectory(incomplete);
        Files.writeString(incomplete.resolve("batch.json.partial"), "{", StandardCharsets.UTF_8);

        OutboxStoredBatch retrySource = first.enqueue(batch("retry-mismatch", 1, true, "retry-state"));
        OutboxClaim retryClaim = first.claimNext(NOW).orElseThrow();
        if (!retryClaim.stored().batch().batchId().equals(retrySource.batch().batchId())) {
            retryClaim.close();
            retryClaim = first.claimNext(NOW).orElseThrow();
        }
        storeRetry(first, retryClaim);
        OutboxStoredBatch retry = first.retryBatches().get(0);
        Files.move(retry.directory(), first.outboxRoot().resolve("pending").resolve(retry.directory().getFileName()),
                StandardCopyOption.ATOMIC_MOVE);

        OutboxStore restarted = store(root);

        assertThat(restarted.recoveryReport().restoredWriting()).isEqualTo(1);
        assertThat(restarted.recoveryReport().deletedIncompleteWriting()).isEqualTo(1);
        assertThat(restarted.recoveryReport().reconciledStateDirectories()).isEqualTo(1);
        assertThat(restarted.pendingBatches()).extracting(item -> item.batch().batchId())
                .contains("complete-writing");
        assertThat(restarted.retryBatches()).extracting(item -> item.batch().batchId())
                .contains("retry-mismatch");
        assertThat(incomplete).doesNotExist();
    }

    @Test
    void corruptPendingIsPreservedAndRecoveryFailsClosed() throws Exception {
        Path root = temp.resolve("corrupt");
        OutboxStore store = store(root);
        OutboxStoredBatch stored = store.enqueue(batch("corrupt-batch", 0, true, "corrupt"));
        Files.writeString(stored.directory().resolve(OutboxFiles.BATCH_FILE), "{}", StandardCharsets.UTF_8);

        assertThatThrownBy(() -> store(root))
                .isInstanceOf(Exception.class);
        assertThat(stored.directory()).isDirectory();
        assertThat(stored.directory().resolve(OutboxFiles.BATCH_FILE)).isRegularFile();
    }

    @Test
    void restartFinishesAtomicDeletionResidue() throws Exception {
        Path root = temp.resolve("deleting-recovery");
        OutboxStore first = store(root);
        OutboxStoredBatch stored = first.enqueue(batch("committed-before-crash", 0, true, "value"));
        Path deleting = first.outboxRoot().resolve("writing")
                .resolve(stored.directory().getFileName() + ".deleting");
        Files.move(stored.directory(), deleting, StandardCopyOption.ATOMIC_MOVE);

        OutboxStore restarted = store(root);

        assertThat(deleting).doesNotExist();
        assertThat(restarted.pendingBatches()).isEmpty();
        assertThat(restarted.retryBatches()).isEmpty();
    }

    @Test
    void childPublishedBeforeParentSplitStateIsNotClaimable() throws Exception {
        OutboxStore store = store(temp.resolve("split-intermediate"));
        BatchEnvelope parent = batch("parent-before-split", 0, true, "parent");
        BatchEnvelope child = batch("child-before-split", 0, true, "child");
        store.enqueue(parent);
        store.enqueue(child, parent.batchId());

        OutboxClaim claim = store.claimNext(NOW).orElseThrow();
        try {
            assertThat(claim.stored().batch().batchId()).isEqualTo(parent.batchId());
        } finally {
            claim.close();
        }
    }

    private void storeRetry(OutboxStore store, OutboxClaim claim) throws Exception {
        assertThat(claim.stored().batch().batchId()).isEqualTo("retry-mismatch");
        store.scheduleRetry(claim, OutboxDelivery.ErrorCategory.TIMEOUT, NOW.plusSeconds(1));
    }

    private OutboxStore store(Path root) throws Exception {
        return new OutboxStore(root, "outbox", "state", CLOCK);
    }

    private BatchEnvelope batch(String batchId, int sequence, boolean last, String value) {
        var payload = new ObjectMapper().createObjectNode().put("value", value);
        StandardEvent event = StandardEvent.create("event-" + batchId + "-" + value,
                "flow_record", NOW, null, null, "service-a", "instance-a",
                "sls/project/logstore/" + sequence, payload);
        return BatchEnvelope.create(batchId, "poll-001", sequence, last,
                "collector-001", "site-001", "ALIYUN", "SLS", "project/logstore",
                NOW.minusSeconds(60), NOW, List.of(event));
    }
}

package cn.kjky.collector.batch;

import cn.kjky.collector.contract.StandardEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** P0-07 确定性、流式组批和本地大小限制测试。 */
class BatchBuilderTest {
    private static final Instant START = Instant.parse("2026-09-03T08:00:00Z");
    private static final Instant END = START.plusSeconds(30);
    private static final BatchBuilder.BatchContext CONTEXT = new BatchBuilder.BatchContext(
            "poll-fixed-001", "collector-001", "site-001", "ALIYUN", "SLS", "project-a/logstore-b", START, END);

    @Test
    void splits3500EventsByRecordLimitAndKeepsOnlyOneCurrentBatchInMemory() {
        BatchBuilder builder = builder(1000, 10_000_000, 10_000_000, 5_000);
        List<BatchBuilder.BuiltBatch> emitted = new ArrayList<>();
        BatchBuilder.Session session = builder.open(CONTEXT, emitted::add);

        for (int index = 0; index < 3_500; index++) {
            session.accept(event(index), START.plusMillis(index));
            assertThat(session.bufferedEventCount()).isLessThanOrEqualTo(1_000);
        }
        session.finish();

        assertThat(emitted).extracting(item -> item.batch().recordCount()).containsExactly(1_000, 1_000, 1_000, 500);
        assertThat(emitted).extracting(item -> item.batch().batchSequence()).containsExactly(0, 1, 2, 3);
        assertThat(emitted).extracting(item -> item.batch().lastBatch()).containsExactly(false, false, false, true);
        assertThat(emitted).allSatisfy(item -> {
            assertThat(item.uncompressedBytes()).isLessThanOrEqualTo(10_000_000);
            assertThat(item.gzipBytes()).isLessThanOrEqualTo(10_000_000);
        });
        assertThat(session.bufferedEventCount()).isZero();
    }

    @Test
    void publishesTailBatchOnlyWhenWindowFinishes() {
        BatchBuilder builder = builder(1_000, 1_000_000, 1_000_000, 5_000);
        List<BatchBuilder.BuiltBatch> emitted = new ArrayList<>();
        BatchBuilder.Session session = builder.open(CONTEXT, emitted::add);

        session.accept(event(1), START);
        session.accept(event(2), START.plusMillis(1));
        session.accept(event(3), START.plusMillis(2));

        assertThat(emitted).isEmpty();
        session.finish();

        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).batch().recordCount()).isEqualTo(3);
        assertThat(emitted.get(0).batch().lastBatch()).isTrue();
    }

    @Test
    void splitsByExactGzipSizeUsingSameCodecAsHttpClient() {
        StandardEvent first = noisyEvent(1);
        StandardEvent second = noisyEvent(2);
        BatchBuilder probe = builder(10, 10_000_000, 10_000_000, 5_000);
        long oneSize = build(probe, List.of(first)).get(0).gzipBytes();
        long pairSize = build(probe, List.of(first, second)).get(0).gzipBytes();
        assertThat(pairSize).isGreaterThan(oneSize + 64);

        long gzipLimit = (oneSize + pairSize) / 2;
        BatchBuilder builder = builder(10, gzipLimit, 10_000_000, 5_000);
        List<BatchBuilder.BuiltBatch> emitted = build(builder, List.of(first, second));

        assertThat(emitted).hasSize(2);
        assertThat(emitted).allSatisfy(item -> assertThat((long) item.gzipBytes()).isLessThanOrEqualTo(gzipLimit));
        assertThat(emitted).extracting(item -> item.batch().lastBatch()).containsExactly(false, true);
    }

    @Test
    void rejectsOneEventThatExceedsUncompressedRequestLimitWithoutEmittingBatch() {
        StandardEvent oversized = eventWithPayload(99, "x".repeat(16_000));
        BatchBuilder probe = builder(10, 1_000_000, 1_000_000, 5_000);
        long encodedSize = build(probe, List.of(oversized)).get(0).uncompressedBytes();
        BatchBuilder builder = builder(10, encodedSize - 1, encodedSize - 1, 5_000);
        List<BatchBuilder.BuiltBatch> emitted = new ArrayList<>();
        BatchBuilder.Session session = builder.open(CONTEXT, emitted::add);

        assertThatThrownBy(() -> session.accept(oversized, START))
                .isInstanceOf(BatchBuilder.BatchTooLargeException.class)
                .hasMessageContaining("未压缩字节超过 maxRequestBytes");
        assertThat(emitted).isEmpty();
        assertThat(session.bufferedEventCount()).isZero();
    }

    @Test
    void flushesWaitingBatchForPaginationStallAndKeepsFollowingTailSequence() {
        BatchBuilder builder = builder(10, 1_000_000, 1_000_000, 5_000);
        List<BatchBuilder.BuiltBatch> emitted = new ArrayList<>();
        BatchBuilder.Session session = builder.open(CONTEXT, emitted::add);

        session.accept(event(1), START);
        assertThat(session.flushIfDue(START.plusMillis(4_999))).isFalse();
        assertThat(session.flushIfDue(START.plusMillis(5_000))).isTrue();
        session.accept(event(2), START.plusMillis(5_001));
        session.finish();

        assertThat(emitted).extracting(item -> item.batch().batchSequence()).containsExactly(0, 1);
        assertThat(emitted).extracting(item -> item.batch().lastBatch()).containsExactly(false, true);
    }

    @Test
    void generatesSameIdsForSamePollInputAndDoesNotRegenerateIdsForRetry() {
        BatchIdGenerator ids = new BatchIdGenerator();
        String firstPoll = ids.pollId("collector-001", "SLS", "project-a/logstore-b", START, END);
        String secondPoll = ids.pollId("collector-001", "SLS", "project-a/logstore-b", START, END);
        assertThat(firstPoll).isEqualTo(secondPoll).startsWith("poll-");

        BatchBuilder builder = builder(2, 1_000_000, 1_000_000, 5_000);
        List<StandardEvent> events = List.of(event(1), event(2), event(3));
        List<BatchBuilder.BuiltBatch> first = build(builder, events);
        List<BatchBuilder.BuiltBatch> retry = build(builder, events);

        assertThat(retry).extracting(item -> item.batch().batchId())
                .containsExactlyElementsOf(first.stream().map(item -> item.batch().batchId()).toList());
        assertThat(retry).extracting(item -> item.batch().contentSha256())
                .containsExactlyElementsOf(first.stream().map(item -> item.batch().contentSha256()).toList());
    }

    @Test
    void deterministicallySplits413ParentAndRetainsParentLink() {
        BatchBuilder builder = builder(10, 1_000_000, 1_000_000, 5_000);
        BatchBuilder.BuiltBatch parent = build(builder, List.of(event(1), event(2), event(3), event(4))).get(0);

        List<BatchBuilder.BuiltBatch> children = builder.splitForPayloadTooLarge(parent);
        List<BatchBuilder.BuiltBatch> retryChildren = builder.splitForPayloadTooLarge(parent);

        assertThat(children).hasSize(2);
        assertThat(children).extracting(BatchBuilder.BuiltBatch::parentBatchId)
                .containsOnly(parent.batch().batchId());
        assertThat(children).extracting(item -> item.batch().batchSequence())
                .containsOnly(parent.batch().batchSequence());
        assertThat(children).extracting(item -> item.batch().lastBatch()).containsExactly(false, true);
        assertThat(children.get(0).batch().events()).extracting(StandardEvent::eventId).containsExactly("event-1", "event-2");
        assertThat(children.get(1).batch().events()).extracting(StandardEvent::eventId).containsExactly("event-3", "event-4");
        assertThat(retryChildren).extracting(item -> item.batch().batchId())
                .containsExactlyElementsOf(children.stream().map(item -> item.batch().batchId()).toList());
        assertThatThrownBy(() -> builder.splitForPayloadTooLarge(build(builder, List.of(event(5))).get(0)))
                .isInstanceOf(BatchBuilder.BatchTooLargeException.class)
                .hasMessageContaining("无法继续拆分");
    }

    private BatchBuilder builder(int maxRecords, long maxCompressedBytes, long maxRequestBytes, long maxWaitMillis) {
        return new BatchBuilder(new BatchBuilder.BatchLimits(maxRecords, maxCompressedBytes, maxRequestBytes, maxWaitMillis));
    }

    private List<BatchBuilder.BuiltBatch> build(BatchBuilder builder, List<StandardEvent> events) {
        List<BatchBuilder.BuiltBatch> emitted = new ArrayList<>();
        BatchBuilder.Session session = builder.open(CONTEXT, emitted::add);
        events.forEach(event -> session.accept(event, START));
        session.finish();
        return emitted;
    }

    private StandardEvent event(int index) {
        return eventWithPayload(index, "value-" + index);
    }

    private StandardEvent noisyEvent(int index) {
        String noise = IntStream.range(0, 192)
                .mapToObj(value -> Integer.toHexString((index + 17) * 1_000_003 + value * 97_531))
                .reduce("", String::concat);
        return eventWithPayload(index, noise);
    }

    private StandardEvent eventWithPayload(int index, String value) {
        var payload = new ObjectMapper().createObjectNode().put("value", value).put("index", index);
        return StandardEvent.create("event-" + index, "flow_record", START.plusMillis(index),
                null, null, "service-a", "instance-a", "sls/project-a/logstore-b/" + index, payload);
    }
}

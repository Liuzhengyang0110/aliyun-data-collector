package cn.kjky.collector.normalize;

import cn.kjky.collector.contract.BatchEnvelope;
import cn.kjky.collector.contract.IngestJson;
import cn.kjky.collector.contract.StandardEvent;
import cn.kjky.collector.sls.SlsApi;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventNormalizerTest {
    private static final Instant WINDOW_START = Instant.parse("2026-08-03T02:00:00Z");
    private static final Instant WINDOW_END = WINDOW_START.plusSeconds(60);
    private final ObjectMapper json = new ObjectMapper();

    @Test
    void normalizesTraceSummaryAndDoesNotLeakUnknownValue() throws Exception {
        NormalizationResult result = new ArmsTraceSearchNormalizer().normalize(
                fixture("arms-trace-search-v1.json"),
                arms("TRACE_SEARCH", "all-traces", "topology_edge", List.of(), List.of("TraceID")));

        assertThat(result.events()).hasSize(1);
        StandardEvent event = result.events().get(0);
        assertThat(event.eventId()).startsWith("evt_").hasSize(68);
        assertThat(event.traceId()).isEqualTo("trace-001");
        assertThat(event.serviceId()).isEqualTo("order-service");
        assertThat(event.eventTime()).isEqualTo(Instant.ofEpochMilli(1785722400123L));
        assertThat(event.payload().has("OperationName")).isTrue();
        assertThat(event.payload().has("unapproved_secret")).isFalse();
        assertThat(result.warnings()).extracting(NormalizationWarning::code)
                .contains("UNKNOWN_FIELDS_IGNORED");
        assertThat(result.warnings().toString()).doesNotContain("synthetic-secret-must-not-leak");
    }

    @Test
    void traceIdentityRemainsStableAcrossAliasesAndPayloadChanges() throws Exception {
        JsonNode first = json.readTree("""
                {"TraceInfos":[{"TraceID":"trace-001","StartTime":1785722400,"Duration":10}]}
                """);
        JsonNode second = json.readTree("""
                {"traceinfos":[{"trace_id":"trace-001","start_time":1785722400999,"duration":99}]}
                """);
        NormalizationContext context = arms("TRACE_SEARCH", "all-traces", "topology_edge", List.of(), List.of());

        StandardEvent a = new ArmsTraceSearchNormalizer().normalize(first, context).events().get(0);
        StandardEvent b = new ArmsTraceSearchNormalizer().normalize(second, context).events().get(0);

        assertThat(b.eventId()).isEqualTo(a.eventId());
        assertThat(b.traceId()).isEqualTo(a.traceId());
    }

    @Test
    void normalizesEverySpanAndUsesRootTraceId() throws Exception {
        NormalizationResult result = new ArmsTraceDetailNormalizer().normalize(
                fixture("arms-trace-detail-v1.json"),
                arms("TRACE_DETAIL", "all-traces", "topology_edge", List.of(), List.of()));

        assertThat(result.events()).hasSize(2);
        assertThat(result.events()).extracting(StandardEvent::traceId).containsOnly("trace-001");
        assertThat(result.events()).extracting(StandardEvent::spanId).containsExactly("span-001", "span-002");
        assertThat(result.events().get(0).eventTime()).isEqualTo(Instant.parse("2026-08-03T02:00:00.123Z"));
        assertThat(result.events().get(1).eventTime()).isEqualTo(Instant.ofEpochSecond(1785722400L, 123_000_000L));
        assertThat(result.events().get(0).eventId()).isNotEqualTo(result.events().get(1).eventId());
    }

    @Test
    void metricFallbackIdIgnoresJsonObjectKeyOrder() throws Exception {
        NormalizationContext context = arms("METRIC", "service-metric", "flow_feature",
                List.of("timestamp", "metric", "value", "dimensions"), List.of());
        StandardEvent fixtureEvent = new ArmsMetricNormalizer().normalize(fixture("arms-metric-v1.json"), context)
                .events().get(0);
        JsonNode reordered = json.readTree("""
                {"Data":[{"dimensions":{"zone":"zone-a"},"value":12.5,"serviceName":"order-service",
                "metric":"service_qps","timestamp":1785722400,"internal_debug":"changed"}]}
                """);
        StandardEvent reorderedEvent = new ArmsMetricNormalizer().normalize(reordered, context).events().get(0);

        assertThat(reorderedEvent.eventId()).isEqualTo(fixtureEvent.eventId());
        assertThat(reorderedEvent.payloadSha256()).isEqualTo(fixtureEvent.payloadSha256());
        assertThat(reorderedEvent.payload().has("internal_debug")).isFalse();
        assertThat(reorderedEvent.eventTime()).isEqualTo(Instant.ofEpochSecond(1785722400L));
    }

    @Test
    void datasetUsesWindowStartWhenTimestampIsMissing() throws Exception {
        NormalizationResult result = new ArmsDatasetNormalizer().normalize(
                fixture("arms-dataset-v1.json"),
                arms("DATASET", "dataset-query", "flow_feature",
                        List.of("values", "dimensions"), List.of()));

        assertThat(result.events()).hasSize(1);
        assertThat(result.events().get(0).eventTime()).isEqualTo(WINDOW_START);
        assertThat(result.events().get(0).payload().has("future_field")).isFalse();
        assertThat(result.warnings()).extracting(NormalizationWarning::code)
                .contains("EVENT_TIME_FALLBACK", "UNKNOWN_FIELDS_IGNORED");
        assertThat(result.warnings().toString()).doesNotContain("synthetic-future-value");
    }

    @Test
    void normalizesSlsNanosecondsConfiguredFieldsAndDuplicateKeys() throws Exception {
        SlsApi.LogRecord record = new SlsApi.LogRecord("10.0.0.8", 1785722400, 123_456_789,
                List.of(
                        new SlsApi.Content("request_id", "req-001"),
                        new SlsApi.Content("message", "created"),
                        new SlsApi.Content("tag", "a"),
                        new SlsApi.Content("tag", "b"),
                        new SlsApi.Content("private_note", "synthetic-private-note")));
        NormalizationContext context = NormalizationContext.sls("project-a/logstore-a", "flow_record",
                WINDOW_START, WINDOW_END, "request-1", List.of("message", "tag"), List.of("request_id"));

        NormalizationResult result = new SlsLogNormalizer().normalize(record, context);
        StandardEvent event = result.events().get(0);

        assertThat(event.eventTime()).isEqualTo(Instant.ofEpochSecond(1785722400L, 123_456_789L));
        assertThat(event.instanceId()).isEqualTo("10.0.0.8");
        assertThat(event.payload().path("tag").isArray()).isTrue();
        assertThat(event.payload().path("tag").size()).isEqualTo(2);
        assertThat(event.payload().has("private_note")).isFalse();
        assertThat(result.warnings().toString()).doesNotContain("synthetic-private-note");

        StandardEvent repeated = new SlsLogNormalizer().normalize(record, context).events().get(0);
        assertThat(repeated.eventId()).isEqualTo(event.eventId());
        assertThat(repeated.payloadSha256()).isEqualTo(event.payloadSha256());
    }

    @Test
    void missingConfiguredIdentityProducesSafeWarningAndDeterministicFallback() throws Exception {
        NormalizationContext context = arms("METRIC", "service-metric", "flow_feature",
                List.of("timestamp", "value"), List.of("missing_business_id"));
        NormalizationResult first = new ArmsMetricNormalizer().normalize(fixture("arms-metric-v1.json"), context);
        NormalizationResult second = new ArmsMetricNormalizer().normalize(fixture("arms-metric-v1.json"), context);

        assertThat(first.events().get(0).eventId()).isEqualTo(second.events().get(0).eventId());
        assertThat(first.warnings()).extracting(NormalizationWarning::code).contains("IDENTITY_FIELDS_MISSING");
        assertThat(first.warnings().toString()).doesNotContain("synthetic-debug-value");
    }

    @Test
    void acceptsEmptyArrayButRejectsMissingOrMalformedRecordArray() throws Exception {
        NormalizationContext context = arms("TRACE_SEARCH", "all-traces", "topology_edge", List.of(), List.of());

        assertThat(new ArmsTraceSearchNormalizer().normalize(json.readTree("{\"TraceInfos\":[]}"), context).events())
                .isEmpty();
        assertThatThrownBy(() -> new ArmsTraceSearchNormalizer().normalize(json.readTree("{\"ok\":true}"), context))
                .isInstanceOf(NormalizationException.class)
                .hasMessageContaining("缺少预期记录数组");
        assertThatThrownBy(() -> new ArmsTraceSearchNormalizer().normalize(json.readTree("{\"TraceInfos\":{}}"), context))
                .isInstanceOf(NormalizationException.class)
                .hasMessageContaining("必须是数组");
    }

    @Test
    void normalizedEventsSerializeUnderExistingBatchContract() throws Exception {
        StandardEvent event = new ArmsMetricNormalizer().normalize(
                fixture("arms-metric-v1.json"),
                arms("METRIC", "service-metric", "flow_feature", List.of("timestamp", "value"), List.of()))
                .events().get(0);
        BatchEnvelope batch = BatchEnvelope.create("batch-normalize-1", "poll-normalize-1", 0, true,
                "aliyun-collector-site01", "site01", "ALIYUN", "ARMS", "service-metric",
                WINDOW_START, WINDOW_END, List.of(event));

        byte[] bytes = IngestJson.writeBatch(batch);
        BatchEnvelope decoded = IngestJson.readBatch(bytes);

        assertThat(decoded.events()).containsExactly(event);
        assertThat(decoded.contentSha256()).isEqualTo(IngestJson.eventsSha256(decoded.events()));
        assertThat(decoded.events().get(0).payloadSha256())
                .isEqualTo(IngestJson.sha256Canonical(decoded.events().get(0).payload()));
    }

    @Test
    void rejectsInvalidSlsNanosecondsWithoutEchoingRecordValues() {
        SlsApi.LogRecord record = new SlsApi.LogRecord("10.0.0.8", 1785722400, 1_000_000_000,
                List.of(new SlsApi.Content("message", "synthetic-message")));
        NormalizationContext context = NormalizationContext.sls("p/l", "flow_record",
                WINDOW_START, WINDOW_END, null, List.of("message"), List.of());

        assertThatThrownBy(() -> new SlsLogNormalizer().normalize(record, context))
                .isInstanceOf(NormalizationException.class)
                .hasMessageContaining("timeNsPart")
                .hasMessageNotContaining("synthetic-message");
    }

    private NormalizationContext arms(String kind, String key, String recordType,
                                      List<String> payloadFields, List<String> identityFields) {
        return NormalizationContext.arms(kind, key, recordType, WINDOW_START, WINDOW_END,
                "request-1", payloadFields, identityFields);
    }

    private JsonNode fixture(String name) throws Exception {
        try (InputStream input = getClass().getResourceAsStream("/normalize/" + name)) {
            if (input == null) throw new IllegalArgumentException("测试资源不存在: " + name);
            return json.readTree(input);
        }
    }
}

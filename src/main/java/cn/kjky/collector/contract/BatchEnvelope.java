package cn.kjky.collector.contract;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** POST /collector/v1/events:batch 的 v1 请求体。 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public record BatchEnvelope(
        String schemaVersion,
        String batchId,
        String pollId,
        int batchSequence,
        @JsonProperty("is_last_batch")
        boolean lastBatch,
        String collectorId,
        String siteId,
        String collectorType,
        String sourceType,
        String sourceKey,
        Instant windowStart,
        Instant windowEnd,
        int recordCount,
        String contentSha256,
        List<StandardEvent> events) {

    public BatchEnvelope {
        requireText(schemaVersion, "schema_version");
        requireText(batchId, "batch_id");
        requireText(pollId, "poll_id");
        if (batchSequence < 0) throw new IllegalArgumentException("batch_sequence 不能小于 0");
        requireText(collectorId, "collector_id");
        requireText(siteId, "site_id");
        requireText(collectorType, "collector_type");
        requireText(sourceType, "source_type");
        requireText(sourceKey, "source_key");
        Objects.requireNonNull(windowStart, "window_start 不能为空");
        Objects.requireNonNull(windowEnd, "window_end 不能为空");
        if (!windowStart.isBefore(windowEnd)) throw new IllegalArgumentException("window_start 必须早于 window_end");
        requireText(contentSha256, "content_sha256");
        events = events == null ? List.of() : List.copyOf(events);
        if (events.isEmpty()) throw new IllegalArgumentException("events 不能为空");
        if (recordCount != events.size()) throw new IllegalArgumentException("record_count 必须等于 events 数量");
    }

    /** 创建批次并固定记录数和事件内容哈希。 */
    public static BatchEnvelope create(String batchId, String pollId, int batchSequence, boolean lastBatch,
                                       String collectorId, String siteId, String collectorType,
                                       String sourceType, String sourceKey, Instant windowStart, Instant windowEnd,
                                       List<StandardEvent> events) {
        List<StandardEvent> immutableEvents = events == null ? List.of() : List.copyOf(events);
        return new BatchEnvelope(IngestJson.SCHEMA_VERSION, batchId, pollId, batchSequence, lastBatch,
                collectorId, siteId, collectorType, sourceType, sourceKey, windowStart, windowEnd,
                immutableEvents.size(), IngestJson.eventsSha256(immutableEvents), immutableEvents);
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " 不能为空");
    }
}

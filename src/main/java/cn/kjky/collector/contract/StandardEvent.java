package cn.kjky.collector.contract;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.time.Instant;
import java.util.Objects;

/** 采集端提交给 cc-ingest 的 v1 标准事件。 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public record StandardEvent(
        String eventId,
        String recordType,
        Instant eventTime,
        String traceId,
        String spanId,
        String serviceId,
        String instanceId,
        String sourceRef,
        String payloadSha256,
        JsonNode payload) {

    public StandardEvent {
        requireText(eventId, "event_id");
        requireText(recordType, "record_type");
        Objects.requireNonNull(eventTime, "event_time 不能为空");
        requireText(sourceRef, "source_ref");
        requireText(payloadSha256, "payload_sha256");
        Objects.requireNonNull(payload, "payload 不能为空");
        payload = payload.deepCopy();
    }

    /** 创建事件并按契约算法计算 payload_sha256。 */
    public static StandardEvent create(String eventId, String recordType, Instant eventTime,
                                       String traceId, String spanId, String serviceId, String instanceId,
                                       String sourceRef, JsonNode payload) {
        return new StandardEvent(eventId, recordType, eventTime, traceId, spanId, serviceId, instanceId,
                sourceRef, IngestJson.sha256Canonical(payload), payload);
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " 不能为空");
    }
}

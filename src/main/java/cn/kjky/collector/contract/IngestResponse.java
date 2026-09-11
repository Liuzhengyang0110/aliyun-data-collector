package cn.kjky.collector.contract;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.time.Instant;

/** cc-ingest v1 对批量上传请求的结构化回执。 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public record IngestResponse(
        String schemaVersion,
        String batchId,
        String contentSha256,
        String status,
        Instant committedAt,
        String reasonCode,
        String message) {
}

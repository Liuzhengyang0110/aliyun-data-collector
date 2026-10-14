package cn.kjky.collector.control;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.time.Instant;

/** cc-control v1 的最小控制契约；只包含 P1-01 首批闭环所需模型。 */
final class ControlProtocol {
    static final String SCHEMA_VERSION = "1.0";

    private ControlProtocol() {}

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = false)
    record ClaimRequest(String schemaVersion, String collectorId, String siteId,
                        String collectorVersion, long lastAppliedConfigVersion,
                        String lastCompletedJobId) {}

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = false)
    record Job(String schemaVersion, String jobId, String collectorId, String siteId,
               String commandType, long configVersion, Instant createdAt, Instant expiresAt,
               JsonNode parameters) {}

    enum Status { RECEIVED, RUNNING, SUCCEEDED, FAILED, REJECTED }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = false)
    record StatusUpdate(String schemaVersion, String jobId, String collectorId, String siteId,
                        String commandType, long configVersion, Status status, Instant occurredAt,
                        Long requestedQuotaBytes, Long effectiveQuotaBytes,
                        Long localMinBytes, Long localMaxBytes,
                        String rejectReason, String message) {}

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = false)
    record StatusAck(String schemaVersion, String jobId, Status acceptedStatus, Instant receivedAt) {}
}

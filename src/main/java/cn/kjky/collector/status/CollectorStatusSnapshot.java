package cn.kjky.collector.status;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.time.Instant;
import java.util.List;

/** cc-control v1 冻结的采集端节点状态快照。所有集合均为不可变副本。 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = false)
public record CollectorStatusSnapshot(
        String schemaVersion,
        String collectorId,
        String siteId,
        String collectorType,
        String collectorVersion,
        DeploymentMode deploymentMode,
        Instant startedAt,
        Instant sentAt,
        long configVersion,
        OverallStatus overallStatus,
        List<StatusReason> statusReasons,
        List<SourceStatus> sources,
        UploadStatus upload,
        CapacityStatus capacity,
        JobSummary lastJob) {

    public static final String SCHEMA_VERSION = "1.0";

    public CollectorStatusSnapshot {
        statusReasons = List.copyOf(statusReasons);
        sources = List.copyOf(sources);
    }

    public enum DeploymentMode { DEVELOPMENT, PRODUCTION }
    public enum OverallStatus { RUNNING, PAUSED, DEGRADED }
    public enum ComponentState { RUNNING, PAUSED, DEGRADED, DISABLED }
    public enum CapacityState { NORMAL, WARNING, CRITICAL, RECOVERING }

    public enum StatusReason {
        MANUAL_PAUSE,
        PHYSICAL_CAPACITY_CRITICAL,
        LOGICAL_QUOTA_CRITICAL,
        UPLOAD_AUTHENTICATION_PAUSED,
        CONFIG_INVALID,
        CAPACITY_CHECK_FAILED,
        COLLECTION_DEGRADED,
        UPLOAD_DEGRADED
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = false)
    public record SourceStatus(String sourceType, boolean enabled, ComponentState state,
                               Instant lastSuccessAt, Instant lastFailureAt,
                               int consecutiveFailures, long collectionLagSeconds,
                               List<CollectionWatermarkStatus> watermarks) {
        public SourceStatus { watermarks = List.copyOf(watermarks); }
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = false)
    public record CollectionWatermarkStatus(String sourceKey, Instant windowEnd, Instant updatedAt) {}

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = false)
    public record UploadStatus(ComponentState state, Instant lastSuccessAt, int inFlightBatches,
                               BacklogStatus pending, BacklogStatus retry, BacklogStatus blocked,
                               List<UploadWatermarkStatus> watermarks) {
        public UploadStatus { watermarks = List.copyOf(watermarks); }
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = false)
    public record BacklogStatus(long count, long bytes, long oldestAgeSeconds) {}

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = false)
    public record UploadWatermarkStatus(String pollId, int contiguousBatchSequence,
                                        boolean complete, Instant updatedAt) {}

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = false)
    public record CapacityStatus(CapacityState state, List<StatusReason> reasons,
                                 long pvcTotalBytes, long pvcUsableBytes, long pvcUsedBytes,
                                 double pvcUsedPercent, long outboxUsedBytes,
                                 long requestedQuotaBytes, long effectiveQuotaBytes,
                                 long localMinBytes, long localMaxBytes, long reservedFreeBytes,
                                 double quotaUsedPercent) {
        public CapacityStatus { reasons = List.copyOf(reasons); }
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = false)
    public record JobSummary(String jobId, String commandType, long configVersion,
                             String status, Instant occurredAt, String rejectReason) {}
}

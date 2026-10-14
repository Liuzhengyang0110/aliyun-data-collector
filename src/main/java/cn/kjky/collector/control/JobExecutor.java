package cn.kjky.collector.control;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.contract.IngestJson;
import cn.kjky.collector.runtime.CollectionSource;
import cn.kjky.collector.runtime.OutboxQuotaManager;
import cn.kjky.collector.runtime.QuotaPolicyRejectedException;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Iterator;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** 校验并幂等执行 P1-01 首批三种控制命令。 */
public final class JobExecutor {
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:-]{0,127}");
    private static final Set<String> SUPPORTED = Set.of(
            "PAUSE_COLLECTION", "RESUME_COLLECTION", "SET_OUTBOX_QUOTA");

    @FunctionalInterface
    public interface StatusReporter {
        boolean report(ControlProtocol.StatusUpdate update);
    }

    private final CollectorConfig config;
    private final JobStateStore jobs;
    private final ManualPauseStateStore pauses;
    private final OutboxQuotaManager quotas;
    private final Clock clock;

    public JobExecutor(CollectorConfig config, JobStateStore jobs,
                       ManualPauseStateStore pauses, OutboxQuotaManager quotas) {
        this(config, jobs, pauses, quotas, Clock.systemUTC());
    }

    JobExecutor(CollectorConfig config, JobStateStore jobs, ManualPauseStateStore pauses,
                OutboxQuotaManager quotas, Clock clock) {
        this.config = Objects.requireNonNull(config, "config 不能为空");
        this.jobs = Objects.requireNonNull(jobs, "jobs 不能为空");
        this.pauses = Objects.requireNonNull(pauses, "pauses 不能为空");
        this.quotas = Objects.requireNonNull(quotas, "quotas 不能为空");
        this.clock = Objects.requireNonNull(clock, "clock 不能为空");
    }

    /** 执行单个任务；每个状态先持久化，再尽力回传，回传失败不会回滚命令。 */
    ControlProtocol.StatusUpdate execute(ControlProtocol.Job job, StatusReporter reporter) throws IOException {
        Objects.requireNonNull(job, "job 不能为空");
        Objects.requireNonNull(reporter, "reporter 不能为空");
        String fingerprint = IngestJson.sha256Canonical(job);

        var existing = validId(job.jobId()) ? jobs.find(job.jobId()) : java.util.Optional.<JobStateStore.JobRecord>empty();
        if (existing.isPresent()) {
            JobStateStore.JobRecord old = existing.get();
            if (!old.fingerprint().equals(fingerprint)) {
                return reportOnly(rejected(job, "JOB_ID_CONFLICT", "同一 job_id 的任务内容发生变化"), reporter);
            }
            if (JobStateStore.terminal(old.status())) return reportStored(old, reporter);
            if (effectAlreadyPersisted(job)) {
                return transition(job, fingerprint, ControlProtocol.Status.SUCCEEDED,
                        null, "任务副作用已在崩溃前持久化", quotaDetails(job), true, reporter);
            }
            if (job.configVersion() <= lastAppliedConfigVersion()) {
                return transition(job, fingerprint, ControlProtocol.Status.REJECTED,
                        "STALE_CONFIG_VERSION", "崩溃前未完成的旧任务已被更新策略取代",
                        quotaDetails(job), false, reporter);
            }
        } else {
            String rejection = validateEnvelope(job);
            if (rejection != null) {
                String[] parts = rejection.split("\u0000", 2);
                return transition(job, fingerprint, ControlProtocol.Status.REJECTED,
                        parts[0], parts[1], quotaDetails(job), false, reporter);
            }
            transition(job, fingerprint, ControlProtocol.Status.RECEIVED,
                    null, null, quotaDetails(job), false, reporter);
        }

        // RECEIVED 已存在但未执行，或崩溃时处于 RUNNING 且副作用尚未持久化。
        transition(job, fingerprint, ControlProtocol.Status.RUNNING,
                null, null, quotaDetails(job), false, reporter);
        try {
            QuotaDetails details = executeEffect(job);
            return transition(job, fingerprint, ControlProtocol.Status.SUCCEEDED,
                    null, successMessage(job, details), details, true, reporter);
        } catch (QuotaPolicyRejectedException e) {
            return transition(job, fingerprint, ControlProtocol.Status.REJECTED,
                    e.reason().name(), safe(e), quotaDetails(job), false, reporter);
        } catch (IllegalArgumentException e) {
            return transition(job, fingerprint, ControlProtocol.Status.REJECTED,
                    "INVALID_PARAMETERS", safe(e), quotaDetails(job), false, reporter);
        } catch (Exception e) {
            return transition(job, fingerprint, ControlProtocol.Status.FAILED,
                    null, safe(e), quotaDetails(job), false, reporter);
        }
    }

    long lastAppliedConfigVersion() {
        return Math.max(jobs.lastAppliedConfigVersion(), Math.max(
                pauses.snapshot().configVersion(), quotas.lastAppliedConfigVersion()));
    }

    String lastCompletedJobId() {
        return jobs.lastCompletedJobId();
    }

    private String validateEnvelope(ControlProtocol.Job job) {
        if (!ControlProtocol.SCHEMA_VERSION.equals(job.schemaVersion())) {
            return reject("INVALID_PARAMETERS", "只支持 schema_version=1.0");
        }
        if (!validId(job.jobId()) || !validId(job.collectorId()) || !validId(job.siteId())) {
            return reject("INVALID_PARAMETERS", "job_id、collector_id 或 site_id 格式非法");
        }
        if (!Objects.equals(config.collector.collectorId, job.collectorId())
                || !Objects.equals(config.collector.siteId, job.siteId())) {
            return reject("IDENTITY_MISMATCH", "任务身份与本采集端不匹配");
        }
        Instant now = clock.instant();
        if (job.expiresAt() == null || !job.expiresAt().isAfter(now)
                || job.createdAt() == null || job.createdAt().isAfter(job.expiresAt())) {
            return reject("EXPIRED_JOB", "任务已过期或时间范围非法");
        }
        if (job.commandType() == null || !SUPPORTED.contains(job.commandType())) {
            return reject("UNKNOWN_COMMAND", "不支持的控制命令");
        }
        if (job.configVersion() < 1) return reject("INVALID_PARAMETERS", "config_version 必须大于 0");
        if (job.configVersion() <= lastAppliedConfigVersion()) {
            return reject("STALE_CONFIG_VERSION", "旧 config_version 不能覆盖已生效策略");
        }
        String invalid = validateParameters(job);
        return invalid == null ? null : reject("INVALID_PARAMETERS", invalid);
    }

    private String validateParameters(ControlProtocol.Job job) {
        JsonNode parameters = job.parameters();
        if (parameters == null || parameters.isNull()) {
            return "SET_OUTBOX_QUOTA".equals(job.commandType()) ? "缺少 requested_quota_bytes" : null;
        }
        if (!parameters.isObject()) return "parameters 必须是 JSON 对象";
        Set<String> allowed = switch (job.commandType()) {
            case "PAUSE_COLLECTION" -> Set.of("source_type", "reason");
            case "RESUME_COLLECTION" -> Set.of("source_type");
            case "SET_OUTBOX_QUOTA" -> Set.of("requested_quota_bytes");
            default -> Set.of();
        };
        Iterator<String> names = parameters.fieldNames();
        while (names.hasNext()) if (!allowed.contains(names.next())) return "parameters 包含未批准字段";
        if (parameters.has("source_type")) {
            JsonNode source = parameters.get("source_type");
            if (!source.isTextual() || parseSource(source.asText()) == null) return "source_type 只能是 ARMS 或 SLS";
        }
        if (parameters.has("reason")) {
            JsonNode reason = parameters.get("reason");
            if (!reason.isTextual() || reason.asText().length() > 256) return "reason 必须是最多 256 字符的字符串";
        }
        if ("SET_OUTBOX_QUOTA".equals(job.commandType())) {
            JsonNode quota = parameters.get("requested_quota_bytes");
            if (quota == null || !quota.isIntegralNumber() || !quota.canConvertToLong() || quota.longValue() <= 0) {
                return "requested_quota_bytes 必须是正整数";
            }
        }
        return null;
    }

    private QuotaDetails executeEffect(ControlProtocol.Job job) throws Exception {
        JsonNode parameters = job.parameters();
        switch (job.commandType()) {
            case "PAUSE_COLLECTION" -> {
                pauses.pause(source(parameters), job.configVersion(), text(parameters, "reason"), clock.instant());
                return quotaDetails(job);
            }
            case "RESUME_COLLECTION" -> {
                pauses.resume(source(parameters), job.configVersion(), clock.instant());
                return quotaDetails(job);
            }
            case "SET_OUTBOX_QUOTA" -> {
                long requested = parameters.get("requested_quota_bytes").longValue();
                OutboxQuotaManager.PolicyApplyResult result = quotas.applyRequestedQuota(requested, job.configVersion());
                return new QuotaDetails(result.requestedQuotaBytes(), result.effectiveQuotaBytes(),
                        result.localMinBytes(), result.localMaxBytes());
            }
            default -> throw new IllegalArgumentException("未知控制命令");
        }
    }

    private boolean effectAlreadyPersisted(ControlProtocol.Job job) {
        return switch (job.commandType() == null ? "" : job.commandType()) {
            case "PAUSE_COLLECTION", "RESUME_COLLECTION" -> pauses.snapshot().configVersion() >= job.configVersion();
            case "SET_OUTBOX_QUOTA" -> quotas.lastAppliedConfigVersion() >= job.configVersion();
            default -> false;
        };
    }

    private ControlProtocol.StatusUpdate transition(ControlProtocol.Job job, String fingerprint,
                                                    ControlProtocol.Status status, String rejectReason,
                                                    String message, QuotaDetails quota, boolean applied,
                                                    StatusReporter reporter) throws IOException {
        Instant now = clock.instant();
        JobStateStore.JobRecord record = new JobStateStore.JobRecord(job.jobId(), fingerprint,
                safeId(job.collectorId()), safeId(job.siteId()), safeCommand(job.commandType()),
                Math.max(1, job.configVersion()), status, now,
                quota.requested(), quota.effective(), quota.minimum(), quota.maximum(),
                rejectReason, message, false);
        jobs.transition(record, applied);
        ControlProtocol.StatusUpdate update = toUpdate(record);
        if (reporter.report(update)) jobs.markReported(record.jobId(), record.status(), record.occurredAt());
        return update;
    }

    private ControlProtocol.StatusUpdate reportStored(JobStateStore.JobRecord record,
                                                       StatusReporter reporter) throws IOException {
        ControlProtocol.StatusUpdate update = toUpdate(record);
        if (reporter.report(update)) jobs.markReported(record.jobId(), record.status(), record.occurredAt());
        return update;
    }

    private ControlProtocol.StatusUpdate reportOnly(ControlProtocol.StatusUpdate update, StatusReporter reporter) {
        reporter.report(update);
        return update;
    }

    private ControlProtocol.StatusUpdate rejected(ControlProtocol.Job job, String reason, String message) {
        QuotaDetails q = quotaDetails(job);
        return new ControlProtocol.StatusUpdate(ControlProtocol.SCHEMA_VERSION, safeId(job.jobId()),
                config.collector.collectorId, config.collector.siteId, safeCommand(job.commandType()),
                Math.max(1, job.configVersion()), ControlProtocol.Status.REJECTED, clock.instant(),
                q.requested(), q.effective(), q.minimum(), q.maximum(), reason, message);
    }

    private ControlProtocol.StatusUpdate toUpdate(JobStateStore.JobRecord record) {
        return new ControlProtocol.StatusUpdate(ControlProtocol.SCHEMA_VERSION, record.jobId(),
                record.collectorId(), record.siteId(), record.commandType(), record.configVersion(),
                record.status(), record.occurredAt(), record.requestedQuotaBytes(),
                record.effectiveQuotaBytes(), record.localMinBytes(), record.localMaxBytes(),
                record.rejectReason(), record.message());
    }

    private QuotaDetails quotaDetails(ControlProtocol.Job job) {
        OutboxQuotaManager.Snapshot snapshot = quotas.snapshot();
        Long requested = null;
        Long effective = null;
        Long minimum = null;
        Long maximum = null;
        if ("SET_OUTBOX_QUOTA".equals(job.commandType())) {
            requested = requestedQuota(job.parameters());
            if (requested == null || requested <= 0) requested = snapshot.requestedQuotaBytes();
            effective = snapshot.effectiveQuotaBytes();
            minimum = snapshot.localMinBytes();
            maximum = snapshot.localMaxBytes();
        }
        return new QuotaDetails(requested, effective, minimum, maximum);
    }

    private Long requestedQuota(JsonNode parameters) {
        if (parameters == null || !parameters.isObject()) return null;
        JsonNode value = parameters.get("requested_quota_bytes");
        return value != null && value.isIntegralNumber() && value.canConvertToLong() ? value.longValue() : null;
    }

    private CollectionSource source(JsonNode parameters) {
        return parameters == null ? null : parseSource(text(parameters, "source_type"));
    }

    private CollectionSource parseSource(String value) {
        if (value == null) return null;
        try { return CollectionSource.valueOf(value.toUpperCase(Locale.ROOT)); }
        catch (IllegalArgumentException ignored) { return null; }
    }

    private String text(JsonNode parameters, String field) {
        if (parameters == null || !parameters.has(field) || !parameters.get(field).isTextual()) return null;
        return parameters.get(field).asText();
    }

    private boolean validId(String value) {
        return value != null && ID.matcher(value).matches();
    }

    private String safeId(String value) {
        return validId(value) ? value : "invalid-job";
    }

    private String safeCommand(String value) {
        return value != null && value.matches("[A-Z][A-Z0-9_]{0,63}") ? value : "UNKNOWN";
    }

    private String reject(String reason, String message) {
        return reason + '\u0000' + message;
    }

    private String safe(Exception error) {
        String message = error.getMessage() == null ? "执行失败" : error.getMessage();
        message = message.replaceAll("(?i)(accesskey|secret|password|token|private[_ -]?key)[^ ,;]*", "[REDACTED]");
        return message.length() <= 512 ? message : message.substring(0, 512);
    }

    private String successMessage(ControlProtocol.Job job, QuotaDetails details) {
        return "SET_OUTBOX_QUOTA".equals(job.commandType())
                ? "配额策略已生效，requested=" + details.requested() + "，effective=" + details.effective()
                : "控制策略已生效";
    }

    private record QuotaDetails(Long requested, Long effective, Long minimum, Long maximum) {}
}

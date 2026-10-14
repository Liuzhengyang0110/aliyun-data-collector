package cn.kjky.collector.control;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 控制任务幂等账本。每次状态变化先写临时文件并 fsync，再在同一目录原子替换；
 * 损坏文件和遗留临时文件均失败关闭，避免重复执行未知任务。
 */
public final class JobStateStore {
    public static final String FILE_NAME = "control-jobs.json";
    private static final String SCHEMA_VERSION = "1.0";

    private final Path file;
    private final Path partial;
    private final ObjectMapper json = new ObjectMapper().registerModule(new JavaTimeModule())
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    private StoreState current;

    public JobStateStore(Path file) throws IOException {
        this.file = Objects.requireNonNull(file, "file 不能为空").toAbsolutePath().normalize();
        if (this.file.getParent() == null) throw new IllegalArgumentException("任务状态文件必须有父目录");
        partial = this.file.resolveSibling(this.file.getFileName() + ".partial");
        Files.createDirectories(this.file.getParent());
        if (Files.exists(partial)) throw new IOException("发现未完成的任务状态临时文件，已失败关闭: " + partial);
        current = Files.exists(this.file) ? read() : StoreState.initial();
    }

    public synchronized Optional<JobRecord> find(String jobId) {
        return Optional.ofNullable(current.jobs().get(jobId));
    }

    public synchronized long lastAppliedConfigVersion() {
        return current.lastAppliedConfigVersion();
    }

    public synchronized String lastCompletedJobId() {
        return current.lastCompletedJobId();
    }

    public synchronized List<JobRecord> pendingReports() {
        List<JobRecord> result = new ArrayList<>();
        current.jobs().values().stream().filter(record -> !record.reported()).forEach(result::add);
        return List.copyOf(result);
    }

    /** 最近一次发生状态变化的控制任务摘要。 */
    public synchronized Optional<JobRecord> latest() {
        return current.jobs().values().stream()
                .max(java.util.Comparator.comparing(JobRecord::occurredAt));
    }

    /** 不暴露控制包内部枚举的只读摘要，供节点状态协议使用。 */
    public synchronized Optional<JobSummaryView> latestSummary() {
        return latest().map(record -> new JobSummaryView(record.jobId(), record.commandType(),
                record.configVersion(), record.status().name(), record.occurredAt(), record.rejectReason()));
    }

    public synchronized JobRecord transition(JobRecord record, boolean policyApplied) throws IOException {
        Objects.requireNonNull(record, "record 不能为空");
        JobRecord before = current.jobs().get(record.jobId());
        if (before != null && terminal(before.status()) && before.status() != record.status()) {
            throw new IOException("终态任务不能回退或改写: " + record.jobId());
        }
        LinkedHashMap<String, JobRecord> jobs = new LinkedHashMap<>(current.jobs());
        jobs.put(record.jobId(), record);
        long version = policyApplied ? Math.max(current.lastAppliedConfigVersion(), record.configVersion())
                : current.lastAppliedConfigVersion();
        String completed = terminal(record.status()) ? record.jobId() : current.lastCompletedJobId();
        save(new StoreState(SCHEMA_VERSION, version, completed, jobs));
        return record;
    }

    public synchronized void markReported(String jobId, ControlProtocol.Status status, Instant occurredAt)
            throws IOException {
        JobRecord record = current.jobs().get(jobId);
        if (record == null || record.status() != status || !Objects.equals(record.occurredAt(), occurredAt)
                || record.reported()) return;
        LinkedHashMap<String, JobRecord> jobs = new LinkedHashMap<>(current.jobs());
        jobs.put(jobId, record.withReported(true));
        save(new StoreState(SCHEMA_VERSION, current.lastAppliedConfigVersion(),
                current.lastCompletedJobId(), jobs));
    }

    private void save(StoreState state) throws IOException {
        validate(state);
        byte[] bytes = json.writeValueAsBytes(state);
        try (FileChannel channel = FileChannel.open(partial, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) channel.write(buffer);
            channel.force(true);
        }
        try {
            Files.move(partial, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            throw new IOException("任务状态所在文件系统不支持原子替换: " + file, e);
        }
        current = state;
    }

    private StoreState read() throws IOException {
        try {
            StoreState state = json.readValue(file.toFile(), StoreState.class);
            validate(state);
            return state;
        } catch (Exception e) {
            throw new IOException("控制任务状态文件损坏，已失败关闭: " + file, e);
        }
    }

    private void validate(StoreState state) throws IOException {
        if (state == null || !SCHEMA_VERSION.equals(state.schemaVersion())
                || state.lastAppliedConfigVersion() < 0 || state.jobs() == null) {
            throw new IOException("控制任务状态字段无效");
        }
        for (Map.Entry<String, JobRecord> entry : state.jobs().entrySet()) {
            JobRecord r = entry.getValue();
            if (r == null || !entry.getKey().equals(r.jobId()) || blank(r.fingerprint())
                    || blank(r.collectorId()) || blank(r.siteId()) || blank(r.commandType())
                    || r.configVersion() < 1 || r.status() == null || r.occurredAt() == null) {
                throw new IOException("控制任务摘要损坏: " + entry.getKey());
            }
        }
    }

    private boolean blank(String value) {
        return value == null || value.isBlank();
    }

    static boolean terminal(ControlProtocol.Status status) {
        return status == ControlProtocol.Status.SUCCEEDED
                || status == ControlProtocol.Status.FAILED
                || status == ControlProtocol.Status.REJECTED;
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = false)
    public record StoreState(String schemaVersion, long lastAppliedConfigVersion,
                             String lastCompletedJobId, Map<String, JobRecord> jobs) {
        static StoreState initial() {
            return new StoreState(SCHEMA_VERSION, 0, null, Map.of());
        }
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = false)
    public record JobRecord(String jobId, String fingerprint, String collectorId, String siteId,
                            String commandType, long configVersion, ControlProtocol.Status status,
                            Instant occurredAt, Long requestedQuotaBytes, Long effectiveQuotaBytes,
                            Long localMinBytes, Long localMaxBytes, String rejectReason,
                            String message, boolean reported) {
        JobRecord withReported(boolean value) {
            return new JobRecord(jobId, fingerprint, collectorId, siteId, commandType, configVersion,
                    status, occurredAt, requestedQuotaBytes, effectiveQuotaBytes, localMinBytes,
                    localMaxBytes, rejectReason, message, value);
        }
    }

    public record JobSummaryView(String jobId, String commandType, long configVersion,
                                 String status, Instant occurredAt, String rejectReason) {}
}

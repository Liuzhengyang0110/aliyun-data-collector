package cn.kjky.collector.runtime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
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
import java.util.Objects;
import java.util.Optional;

/** 原子保存容量保护状态，使 Pod 重启后不会暂时绕过既有 CRITICAL 状态。 */
public final class CapacityStateStore {
    public static final String FILE_NAME = "capacity-state.json";
    public static final String SCHEMA_VERSION = "1.0";

    private final Path file;
    private final Path partial;
    private final ObjectMapper json = new ObjectMapper().registerModule(new JavaTimeModule());
    private CapacitySnapshot current;

    public CapacityStateStore(Path file) throws IOException {
        this.file = Objects.requireNonNull(file, "file 不能为空").toAbsolutePath().normalize();
        if (this.file.getParent() == null) throw new IllegalArgumentException("容量状态文件必须有父目录");
        this.partial = this.file.resolveSibling(this.file.getFileName() + ".partial");
        Files.createDirectories(this.file.getParent());
        if (Files.exists(partial)) {
            throw new IOException("发现未完成的容量状态临时文件，拒绝静默恢复: " + partial);
        }
        if (Files.exists(this.file)) current = readAndValidate();
    }

    public synchronized Optional<CapacitySnapshot> load() {
        return Optional.ofNullable(current);
    }

    public synchronized void save(CapacitySnapshot snapshot) throws IOException {
        validate(snapshot);
        byte[] bytes = json.writeValueAsBytes(snapshot);
        try (FileChannel channel = FileChannel.open(partial,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) channel.write(buffer);
            channel.force(true);
        }
        try {
            Files.move(partial, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            throw new IOException("容量状态所在文件系统不支持原子替换: " + file, e);
        }
        current = snapshot;
    }

    public Path file() {
        return file;
    }

    private CapacitySnapshot readAndValidate() throws IOException {
        try {
            CapacitySnapshot snapshot = json.readValue(file.toFile(), CapacitySnapshot.class);
            validate(snapshot);
            return snapshot;
        } catch (Exception e) {
            throw new IOException("容量状态文件损坏，已失败关闭: " + file, e);
        }
    }

    private void validate(CapacitySnapshot snapshot) throws IOException {
        if (snapshot == null || !SCHEMA_VERSION.equals(snapshot.schemaVersion())) {
            throw new IOException("不支持或缺失的容量状态 Schema");
        }
        if (snapshot.state() == null || snapshot.checkedAt() == null) {
            throw new IOException("容量状态缺少 state 或 checked_at");
        }
        if (!Double.isFinite(snapshot.usedPercent())
                || snapshot.usedPercent() < 0 || snapshot.usedPercent() > 100) {
            throw new IOException("容量状态 used_percent 必须在 0..100");
        }
        if (snapshot.totalBytes() < 0 || snapshot.usableBytes() < 0
                || snapshot.usableBytes() > snapshot.totalBytes()) {
            throw new IOException("容量状态字节数无效");
        }
    }

    /** 持久化的最近容量状态；reason 仅保存有限分类，不保存异常正文。 */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = false)
    public record CapacitySnapshot(String schemaVersion, CapacityState state, double usedPercent,
                                   long totalBytes, long usableBytes, Instant checkedAt, String reason) {
        public CapacitySnapshot {
            Objects.requireNonNull(schemaVersion, "schemaVersion 不能为空");
            Objects.requireNonNull(state, "state 不能为空");
            Objects.requireNonNull(checkedAt, "checkedAt 不能为空");
        }

        public static CapacitySnapshot measured(CapacityState state, DiskUsageProbe.Usage usage,
                                                Instant checkedAt) {
            return new CapacitySnapshot(SCHEMA_VERSION, state, usage.usedPercent(),
                    usage.totalBytes(), usage.usableBytes(), checkedAt, null);
        }
    }
}

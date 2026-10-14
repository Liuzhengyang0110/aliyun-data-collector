package cn.kjky.collector.control;

import cn.kjky.collector.runtime.CollectionSource;
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
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/** 原子持久化中控人工暂停；文件损坏或遗留临时文件时构造失败并阻止 daemon 启动。 */
public final class ManualPauseStateStore {
    public static final String FILE_NAME = "manual-pause-state.json";
    private static final String SCHEMA_VERSION = "1.0";

    private final Path file;
    private final Path partial;
    private final ObjectMapper json = new ObjectMapper().registerModule(new JavaTimeModule())
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    private PauseState current;

    public ManualPauseStateStore(Path file) throws IOException {
        this.file = Objects.requireNonNull(file, "file 不能为空").toAbsolutePath().normalize();
        if (this.file.getParent() == null) throw new IllegalArgumentException("暂停状态文件必须有父目录");
        this.partial = this.file.resolveSibling(this.file.getFileName() + ".partial");
        Files.createDirectories(this.file.getParent());
        if (Files.exists(partial)) throw new IOException("发现未完成的暂停状态临时文件，已失败关闭: " + partial);
        current = Files.exists(this.file) ? read() : PauseState.initial();
    }

    public synchronized PauseState snapshot() {
        return current;
    }

    public synchronized boolean paused(CollectionSource source) {
        return current.globalPaused() || current.pausedSources().contains(source);
    }

    public synchronized PauseState pause(CollectionSource source, long configVersion,
                                         String reason, Instant now) throws IOException {
        ensureNewer(configVersion);
        EnumSet<CollectionSource> sources = copySources();
        boolean global = current.globalPaused();
        if (source == null) global = true;
        else sources.add(source);
        PauseState next = new PauseState(SCHEMA_VERSION, configVersion, global,
                Set.copyOf(sources), safeReason(reason), now);
        save(next);
        return next;
    }

    public synchronized PauseState resume(CollectionSource source, long configVersion,
                                          Instant now) throws IOException {
        ensureNewer(configVersion);
        EnumSet<CollectionSource> sources = copySources();
        boolean global = current.globalPaused();
        if (source == null) {
            global = false;
            sources.clear();
        } else {
            sources.remove(source);
        }
        PauseState next = new PauseState(SCHEMA_VERSION, configVersion, global,
                Set.copyOf(sources), null, now);
        save(next);
        return next;
    }

    private EnumSet<CollectionSource> copySources() {
        return current.pausedSources().isEmpty()
                ? EnumSet.noneOf(CollectionSource.class)
                : EnumSet.copyOf(current.pausedSources());
    }

    private void ensureNewer(long configVersion) {
        if (configVersion <= current.configVersion()) {
            throw new IllegalArgumentException("config_version 必须大于暂停策略版本 " + current.configVersion());
        }
    }

    private void save(PauseState state) throws IOException {
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
            throw new IOException("暂停状态所在文件系统不支持原子替换: " + file, e);
        }
        current = state;
    }

    private PauseState read() throws IOException {
        try {
            PauseState state = json.readValue(file.toFile(), PauseState.class);
            validate(state);
            return state;
        } catch (Exception e) {
            throw new IOException("人工暂停状态文件损坏，已失败关闭: " + file, e);
        }
    }

    private void validate(PauseState state) throws IOException {
        if (state == null || !SCHEMA_VERSION.equals(state.schemaVersion())) throw new IOException("暂停状态 Schema 无效");
        if (state.configVersion() < 0 || state.pausedSources() == null || state.updatedAt() == null) {
            throw new IOException("暂停状态字段无效");
        }
        if (state.reason() != null && state.reason().length() > 256) throw new IOException("暂停原因过长");
    }

    private String safeReason(String reason) {
        if (reason == null || reason.isBlank()) return null;
        return reason.length() <= 256 ? reason : reason.substring(0, 256);
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = false)
    public record PauseState(String schemaVersion, long configVersion, boolean globalPaused,
                             Set<CollectionSource> pausedSources, String reason, Instant updatedAt) {
        static PauseState initial() {
            return new PauseState(SCHEMA_VERSION, 0, false, Set.of(), null, Instant.EPOCH);
        }
    }
}

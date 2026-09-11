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
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 生产采集链路的任务级水位存储。
 * <p>
 * 仅当某个任务窗口的所有批次已经可靠进入 Outbox pending 后，调用方才可推进水位。
 * 文件损坏、未知 Schema 和残留临时文件一律失败关闭，禁止把状态静默重置为当前时间。
 * </p>
 */
public final class CollectionWatermarkStore {
    public static final String SCHEMA_VERSION = "1.0";

    private final Path file;
    private final Path partial;
    private final Clock clock;
    private final ObjectMapper json = new ObjectMapper().registerModule(new JavaTimeModule());
    private final Map<String, Watermark> completed = new LinkedHashMap<>();
    private final Map<String, Instant> initialStarts = new LinkedHashMap<>();

    public CollectionWatermarkStore(Path file) throws IOException {
        this(file, Clock.systemUTC());
    }

    CollectionWatermarkStore(Path file, Clock clock) throws IOException {
        this.file = Objects.requireNonNull(file, "file 不能为空").toAbsolutePath().normalize();
        this.partial = this.file.resolveSibling(this.file.getFileName() + ".partial");
        this.clock = Objects.requireNonNull(clock, "clock 不能为空");
        if (this.file.getParent() == null) throw new IllegalArgumentException("水位文件必须有父目录");
        Files.createDirectories(this.file.getParent());
        if (Files.exists(partial)) {
            throw new IOException("发现未完成的采集水位临时文件，拒绝静默恢复: " + partial);
        }
        if (Files.exists(this.file)) load();
    }

    /** 返回任务已完整进入 pending 的最近窗口上界。 */
    public synchronized Optional<Watermark> get(String taskKey) {
        return Optional.ofNullable(completed.get(requireKey(taskKey)));
    }

    /** 返回所有任务水位的不可变快照，便于健康检查和测试。 */
    public synchronized Map<String, Watermark> snapshot() {
        return Map.copyOf(completed);
    }

    /**
     * 返回任务下一次查询起点。首轮起点先可靠持久化为锚点，确保首轮失败或重启时不会缩短回看范围。
     */
    public synchronized Instant startFor(String taskKey, Instant proposedInitialStart) throws IOException {
        String key = requireKey(taskKey);
        Objects.requireNonNull(proposedInitialStart, "proposedInitialStart 不能为空");
        Watermark watermark = completed.get(key);
        if (watermark != null) return watermark.windowEnd();
        Instant anchored = initialStarts.get(key);
        if (anchored != null) return anchored;
        Map<String, Instant> candidateStarts = new LinkedHashMap<>(initialStarts);
        candidateStarts.put(key, proposedInitialStart);
        save(completed, candidateStarts);
        initialStarts.clear();
        initialStarts.putAll(candidateStarts);
        return proposedInitialStart;
    }

    /**
     * 原子推进任务水位。相同或更早值不会倒退；成功时间默认取当前 UTC 时钟。
     */
    public synchronized void advance(String taskKey, Instant windowEnd) throws IOException {
        advance(taskKey, windowEnd, clock.instant());
    }

    /** 供确定性测试或上层传入统一完成时刻使用。 */
    public synchronized void advance(String taskKey, Instant windowEnd, Instant succeededAt) throws IOException {
        String key = requireKey(taskKey);
        Objects.requireNonNull(windowEnd, "windowEnd 不能为空");
        Objects.requireNonNull(succeededAt, "succeededAt 不能为空");
        Watermark current = completed.get(key);
        if (current != null && !windowEnd.isAfter(current.windowEnd())) return;

        Map<String, Watermark> candidate = new LinkedHashMap<>(completed);
        candidate.put(key, new Watermark(key, windowEnd, succeededAt));
        Map<String, Instant> candidateStarts = new LinkedHashMap<>(initialStarts);
        candidateStarts.remove(key);
        save(candidate, candidateStarts);
        completed.clear();
        completed.putAll(candidate);
        initialStarts.clear();
        initialStarts.putAll(candidateStarts);
    }

    public Path file() {
        return file;
    }

    private void load() throws IOException {
        try {
            State state = json.readValue(file.toFile(), State.class);
            if (state == null || !SCHEMA_VERSION.equals(state.schemaVersion)) {
                throw new IOException("不支持或缺失的采集水位 Schema: " + (state == null ? null : state.schemaVersion));
            }
            Map<String, Watermark> loadedCompleted = state.completed == null ? Map.of() : state.completed;
            Map<String, Instant> loadedInitial = state.initialStarts == null ? Map.of() : state.initialStarts;
            for (var entry : loadedCompleted.entrySet()) {
                String key = requireKey(entry.getKey());
                Watermark value = Objects.requireNonNull(entry.getValue(), "任务水位不能为空: " + key);
                if (!key.equals(value.taskKey())) throw new IOException("任务水位键与内容不一致: " + key);
                completed.put(key, value);
            }
            for (var entry : loadedInitial.entrySet()) {
                initialStarts.put(requireKey(entry.getKey()), Objects.requireNonNull(entry.getValue(), "初始水位不能为空"));
            }
        } catch (Exception e) {
            throw new IOException("采集水位文件损坏，已失败关闭: " + file, e);
        }
    }

    private void save(Map<String, Watermark> candidateCompleted,
                      Map<String, Instant> candidateStarts) throws IOException {
        State state = new State();
        state.schemaVersion = SCHEMA_VERSION;
        state.completed.putAll(candidateCompleted);
        state.initialStarts.putAll(candidateStarts);
        byte[] bytes = json.writeValueAsBytes(state);
        try (FileChannel channel = FileChannel.open(partial, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) channel.write(buffer);
            channel.force(true);
        }
        try {
            Files.move(partial, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            throw new IOException("采集水位所在文件系统不支持原子替换: " + file, e);
        }
    }

    private static String requireKey(String taskKey) {
        if (taskKey == null || taskKey.isBlank()) throw new IllegalArgumentException("taskKey 不能为空");
        return taskKey.trim();
    }

    /** 一个任务的最新完整成功窗口。 */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = false)
    public record Watermark(String taskKey, Instant windowEnd, Instant lastSuccessAt) {
        public Watermark {
            requireKey(taskKey);
            Objects.requireNonNull(windowEnd, "windowEnd 不能为空");
            Objects.requireNonNull(lastSuccessAt, "lastSuccessAt 不能为空");
        }
    }

    /** 水位文件的版本化根结构。 */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = false)
    public static final class State {
        public String schemaVersion;
        public Map<String, Watermark> completed = new LinkedHashMap<>();
        public Map<String, Instant> initialStarts = new LinkedHashMap<>();
    }
}

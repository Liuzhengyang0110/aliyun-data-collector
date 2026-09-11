package cn.kjky.collector.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

/**
 * 保存常驻轮询中每个数据源已经完整采集的时间上界。
 * 只有整轮采集成功后才允许推进，文件通过同目录临时文件原子替换。
 */
public final class PollingWatermarkStore {
    private final Path file;
    private final ObjectMapper json = new ObjectMapper().registerModule(new JavaTimeModule());
    private final Map<CollectionSource, Instant> watermarks = new EnumMap<>(CollectionSource.class);
    private final Map<CollectionSource, Instant> initialStarts = new EnumMap<>(CollectionSource.class);

    /** 打开已有水位文件，或者创建其父目录以准备首次写入。 */
    public PollingWatermarkStore(Path file) throws IOException {
        this.file = file.toAbsolutePath().normalize();
        Path parent = this.file.getParent();
        if (parent == null) throw new IllegalArgumentException("水位文件必须有父目录");
        Files.createDirectories(parent);
        if (Files.isRegularFile(this.file)) {
            State saved = json.readValue(this.file.toFile(), State.class);
            restore(saved.completed, watermarks);
            restore(saved.initialStarts, initialStarts);
        }
    }

    /** 返回数据源最近一次完整成功采集的时间上界。 */
    public synchronized Optional<Instant> get(CollectionSource source) {
        return Optional.ofNullable(watermarks.get(source));
    }

    /**
     * 返回下一轮查询起点。首次运行时先把建议起点作为锚点可靠落盘，
     * 因此首轮失败或进程重启也不会让回看窗口的起点向前滑动。
     */
    public synchronized Instant startFor(CollectionSource source, Instant proposedInitialStart) throws IOException {
        Instant completed = watermarks.get(source);
        if (completed != null) return completed;
        Instant anchored = initialStarts.get(source);
        if (anchored != null) return anchored;
        Map<CollectionSource, Instant> candidateStarts = new EnumMap<>(initialStarts);
        candidateStarts.put(source, proposedInitialStart);
        save(watermarks, candidateStarts);
        initialStarts.clear();
        initialStarts.putAll(candidateStarts);
        return proposedInitialStart;
    }

    /**
     * 向前推进一个数据源的水位并原子落盘。相同或更早的值会被忽略，
     * 防止系统时间回拨导致水位倒退。
     */
    public synchronized void advance(CollectionSource source, Instant completedUntil) throws IOException {
        Instant current = watermarks.get(source);
        if (current != null && !completedUntil.isAfter(current)) return;
        Map<CollectionSource, Instant> candidate = new EnumMap<>(watermarks);
        candidate.put(source, completedUntil);
        Map<CollectionSource, Instant> candidateStarts = new EnumMap<>(initialStarts);
        candidateStarts.remove(source);
        save(candidate, candidateStarts);
        watermarks.clear();
        watermarks.putAll(candidate);
        initialStarts.clear();
        initialStarts.putAll(candidateStarts);
    }

    /** 把完整状态写入临时文件并原子替换正式文件。 */
    private void save(Map<CollectionSource, Instant> completed,
                      Map<CollectionSource, Instant> starts) throws IOException {
        State saved = new State();
        copy(completed, saved.completed);
        copy(starts, saved.initialStarts);
        Path partial = file.resolveSibling(file.getFileName() + ".partial");
        json.writeValue(partial.toFile(), saved);
        try {
            Files.move(partial, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(partial, file, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private void restore(Map<String, Instant> source, Map<CollectionSource, Instant> target) {
        if (source == null) return;
        for (var entry : source.entrySet()) {
            target.put(CollectionSource.valueOf(entry.getKey()), entry.getValue());
        }
    }

    private void copy(Map<CollectionSource, Instant> source, Map<String, Instant> target) {
        for (CollectionSource value : CollectionSource.values()) {
            Instant instant = source.get(value);
            if (instant != null) target.put(value.name(), instant);
        }
    }

    /** poll-watermarks.json 的版本化前内部结构。 */
    public static final class State {
        public Map<String, Instant> completed = new java.util.LinkedHashMap<>();
        public Map<String, Instant> initialStarts = new java.util.LinkedHashMap<>();
    }
}

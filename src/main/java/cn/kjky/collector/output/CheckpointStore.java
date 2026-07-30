package cn.kjky.collector.output;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 断点文件存储。
 * 使用字符串键记录已经完成的页、Trace 详情或任务窗口。
 */
public final class CheckpointStore {
    /** checkpoint.json 的绝对路径。 */
    private final Path file;
    /** 与输出会话共享时间类型配置的 JSON 解析器。 */
    private final ObjectMapper json;
    /** 内存中的已完成键集合，LinkedHashSet 保持写入顺序。 */
    private final Set<String> completed;

    /**
     * 打开或创建 checkpoint。
     *
     * @param root 输出根目录
     * @param json JSON 序列化器
     * @throws IOException 已有 checkpoint 无法读取
     */
    public CheckpointStore(Path root, ObjectMapper json) throws IOException {
        this.file = root.resolve("checkpoints/checkpoint.json");
        this.json = json;
        if (Files.isRegularFile(file)) {
            completed = json.readValue(file.toFile(), new TypeReference<Set<String>>() {});
        }
        else completed = new LinkedHashSet<>();
    }

    /**
     * 判断某个工作单元是否已经成功。
     *
     * @param key 由采集源、任务、窗口、页码等组成的稳定键
     * @return true 表示恢复时应跳过该工作单元
     */
    public synchronized boolean contains(String key) { return completed.contains(key); }

    /**
     * 把工作单元标记为完成，并原子更新 checkpoint 文件。
     *
     * @param key 已完成工作单元的稳定键
     * @throws IOException 临时文件写入或改名失败
     */
    public synchronized void mark(String key) throws IOException {
        if (!completed.add(key)) return;
        // partial 是新 checkpoint 的临时文件，写完后才替换正式文件。
        Path partial = file.resolveSibling(file.getFileName() + ".partial");
        json.writeValue(partial.toFile(), completed);
        try {
            Files.move(partial, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(partial, file, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}

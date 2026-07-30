package cn.kjky.collector.output;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.core.TimeWindow;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * 一次批次输出会话。
 * 统一管理目录、分片号、原始文件原子写入、manifest 和运行日志。
 */
public final class OutputSession {
    /** 当前批次配置。 */
    private final CollectorConfig config;
    /** 规范化后的绝对输出根目录。 */
    private final Path root;
    /** 支持 Instant 的 JSON 序列化器。 */
    private final ObjectMapper json;
    /** 文件名生成器。 */
    private final FileNaming naming = new FileNaming();
    /** SHA-256 计算器。 */
    private final Hashing hashing = new Hashing();
    /** 每个“采集器 + 类型 + 窗口”当前最大分片号。 */
    private final Map<String, Integer> parts = new HashMap<>();
    /** 当前批次内存中的正式清单。 */
    private final Manifest manifest;

    /**
     * 初始化输出目录，并新建或恢复 manifest。
     *
     * @param config 当前采集配置
     * @param start 本次命令开始时间
     * @param end 本次命令结束时间
     * @throws IOException 目录或已有 manifest 无法读取
     * @throws IllegalStateException 输出目录属于其他项目或批次
     */
    public OutputSession(CollectorConfig config, Instant start, Instant end) throws IOException {
        this.config = config;
        this.root = Path.of(config.outputRoot).toAbsolutePath().normalize();
        this.json = new ObjectMapper().registerModule(new JavaTimeModule()).enable(SerializationFeature.INDENT_OUTPUT);
        Files.createDirectories(root.resolve("raw/arms"));
        Files.createDirectories(root.resolve("raw/sls"));
        Files.createDirectories(root.resolve("manifest"));
        Files.createDirectories(root.resolve("checkpoints"));
        Files.createDirectories(root.resolve("logs"));
        // existing 指向可能存在的正式清单；resume 会从这里恢复文件和分片状态。
        Path existing = root.resolve("manifest/manifest.json");
        if (Files.isRegularFile(existing)) {
            this.manifest = json.readValue(existing.toFile(), Manifest.class);
            if (!config.batchId.equals(manifest.batchId) || !config.projectCode.equals(manifest.projectCode)) {
                throw new IllegalStateException("outputRoot 中已有其他项目或批次的 manifest，请更换输出目录");
            }
            if (manifest.collectionStart == null || start.isBefore(manifest.collectionStart)) manifest.collectionStart = start;
            if (manifest.collectionEnd == null || end.isAfter(manifest.collectionEnd)) manifest.collectionEnd = end;
        } else {
            this.manifest = new Manifest();
            manifest.schemaVersion = config.schemaVersion;
            manifest.projectCode = config.projectCode;
            manifest.envCode = config.envCode;
            manifest.batchId = config.batchId;
            manifest.caseId = config.caseId;
            manifest.timezone = config.timezone;
            manifest.collectionStart = start;
            manifest.collectionEnd = end;
            manifest.collectorVersion = "0.1.0";
        }
        // 根据已有 FileEntry 恢复每组最大分片号，保证续采不覆盖旧文件。
        for (Manifest.FileEntry e : manifest.files) {
            String key = e.collectorId + "|" + e.recordType + "|" + e.windowStart + "|" + e.windowEnd;
            parts.merge(key, e.part, Math::max);
        }
    }

    /**
     * 原子写入一份 raw 数据，并立即追加 manifest 文件项。
     *
     * @param collectorId 采集源标识：arms 或 sls
     * @param recordType 命名规范中的记录类型
     * @param window 数据覆盖的逻辑时间窗口
     * @param content 要写入的 UTF-8 JSON 字节
     * @param records 可识别的记录数；无法判断时可为 -1
     * @param requestId 服务端请求 ID
     * @return 最终正式文件路径
     * @throws IOException 目录、文件、哈希或 manifest 写入失败
     */
    public synchronized Path write(String collectorId, String recordType, TimeWindow window, byte[] content,
                                   long records, String requestId) throws IOException {
        // key 用于在内存中隔离不同采集器、类型和时间窗的分片号。
        String key = collectorId + "|" + recordType + "|" + window.start() + "|" + window.end();
        // part 是当前文件的新分片号。
        int part = parts.merge(key, 1, Integer::sum);
        String filename = naming.name(config, collectorId, recordType, window, part, "json");
        // directory 只能位于 root/raw 下，startsWith 检查防止路径穿越。
        Path directory = root.resolve("raw").resolve(collectorId).normalize();
        if (!directory.startsWith(root.resolve("raw"))) throw new SecurityException("输出路径越界");
        Files.createDirectories(directory);
        // target 是正式文件，partial 是写入过程中的临时文件。
        Path target = directory.resolve(filename);
        Path partial = directory.resolve(filename + ".partial");
        Files.write(partial, content);
        moveAtomic(partial, target);

        // entry 保存本文件的来源、窗口、大小和完整性信息。
        Manifest.FileEntry entry = new Manifest.FileEntry();
        entry.relativePath = root.relativize(target).toString().replace('\\', '/');
        entry.collectorId = collectorId;
        entry.recordType = recordType;
        entry.windowStart = window.start();
        entry.windowEnd = window.end();
        entry.part = part;
        entry.bytes = Files.size(target);
        entry.records = records;
        entry.sha256 = hashing.sha256(target);
        entry.requestId = requestId;
        manifest.files.add(entry);
        saveManifest();
        return target;
    }

    /**
     * 把异常摘要追加到 manifest。
     *
     * @param operation 失败的操作或窗口描述
     * @param e 原始异常
     * @throws IOException manifest 更新失败
     */
    public synchronized void error(String operation, Exception e) throws IOException {
        // entry 只保存脱敏摘要，不保存堆栈和凭据。
        Manifest.ErrorEntry entry = new Manifest.ErrorEntry();
        entry.time = Instant.now();
        entry.operation = operation;
        entry.message = e.getClass().getSimpleName() + ": " + safe(e.getMessage());
        manifest.errors.add(entry);
        saveManifest();
    }

    /**
     * 更新时间戳并原子保存正式 manifest。
     *
     * @throws IOException JSON 序列化或文件替换失败
     */
    public synchronized void saveManifest() throws IOException {
        manifest.generatedAt = Instant.now();
        writeJsonAtomic(root.resolve("manifest/manifest.json"), manifest);
    }

    /**
     * 向 collector.log 追加一行已脱敏运行日志。
     *
     * @param level 日志级别，例如 INFO 或 ERROR
     * @param message 日志正文
     * @throws IOException 日志文件写入失败
     */
    public void appendRunLog(String level, String message) throws IOException {
        // sanitized 去掉凭据关键词和换行，确保每条日志只占一行。
        String sanitized = safe(message).replace("\r", " ").replace("\n", " ");
        String line = Instant.now() + "\t" + level + "\t" + sanitized + System.lineSeparator();
        Files.writeString(root.resolve("logs/collector.log"), line, StandardCharsets.UTF_8,
                java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
    }

    /** @return 绝对输出根目录 */
    public Path root() { return root; }
    /** @return 当前会话 JSON 序列化器 */
    public ObjectMapper json() { return json; }
    /** @return 当前内存 manifest */
    public Manifest manifest() { return manifest; }

    /**
     * 对可能进入日志或 manifest 的文本执行基础凭据脱敏。
     *
     * @param value 原始文本
     * @return 脱敏文本；null 转为空字符串
     */
    private String safe(String value) {
        if (value == null) return "";
        return value.replaceAll("(?i)(accesskey|secret|password|token)[^ ,;]*", "[REDACTED]");
    }

    /**
     * 把任意对象序列化到临时 JSON，再原子替换目标文件。
     *
     * @param target 正式目标路径
     * @param value 待序列化对象
     * @throws IOException 写入或替换失败
     */
    private void writeJsonAtomic(Path target, Object value) throws IOException {
        // partial 与 target 位于同一目录，尽可能保证原子移动可用。
        Path partial = target.resolveSibling(target.getFileName() + ".partial");
        json.writeValue(partial.toFile(), value);
        moveAtomic(partial, target);
    }

    /**
     * 优先原子移动文件；文件系统不支持时退化为普通替换移动。
     *
     * @param source 已完整写好的临时文件
     * @param target 正式目标文件
     * @throws IOException 两种移动方式都失败
     */
    private void moveAtomic(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}

package cn.kjky.collector.service;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.output.Hashing;
import cn.kjky.collector.output.Manifest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * 在正式 manifest 丢失时，从 raw 文件名和内容重建辅助清单。
 * 无法从文件恢复的记录数和 Request ID 不会伪造。
 */
public final class ManifestRebuilder {
    /** 从文件名尾部提取开始时间、结束时间和四位分片号。 */
    private static final Pattern TIMES = Pattern.compile("_(\\d{8}T\\d{6}Z)_(\\d{8}T\\d{6}Z)_p(\\d{4})\\.json$");
    /** 文件名 UTC 时间解析器。 */
    private static final DateTimeFormatter UTC = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

    /**
     * 扫描 raw 目录并生成 manifest-rebuilt.json。
     *
     * @param c 提供 outputRoot 和命名字段的配置
     * @return 生成的辅助清单路径
     * @throws IOException 目录扫描、文件读取、哈希或 JSON 写入失败
     */
    public Path rebuild(CollectorConfig c) throws IOException {
        // root 是规范化输出根目录；m 是待构建的新清单。
        Path root = Paths.get(c.outputRoot).toAbsolutePath().normalize();
        Manifest m = new Manifest();
        m.schemaVersion = c.schemaVersion;
        m.projectCode = c.projectCode;
        m.envCode = c.envCode;
        m.batchId = c.batchId;
        m.caseId = c.caseId;
        m.timezone = c.timezone;
        m.generatedAt = Instant.now();
        m.collectorVersion = "0.1.0-rebuilt";
        Hashing hashing = new Hashing();
        // raw 是扫描起点，仅处理其中的 .json 正式文件。
        Path raw = root.resolve("raw");
        if (Files.isDirectory(raw)) try (Stream<Path> paths = Files.walk(raw)) {
            paths.filter(Files::isRegularFile).filter(p -> p.getFileName().toString().endsWith(".json")).sorted().forEach(p -> {
                try {
                    // e 保存能够从路径、文件名和内容重新推导的字段。
                    Manifest.FileEntry e = new Manifest.FileEntry();
                    e.relativePath = root.relativize(p).toString().replace('\\', '/');
                    e.collectorId = raw.relativize(p).getName(0).toString();
                    // name 是文件名；prefix 用于定位 recordType 开始位置。
                    String name = p.getFileName().toString();
                    String prefix = c.projectCode + "_" + c.envCode + "_" + c.batchId + "_" + e.collectorId + "_";
                    // matcher 从固定尾部提取两个 UTC 时间和分片号。
                    Matcher matcher = TIMES.matcher(name);
                    if (name.startsWith(prefix) && matcher.find()) {
                        e.recordType = name.substring(prefix.length(), matcher.start());
                        e.windowStart = Instant.from(UTC.parse(matcher.group(1)));
                        e.windowEnd = Instant.from(UTC.parse(matcher.group(2)));
                        e.part = Integer.parseInt(matcher.group(3));
                    }
                    e.bytes = Files.size(p);
                    e.records = -1;
                    e.sha256 = hashing.sha256(p);
                    m.files.add(e);
                } catch (IOException ex) { throw new java.io.UncheckedIOException(ex); }
            });
        }
        // out 与正式 manifest 分开，避免辅助重建结果覆盖原清单。
        Path out = root.resolve("manifest/manifest-rebuilt.json");
        Files.createDirectories(out.getParent());
        new ObjectMapper().registerModule(new JavaTimeModule()).enable(SerializationFeature.INDENT_OUTPUT).writeValue(out.toFile(), m);
        return out;
    }
}

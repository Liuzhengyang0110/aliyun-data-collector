package cn.kjky.collector.service;

import cn.kjky.collector.output.Hashing;
import cn.kjky.collector.output.Manifest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** 根据正式 manifest 校验本地采集包完整性。 */
public final class OutputValidator {
    /**
     * 校验 manifest 中每个文件的存在性、大小、哈希和路径安全性。
     *
     * @param root 输出根目录
     * @return 所有发现的问题；空列表表示校验通过
     * @throws IOException manifest、raw 目录或文件无法读取
     */
    public List<String> validate(Path root) throws IOException {
        // errors 汇总全部问题，便于一次修复。
        List<String> errors = new ArrayList<>();
        // manifestFile 是正式清单位置。
        Path manifestFile = root.resolve("manifest/manifest.json");
        if (!Files.isRegularFile(manifestFile)) return List.of("manifest/manifest.json 不存在");
        ObjectMapper json = new ObjectMapper().registerModule(new JavaTimeModule());
        // manifest 提供期望文件列表；hashing 重新计算实际 SHA-256。
        Manifest manifest = json.readValue(manifestFile.toFile(), Manifest.class);
        Hashing hashing = new Hashing();
        // paths 用于检查清单内部是否重复记录同一路径。
        Set<String> paths = new HashSet<>();
        for (Manifest.FileEntry entry : manifest.files) {
            if (!paths.add(entry.relativePath)) errors.add("manifest 中路径重复: " + entry.relativePath);
            // file 经过 normalize 后仍必须位于 root 内，防止清单路径越界。
            Path file = root.resolve(entry.relativePath).normalize();
            if (!file.startsWith(root.normalize())) { errors.add("路径越界: " + entry.relativePath); continue; }
            if (!Files.isRegularFile(file)) { errors.add("文件缺失: " + entry.relativePath); continue; }
            if (Files.size(file) != entry.bytes) errors.add("文件大小不符: " + entry.relativePath);
            if (!hashing.sha256(file).equalsIgnoreCase(entry.sha256)) errors.add("SHA-256 不符: " + entry.relativePath);
        }
        // stream 扫描所有 raw 子目录，发现中断遗留的 .partial 文件。
        try (var stream = Files.walk(root.resolve("raw"))) {
            stream.filter(Files::isRegularFile).filter(p -> p.getFileName().toString().endsWith(".partial"))
                    .forEach(p -> errors.add("发现未完成文件: " + root.relativize(p)));
        }
        return errors;
    }
}

package cn.kjky.collector.runtime;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Objects;

/** 只统计 Outbox writing、pending 和 retry 中的文件内容字节。 */
public class OutboxUsageProbe {
    private static final List<String> COUNTED_DIRECTORIES = List.of("writing", "pending", "retry");

    public long measure(Path outboxRoot) throws IOException {
        Path root = Objects.requireNonNull(outboxRoot, "outboxRoot 不能为空")
                .toAbsolutePath().normalize();
        long total = 0;
        for (String name : COUNTED_DIRECTORIES) {
            Path directory = root.resolve(name);
            if (!Files.exists(directory)) continue;
            total = Math.addExact(total, measureTree(directory));
        }
        return total;
    }

    /** 供 OutboxStore 在删除完整批次前计算准确的持久化字节差量。 */
    public long measureTree(Path directory) throws IOException {
        if (!Files.exists(directory)) return 0;
        final long[] total = {0};
        Files.walkFileTree(directory, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                if (attributes.isSymbolicLink()) {
                    throw new IOException("Outbox 中不允许符号链接: " + file);
                }
                if (attributes.isRegularFile()) total[0] = Math.addExact(total[0], attributes.size());
                return FileVisitResult.CONTINUE;
            }
        });
        return total[0];
    }
}

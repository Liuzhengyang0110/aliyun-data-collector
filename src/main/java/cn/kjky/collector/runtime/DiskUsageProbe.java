package cn.kjky.collector.runtime;

import java.io.IOException;
import java.nio.file.Path;

/** 可替换的文件系统容量探针，生产读取 FileStore，测试可注入确定性结果。 */
@FunctionalInterface
public interface DiskUsageProbe {
    Usage measure(Path path) throws IOException;

    /** 文件系统总字节和当前可用字节。 */
    record Usage(long totalBytes, long usableBytes) {
        public Usage {
            if (totalBytes <= 0) throw new IllegalArgumentException("totalBytes 必须大于 0");
            if (usableBytes < 0 || usableBytes > totalBytes) {
                throw new IllegalArgumentException("usableBytes 必须在 0..totalBytes");
            }
        }

        public double usedPercent() {
            return (totalBytes - usableBytes) * 100.0d / totalBytes;
        }
    }
}

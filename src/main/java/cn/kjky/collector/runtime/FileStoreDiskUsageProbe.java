package cn.kjky.collector.runtime;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** 使用 Java FileStore 读取 Outbox 所在 PVC 文件系统的容量。 */
public final class FileStoreDiskUsageProbe implements DiskUsageProbe {
    @Override
    public Usage measure(Path path) throws IOException {
        Path target = path.toAbsolutePath().normalize();
        if (!Files.exists(target)) throw new IOException("容量检测路径不存在: " + target);
        var store = Files.getFileStore(target);
        return new Usage(store.getTotalSpace(), store.getUsableSpace());
    }
}

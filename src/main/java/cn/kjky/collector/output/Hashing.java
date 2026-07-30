package cn.kjky.collector.output;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** 文件完整性哈希工具。 */
public final class Hashing {
    /**
     * 流式计算文件 SHA-256，不把整个文件一次性读入内存。
     *
     * @param path 待计算的文件
     * @return 小写十六进制 SHA-256
     * @throws IOException 文件读取失败
     */
    public String sha256(Path path) throws IOException {
        try {
            // digest 保存 SHA-256 的增量计算状态。
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (var in = Files.newInputStream(path)) {
                // buffer 是固定 8 KiB 读取缓冲区；n 是本次实际读取字节数。
                byte[] buffer = new byte[8192];
                int n;
                while ((n = in.read(buffer)) >= 0) digest.update(buffer, 0, n);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}

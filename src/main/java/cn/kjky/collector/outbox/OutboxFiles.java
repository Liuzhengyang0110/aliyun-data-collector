package cn.kjky.collector.outbox;

import cn.kjky.collector.contract.BatchEnvelope;
import cn.kjky.collector.contract.IngestJson;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;

/** Outbox 文件格式、哈希和严格原子文件操作的包内工具。 */
final class OutboxFiles {
    static final String BATCH_FILE = "batch.json";
    static final String DESCRIPTOR_FILE = "outbox.json";
    static final String DELIVERY_FILE = "delivery.json";

    private static final ObjectMapper JSON = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    private OutboxFiles() {}

    static byte[] writeDescriptor(OutboxBatch value) throws IOException {
        return JSON.writeValueAsBytes(value);
    }

    static byte[] writeDelivery(OutboxDelivery value) throws IOException {
        return JSON.writeValueAsBytes(value);
    }

    static OutboxStoredBatch load(Path directory) throws IOException {
        Path normalized = directory.toAbsolutePath().normalize();
        byte[] batchBytes = Files.readAllBytes(normalized.resolve(BATCH_FILE));
        byte[] canonicalBytes;
        BatchEnvelope batch = IngestJson.readBatch(batchBytes);
        canonicalBytes = IngestJson.writeBatch(batch);
        if (!Arrays.equals(batchBytes, canonicalBytes)) {
            throw new IOException("batch.json 不是规范化 UTF-8 JSON: " + normalized.getFileName());
        }
        String eventHash = IngestJson.eventsSha256(batch.events());
        if (!eventHash.equalsIgnoreCase(batch.contentSha256())) {
            throw new IOException("batch.json 的 content_sha256 与事件内容不一致: " + batch.batchId());
        }
        for (var event : batch.events()) {
            String payloadHash = IngestJson.sha256Canonical(event.payload());
            if (!payloadHash.equalsIgnoreCase(event.payloadSha256())) {
                throw new IOException("事件 payload_sha256 不一致: " + event.eventId());
            }
        }

        OutboxBatch descriptor = JSON.readValue(normalized.resolve(DESCRIPTOR_FILE).toFile(), OutboxBatch.class);
        OutboxDelivery delivery = JSON.readValue(normalized.resolve(DELIVERY_FILE).toFile(), OutboxDelivery.class);
        try {
            descriptor.verify(batch, batchBytes);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new IOException(e.getMessage(), e);
        }
        if (!batch.batchId().equals(delivery.batchId())) {
            throw new IOException("delivery.json 与 batch.json 的 batch_id 不一致");
        }
        return new OutboxStoredBatch(descriptor, batch, delivery, normalized, batchBytes);
    }

    static boolean hasCompleteFileSet(Path directory) {
        return Files.isRegularFile(directory.resolve(BATCH_FILE))
                && Files.isRegularFile(directory.resolve(DESCRIPTOR_FILE))
                && Files.isRegularFile(directory.resolve(DELIVERY_FILE));
    }

    static void writeAtomic(Path target, byte[] bytes) throws IOException {
        Files.createDirectories(target.getParent());
        Path partial = target.resolveSibling(target.getFileName() + ".partial");
        try (FileChannel channel = FileChannel.open(partial, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) channel.write(buffer);
            channel.force(true);
        }
        moveAtomic(partial, target, true);
    }

    static void moveAtomic(Path source, Path target, boolean replaceExisting) throws IOException {
        try {
            if (replaceExisting) {
                Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } else {
                Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
            }
        } catch (AtomicMoveNotSupportedException e) {
            throw new IOException("Outbox 所在文件系统不支持同文件系统原子移动: " + source + " -> " + target, e);
        }
    }

    static String directoryKey(String batchId) {
        if (batchId == null || batchId.isBlank()) throw new IllegalArgumentException("batch_id 不能为空");
        return "batch-" + sha256(batchId.getBytes(StandardCharsets.UTF_8));
    }

    static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("当前 JRE 不支持 SHA-256", e);
        }
    }

    static void deleteIncompleteWriting(Path writingRoot, Path candidate) throws IOException {
        deleteTransientDirectory(writingRoot, candidate, ".partial");
    }

    /** 删除 writing 下具有明确后缀的瞬态目录；用于不完整写入和已确认后的删除残留。 */
    static void deleteTransientDirectory(Path writingRoot, Path candidate, String requiredSuffix) throws IOException {
        Path root = writingRoot.toAbsolutePath().normalize();
        Path target = candidate.toAbsolutePath().normalize();
        if (!target.startsWith(root) || target.equals(root)
                || requiredSuffix == null || !target.getFileName().toString().endsWith(requiredSuffix)) {
            throw new SecurityException("拒绝删除 Outbox writing 目录以外的路径: " + target);
        }
        if (!Files.exists(target)) return;
        try (var paths = Files.walk(target)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
        }
    }
}

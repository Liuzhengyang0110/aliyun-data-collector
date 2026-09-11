package cn.kjky.collector.outbox;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** 启动时校验并恢复 writing、pending 和 retry 中的 Outbox 批次。 */
public final class OutboxRecovery {
    private final Path outboxRoot;
    private final Path writingRoot;
    private final Path pendingRoot;
    private final Path retryRoot;

    public OutboxRecovery(Path outboxRoot) {
        this.outboxRoot = outboxRoot.toAbsolutePath().normalize();
        this.writingRoot = this.outboxRoot.resolve("writing");
        this.pendingRoot = this.outboxRoot.resolve("pending");
        this.retryRoot = this.outboxRoot.resolve("retry");
    }

    /**
     * 删除确定不完整的 writing 临时目录，恢复完整批次，并校验所有可发送批次。
     * pending/retry 只要存在完整文件集但校验失败就失败关闭，不会自动删除。
     */
    public RecoveryReport recover() throws IOException {
        Files.createDirectories(writingRoot);
        Files.createDirectories(pendingRoot);
        Files.createDirectories(retryRoot);

        int deletedIncomplete = 0;
        int restoredWriting = 0;
        int reconciledState = 0;

        for (Path candidate : childDirectories(writingRoot)) {
            if (candidate.getFileName().toString().endsWith(".deleting")) {
                // 该标记只会在上传水位已原子持久化后创建，重启时可以幂等完成删除。
                OutboxFiles.deleteTransientDirectory(writingRoot, candidate, ".deleting");
                continue;
            }
            requireWritingName(candidate);
            if (!OutboxFiles.hasCompleteFileSet(candidate)) {
                OutboxFiles.deleteIncompleteWriting(writingRoot, candidate);
                deletedIncomplete++;
                continue;
            }
            cleanupSidecarPartials(candidate);
            OutboxStoredBatch stored = loadStrict(candidate);
            Path destination = destination(stored.delivery().state()).resolve(baseName(candidate));
            moveRecovered(candidate, destination);
            restoredWriting++;
        }

        for (Path candidate : childDirectories(pendingRoot)) {
            requireStableName(candidate);
            cleanupSidecarPartials(candidate);
            OutboxStoredBatch stored = loadStrict(candidate);
            if (stored.delivery().state() == OutboxDelivery.State.RETRY) {
                moveRecovered(candidate, retryRoot.resolve(candidate.getFileName()));
                reconciledState++;
            }
        }

        for (Path candidate : childDirectories(retryRoot)) {
            requireStableName(candidate);
            cleanupSidecarPartials(candidate);
            OutboxStoredBatch stored = loadStrict(candidate);
            if (stored.delivery().state() != OutboxDelivery.State.RETRY) {
                moveRecovered(candidate, pendingRoot.resolve(candidate.getFileName()));
                reconciledState++;
            }
        }

        int pending = verifyStableDirectory(pendingRoot, false);
        int retry = verifyStableDirectory(retryRoot, true);
        return new RecoveryReport(restoredWriting, deletedIncomplete, reconciledState, pending, retry);
    }

    private int verifyStableDirectory(Path root, boolean retryExpected) throws IOException {
        int count = 0;
        for (Path candidate : childDirectories(root)) {
            OutboxStoredBatch stored = loadStrict(candidate);
            boolean isRetry = stored.delivery().state() == OutboxDelivery.State.RETRY;
            if (retryExpected != isRetry) {
                throw new IOException("Outbox 目录与 delivery 状态不一致: " + candidate);
            }
            count++;
        }
        return count;
    }

    private OutboxStoredBatch loadStrict(Path candidate) throws IOException {
        if (!OutboxFiles.hasCompleteFileSet(candidate)) {
            throw new IOException("Outbox 批次文件不完整，已保留并停止自动恢复: " + candidate);
        }
        try {
            OutboxStoredBatch stored = OutboxFiles.load(candidate);
            String expected = OutboxFiles.directoryKey(stored.batch().batchId());
            if (!expected.equals(baseName(candidate))) {
                throw new IOException("Outbox 目录名与 batch_id 不一致: " + candidate);
            }
            return stored;
        } catch (RuntimeException e) {
            throw new IOException("Outbox 批次校验失败，已保留并停止自动恢复: " + candidate, e);
        }
    }

    private Path destination(OutboxDelivery.State state) {
        return state == OutboxDelivery.State.RETRY ? retryRoot : pendingRoot;
    }

    private void moveRecovered(Path source, Path target) throws IOException {
        if (Files.exists(target)) throw new IOException("Outbox 恢复目标已存在，拒绝覆盖: " + target);
        OutboxFiles.moveAtomic(source, target, false);
    }

    private void cleanupSidecarPartials(Path directory) throws IOException {
        Files.deleteIfExists(directory.resolve(OutboxFiles.BATCH_FILE + ".partial"));
        Files.deleteIfExists(directory.resolve(OutboxFiles.DESCRIPTOR_FILE + ".partial"));
        Files.deleteIfExists(directory.resolve(OutboxFiles.DELIVERY_FILE + ".partial"));
    }

    private List<Path> childDirectories(Path root) throws IOException {
        try (var stream = Files.list(root)) {
            return stream.filter(Files::isDirectory).sorted().toList();
        }
    }

    private void requireWritingName(Path directory) throws IOException {
        if (!directory.getFileName().toString().endsWith(".partial")) {
            throw new IOException("writing 中存在非临时批次目录: " + directory);
        }
    }

    private void requireStableName(Path directory) throws IOException {
        if (!directory.getFileName().toString().matches("batch-[0-9a-f]{64}")) {
            throw new IOException("Outbox 中存在无法识别的批次目录: " + directory);
        }
    }

    private String baseName(Path directory) {
        String name = directory.getFileName().toString();
        return name.endsWith(".partial") ? name.substring(0, name.length() - ".partial".length()) : name;
    }

    public record RecoveryReport(
            int restoredWriting,
            int deletedIncompleteWriting,
            int reconciledStateDirectories,
            int pendingBatches,
            int retryBatches) {
    }
}

package cn.kjky.collector.outbox;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.contract.BatchEnvelope;
import cn.kjky.collector.contract.IngestJson;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单 Pod、单进程使用的 PVC 文件式 Outbox。
 * <p>
 * 批次目录从 writing 原子移动到 pending 后才对上传工作线程可见；进程内认领集合
 * 防止多个上传线程同时发送同一批次。多 Pod 共享同一 PVC 不在首期支持范围内。
 * </p>
 */
public final class OutboxStore {
    private final Path dataRoot;
    private final Path outboxRoot;
    private final Path writingRoot;
    private final Path pendingRoot;
    private final Path retryRoot;
    private final Path stateRoot;
    private final Clock clock;
    private final Set<String> claimed = new HashSet<>();
    private final OutboxRecovery.RecoveryReport recoveryReport;
    private final AtomicLong changeVersion = new AtomicLong();
    private final ReentrantLock changeLock = new ReentrantLock();
    private final Condition changed = changeLock.newCondition();
    private boolean preferRetry;

    /** 按采集配置初始化目录并执行启动恢复。不会创建 raw、manifest 或 committed。 */
    public OutboxStore(CollectorConfig config) throws IOException {
        this(resolveRoot(config), config.storage.outboxDir, config.storage.stateDir, Clock.systemUTC());
    }

    /** 允许测试或嵌入场景直接指定根目录和时钟。 */
    public OutboxStore(Path dataRoot, String outboxDirectory, String stateDirectory, Clock clock) throws IOException {
        this.dataRoot = Objects.requireNonNull(dataRoot, "dataRoot 不能为空").toAbsolutePath().normalize();
        this.clock = Objects.requireNonNull(clock, "clock 不能为空");
        this.outboxRoot = resolveInside(this.dataRoot, outboxDirectory, "outboxDirectory");
        this.stateRoot = resolveInside(this.dataRoot, stateDirectory, "stateDirectory");
        if (outboxRoot.startsWith(stateRoot) || stateRoot.startsWith(outboxRoot)) {
            throw new IllegalArgumentException("Outbox 与 state 目录不能相同或相互嵌套");
        }
        this.writingRoot = outboxRoot.resolve("writing");
        this.pendingRoot = outboxRoot.resolve("pending");
        this.retryRoot = outboxRoot.resolve("retry");

        Files.createDirectories(this.dataRoot);
        Files.createDirectories(this.stateRoot);
        this.recoveryReport = new OutboxRecovery(this.outboxRoot).recover();
    }

    /**
     * 将不可变上传批次写入 writing，强制落盘后将整个批次目录原子移动到 pending。
     * 同一 batch_id、同一内容的重复调用幂等返回；同 ID 异内容会失败。
     */
    public synchronized OutboxStoredBatch enqueue(BatchEnvelope batch, String parentBatchId) throws IOException {
        Objects.requireNonNull(batch, "batch 不能为空");
        validateBatchHashes(batch);
        byte[] serialized = IngestJson.writeBatch(batch);
        String directoryName = OutboxFiles.directoryKey(batch.batchId());

        Optional<OutboxStoredBatch> existing = findExisting(directoryName);
        if (existing.isPresent()) {
            OutboxStoredBatch stored = existing.get();
            if (!java.util.Arrays.equals(serialized, stored.serializedBatch())
                    || !Objects.equals(normalizeParent(parentBatchId), stored.descriptor().parentBatchId())) {
                throw new IllegalStateException("同一 batch_id 已存在不同的不可变内容: " + batch.batchId());
            }
            signalAvailabilityChange();
            return stored;
        }

        Path writing = writingRoot.resolve(directoryName + ".partial");
        if (Files.exists(writing)) {
            throw new IOException("存在同批次 writing 残留，请重新初始化 Outbox 执行恢复: " + batch.batchId());
        }
        Files.createDirectory(writing);

        Instant now = clock.instant();
        OutboxBatch descriptor = OutboxBatch.from(batch, serialized, now, normalizeParent(parentBatchId));
        OutboxDelivery delivery = OutboxDelivery.pending(batch.batchId(), now);
        OutboxFiles.writeAtomic(writing.resolve(OutboxFiles.BATCH_FILE), serialized);
        OutboxFiles.writeAtomic(writing.resolve(OutboxFiles.DESCRIPTOR_FILE), OutboxFiles.writeDescriptor(descriptor));
        OutboxFiles.writeAtomic(writing.resolve(OutboxFiles.DELIVERY_FILE), OutboxFiles.writeDelivery(delivery));

        Path pending = pendingRoot.resolve(directoryName);
        if (Files.exists(pending)) throw new IOException("pending 中已存在同名批次，拒绝覆盖: " + batch.batchId());
        OutboxFiles.moveAtomic(writing, pending, false);
        signalAvailabilityChange();
        return OutboxFiles.load(pending);
    }

    public OutboxStoredBatch enqueue(BatchEnvelope batch) throws IOException {
        return enqueue(batch, null);
    }

    /**
     * 公平认领一个可发送批次：pending 按创建时间，retry 仅选择到期项；两类均有数据时交替选择。
     */
    public synchronized Optional<OutboxClaim> claimNext(Instant now) throws IOException {
        Objects.requireNonNull(now, "now 不能为空");
        List<OutboxStoredBatch> pending = pendingBatchesInternal().stream()
                .filter(item -> item.delivery().state() == OutboxDelivery.State.PENDING)
                .filter(this::childIsEligible)
                .filter(item -> !claimed.contains(item.descriptor().batchId()))
                .toList();
        List<OutboxStoredBatch> retry = retryBatchesInternal().stream()
                .filter(item -> item.delivery().state() == OutboxDelivery.State.RETRY)
                .filter(item -> !item.delivery().nextRetryAt().isAfter(now))
                .filter(this::childIsEligible)
                .filter(item -> !claimed.contains(item.descriptor().batchId()))
                .toList();

        OutboxStoredBatch selected = null;
        if (!pending.isEmpty() && !retry.isEmpty()) {
            selected = preferRetry ? retry.get(0) : pending.get(0);
            preferRetry = !preferRetry;
        } else if (!pending.isEmpty()) {
            selected = pending.get(0);
        } else if (!retry.isEmpty()) {
            selected = retry.get(0);
        }
        if (selected == null) return Optional.empty();
        claimed.add(selected.descriptor().batchId());
        return Optional.of(new OutboxClaim(this, selected));
    }

    /** 上传失败后更新最小重试元数据，并将批次原子移动到 retry。 */
    public synchronized void scheduleRetry(OutboxClaim claim, OutboxDelivery.ErrorCategory category,
                                           Instant nextRetryAt) throws IOException {
        claim.requireOwner(this);
        OutboxStoredBatch current = reloadClaim(claim);
        OutboxDelivery updated = current.delivery().retry(category, nextRetryAt, clock.instant());
        OutboxFiles.writeAtomic(current.directory().resolve(OutboxFiles.DELIVERY_FILE), OutboxFiles.writeDelivery(updated));
        Path target = retryRoot.resolve(current.directory().getFileName());
        if (!current.directory().getParent().equals(retryRoot)) {
            if (Files.exists(target)) throw new IOException("retry 中已存在同名批次，拒绝覆盖: " + target);
            OutboxFiles.moveAtomic(current.directory(), target, false);
        }
        finishClaim(claim);
        signalAvailabilityChange();
    }

    /** 不可自动重试的批次留在 pending，并通过有限错误分类标记为 BLOCKED。 */
    public synchronized void markBlocked(OutboxClaim claim, OutboxDelivery.ErrorCategory category) throws IOException {
        claim.requireOwner(this);
        OutboxStoredBatch current = reloadClaim(claim);
        OutboxDelivery updated = current.delivery().blocked(category, clock.instant());
        OutboxFiles.writeAtomic(current.directory().resolve(OutboxFiles.DELIVERY_FILE), OutboxFiles.writeDelivery(updated));
        Path target = pendingRoot.resolve(current.directory().getFileName());
        if (!current.directory().getParent().equals(pendingRoot)) {
            if (Files.exists(target)) throw new IOException("pending 中已存在同名批次，拒绝覆盖: " + target);
            OutboxFiles.moveAtomic(current.directory(), target, false);
        }
        finishClaim(claim);
        signalAvailabilityChange();
    }

    /**
     * 将 413 父批替换为两个已经可靠发布到 pending 的确定性子批。
     * 子批全部 enqueue 成功后才改变父批状态；重复执行会复用相同子批并幂等恢复父状态。
     */
    public synchronized void markSplit(OutboxClaim parentClaim, List<BatchEnvelope> children) throws IOException {
        parentClaim.requireOwner(this);
        OutboxStoredBatch parent = reloadClaim(parentClaim);
        validateChildren(parent.batch(), children);
        List<String> childIds = children.stream().map(BatchEnvelope::batchId).toList();

        for (BatchEnvelope child : children) enqueue(child, parent.batch().batchId());

        OutboxDelivery updated = parent.delivery().state() == OutboxDelivery.State.SPLIT
                ? parent.delivery()
                : parent.delivery().split(childIds, clock.instant());
        if (!updated.childBatchIds().equals(childIds)) {
            throw new IOException("父批已经保存了不同的 413 子批关系: " + parent.batch().batchId());
        }
        OutboxFiles.writeAtomic(parent.directory().resolve(OutboxFiles.DELIVERY_FILE),
                OutboxFiles.writeDelivery(updated));
        Path target = pendingRoot.resolve(parent.directory().getFileName());
        if (!parent.directory().getParent().equals(pendingRoot)) {
            if (Files.exists(target)) throw new IOException("pending 中已存在同名父批，拒绝覆盖: " + target);
            OutboxFiles.moveAtomic(parent.directory(), target, false);
        }
        finishClaim(parentClaim);
        signalAvailabilityChange();
    }

    /** 在父批 delivery.json 中幂等记录一个子批的可靠提交证据。 */
    public synchronized OutboxStoredBatch acknowledgeSplitChild(String parentBatchId, String childBatchId)
            throws IOException {
        OutboxStoredBatch parent = findByBatchId(parentBatchId)
                .orElseThrow(() -> new IOException("找不到 413 父批: " + parentBatchId));
        OutboxDelivery updated = parent.delivery().childCommitted(childBatchId, clock.instant());
        if (updated != parent.delivery()) {
            OutboxFiles.writeAtomic(parent.directory().resolve(OutboxFiles.DELIVERY_FILE),
                    OutboxFiles.writeDelivery(updated));
        }
        signalAvailabilityChange();
        return OutboxFiles.load(parent.directory());
    }

    /** 返回所有子批均已可靠提交、等待推进逻辑上传水位的父批。 */
    public synchronized List<OutboxStoredBatch> completedSplitParents() throws IOException {
        return pendingBatchesInternal().stream()
                .filter(item -> item.delivery().splitComplete())
                .toList();
    }

    /** 上传水位已经持久化后，原子移出并删除一个普通批次。 */
    public synchronized void deleteCommitted(OutboxClaim claim) throws IOException {
        claim.requireOwner(this);
        OutboxStoredBatch current = reloadClaim(claim);
        moveToDeletingAndDelete(current.directory());
        finishClaim(claim);
        signalAvailabilityChange();
    }

    /** 逻辑父批水位已经持久化后，幂等删除完成的 SPLIT 父批。 */
    public synchronized void deleteCompletedSplit(String batchId) throws IOException {
        Optional<OutboxStoredBatch> existing = findByBatchId(batchId);
        if (existing.isEmpty()) return;
        OutboxStoredBatch parent = existing.get();
        if (!parent.delivery().splitComplete()) {
            throw new IOException("413 父批的子批尚未全部可靠提交: " + batchId);
        }
        moveToDeletingAndDelete(parent.directory());
        signalAvailabilityChange();
    }

    /** 返回经过完整哈希校验的 pending 快照，包括 BLOCKED 批次。 */
    public synchronized List<OutboxStoredBatch> pendingBatches() throws IOException {
        return List.copyOf(pendingBatchesInternal());
    }

    /** 返回经过完整哈希校验的 retry 快照。 */
    public synchronized List<OutboxStoredBatch> retryBatches() throws IOException {
        return List.copyOf(retryBatchesInternal());
    }

    /** 返回当前最早的重试到期时间；空结果表示没有 retry 批次。 */
    public synchronized Optional<Instant> nextRetryAt() throws IOException {
        return retryBatchesInternal().stream()
                .map(item -> item.delivery().nextRetryAt())
                .min(Comparator.naturalOrder());
    }

    /**
     * 返回 Outbox 内容变化版本。调用方先读取版本、检查队列，再按该版本等待，
     * 可以避免“检查为空”和“进入等待”之间新增 pending 导致的丢失唤醒。
     */
    public long changeVersion() {
        return changeVersion.get();
    }

    /** 等待 Outbox 发生变化或到达 retry 的定时唤醒点；不会扫描目录或忙轮询。 */
    public boolean awaitChange(long observedVersion, Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout 不能为空");
        if (timeout.isNegative() || timeout.isZero()) return changeVersion.get() != observedVersion;
        long nanos;
        try {
            nanos = timeout.toNanos();
        } catch (ArithmeticException ignored) {
            nanos = Long.MAX_VALUE;
        }
        changeLock.lockInterruptibly();
        try {
            while (changeVersion.get() == observedVersion && nanos > 0) {
                nanos = changed.awaitNanos(nanos);
            }
            return changeVersion.get() != observedVersion;
        } finally {
            changeLock.unlock();
        }
    }

    /** 唤醒等待新批次的上传循环，也用于 daemon 关闭和鉴权恢复。 */
    public void signalAvailabilityChange() {
        changeLock.lock();
        try {
            changeVersion.incrementAndGet();
            changed.signalAll();
        } finally {
            changeLock.unlock();
        }
    }

    /** 释放未完成状态迁移的认领，供下一工作线程重新领取。 */
    synchronized void release(OutboxClaim claim) {
        if (!claim.ownedBy(this)) throw new IllegalArgumentException("OutboxClaim 不属于当前 OutboxStore");
        if (claim.finish()) claimed.remove(claim.stored().descriptor().batchId());
    }

    public OutboxRecovery.RecoveryReport recoveryReport() {
        return recoveryReport;
    }

    public Path outboxRoot() {
        return outboxRoot;
    }

    public Path stateRoot() {
        return stateRoot;
    }

    private OutboxStoredBatch reloadClaim(OutboxClaim claim) throws IOException {
        Path path = claim.stored().directory();
        if (!Files.isDirectory(path)) throw new IOException("已认领的 Outbox 批次不存在: " + path);
        OutboxStoredBatch current = OutboxFiles.load(path);
        if (!current.descriptor().serializedSha256().equals(claim.stored().descriptor().serializedSha256())) {
            throw new IOException("已认领批次的不可变内容发生变化: " + current.descriptor().batchId());
        }
        return current;
    }

    private void finishClaim(OutboxClaim claim) {
        if (!claim.finish()) throw new IllegalStateException("OutboxClaim 已经完成或释放");
        claimed.remove(claim.stored().descriptor().batchId());
    }

    private List<OutboxStoredBatch> pendingBatchesInternal() throws IOException {
        List<OutboxStoredBatch> result = loadDirectories(pendingRoot);
        result.sort(Comparator
                .comparing((OutboxStoredBatch item) -> item.descriptor().createdAt())
                .thenComparing(item -> item.descriptor().batchId()));
        return result;
    }

    private List<OutboxStoredBatch> retryBatchesInternal() throws IOException {
        List<OutboxStoredBatch> result = loadDirectories(retryRoot);
        result.sort(Comparator
                .comparing((OutboxStoredBatch item) -> item.delivery().nextRetryAt())
                .thenComparing(item -> item.descriptor().createdAt())
                .thenComparing(item -> item.descriptor().batchId()));
        return result;
    }

    private List<OutboxStoredBatch> loadDirectories(Path root) throws IOException {
        List<OutboxStoredBatch> result = new ArrayList<>();
        try (var stream = Files.list(root)) {
            for (Path directory : stream.filter(Files::isDirectory).toList()) result.add(OutboxFiles.load(directory));
        }
        return result;
    }

    private Optional<OutboxStoredBatch> findExisting(String directoryName) throws IOException {
        Path pending = pendingRoot.resolve(directoryName);
        if (Files.isDirectory(pending)) return Optional.of(OutboxFiles.load(pending));
        Path retry = retryRoot.resolve(directoryName);
        if (Files.isDirectory(retry)) return Optional.of(OutboxFiles.load(retry));
        return Optional.empty();
    }

    private Optional<OutboxStoredBatch> findByBatchId(String batchId) throws IOException {
        return findExisting(OutboxFiles.directoryKey(batchId));
    }

    /** 子批在父批尚未原子切换到 SPLIT 时不可发送，避免崩溃中间态越过父状态。 */
    private boolean childIsEligible(OutboxStoredBatch item) {
        String parentId = item.descriptor().parentBatchId();
        if (parentId == null) return true;
        try {
            Optional<OutboxStoredBatch> parent = findByBatchId(parentId);
            return parent.isPresent()
                    && parent.get().delivery().state() == OutboxDelivery.State.SPLIT
                    && parent.get().delivery().childBatchIds().contains(item.batch().batchId());
        } catch (IOException e) {
            throw new IllegalStateException("无法校验 413 父子状态", e);
        }
    }

    private void moveToDeletingAndDelete(Path source) throws IOException {
        Path deleting = writingRoot.resolve(source.getFileName() + ".deleting");
        if (Files.exists(deleting)) throw new IOException("存在同名待删除批次，拒绝覆盖: " + deleting);
        OutboxFiles.moveAtomic(source, deleting, false);
        OutboxFiles.deleteTransientDirectory(writingRoot, deleting, ".deleting");
    }

    private void validateChildren(BatchEnvelope parent, List<BatchEnvelope> children) {
        if (children == null || children.size() != 2) {
            throw new IllegalArgumentException("413 确定性二分必须产生两个子批");
        }
        List<String> combinedEvents = children.stream().flatMap(child -> child.events().stream())
                .map(event -> event.eventId() + ":" + event.payloadSha256()).toList();
        List<String> parentEvents = parent.events().stream()
                .map(event -> event.eventId() + ":" + event.payloadSha256()).toList();
        if (!combinedEvents.equals(parentEvents)) throw new IllegalArgumentException("413 子批事件不是父批的有序完整分区");
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < children.size(); i++) {
            BatchEnvelope child = children.get(i);
            if (!ids.add(child.batchId())) throw new IllegalArgumentException("413 子批 batch_id 不能重复");
            if (!parent.pollId().equals(child.pollId())
                    || parent.batchSequence() != child.batchSequence()
                    || !parent.collectorId().equals(child.collectorId())
                    || !parent.sourceType().equals(child.sourceType())
                    || !parent.sourceKey().equals(child.sourceKey())
                    || !parent.windowStart().equals(child.windowStart())
                    || !parent.windowEnd().equals(child.windowEnd())
                    || child.lastBatch() != (i == children.size() - 1 && parent.lastBatch())) {
                throw new IllegalArgumentException("413 子批身份、窗口、序号或尾批标志与父批不一致");
            }
        }
    }

    private void validateBatchHashes(BatchEnvelope batch) {
        String expected = IngestJson.eventsSha256(batch.events());
        if (!expected.equalsIgnoreCase(batch.contentSha256())) {
            throw new IllegalArgumentException("content_sha256 与 events 内容不一致: " + batch.batchId());
        }
        batch.events().forEach(event -> {
            String payloadHash = IngestJson.sha256Canonical(event.payload());
            if (!payloadHash.equalsIgnoreCase(event.payloadSha256())) {
                throw new IllegalArgumentException("payload_sha256 与 payload 不一致: " + event.eventId());
            }
        });
    }

    private static Path resolveRoot(CollectorConfig config) {
        Objects.requireNonNull(config, "config 不能为空");
        if (config.storage == null) throw new IllegalArgumentException("storage 配置不能为空");
        return Path.of(config.effectiveDataRoot());
    }

    private static Path resolveInside(Path root, String relative, String name) {
        if (relative == null || relative.isBlank()) throw new IllegalArgumentException(name + " 不能为空");
        Path value = Path.of(relative);
        if (value.isAbsolute()) throw new IllegalArgumentException(name + " 必须是相对路径");
        Path resolved = root.resolve(value).normalize();
        if (!resolved.startsWith(root) || resolved.equals(root)) throw new IllegalArgumentException(name + " 不能越过 dataRoot");
        return resolved;
    }

    private static String normalizeParent(String parentBatchId) {
        return parentBatchId == null || parentBatchId.isBlank() ? null : parentBatchId;
    }
}

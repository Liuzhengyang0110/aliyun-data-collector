package cn.kjky.collector.runtime;

import cn.kjky.collector.config.CollectorConfig;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * 独立保存 cc-ingest 可靠 ACK 的上传水位。
 * <p>
 * Store 不判断 HTTP 响应，也不删除 Outbox 批次。上层只有确认响应为
 * COMMITTED 或 ALREADY_COMMITTED 后，才允许调用 {@link #acknowledge(UploadAcknowledgement)}。
 * 每次修改先强制写入同目录临时文件，再原子替换正式文件；任一步失败均不修改内存状态。
 * </p>
 */
public final class UploadWatermarkStore {
    public static final String SCHEMA_VERSION = "1.0";
    public static final String FILE_NAME = "upload-watermarks.json";

    private final Path file;
    private final Path partial;
    private final Clock clock;
    private final AtomicMover mover;
    private final ObjectMapper json = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
    private final Map<String, PollDocument> polls = new LinkedHashMap<>();

    /** 按 storage.dataRoot/stateDir 使用标准文件名创建 Store。 */
    public UploadWatermarkStore(CollectorConfig config) throws IOException {
        this(resolveFile(config));
    }

    /** 直接指定状态文件，主要用于独立组件和测试。 */
    public UploadWatermarkStore(Path file) throws IOException {
        this(file, Clock.systemUTC());
    }

    UploadWatermarkStore(Path file, Clock clock) throws IOException {
        this(file, clock, UploadWatermarkStore::moveAtomically);
    }

    UploadWatermarkStore(Path file, Clock clock, AtomicMover mover) throws IOException {
        this.file = Objects.requireNonNull(file, "file 不能为空").toAbsolutePath().normalize();
        this.partial = this.file.resolveSibling(this.file.getFileName() + ".partial");
        this.clock = Objects.requireNonNull(clock, "clock 不能为空");
        this.mover = Objects.requireNonNull(mover, "mover 不能为空");
        Path parent = this.file.getParent();
        if (parent == null) throw new IllegalArgumentException("上传水位文件必须有父目录");
        Files.createDirectories(parent);
        if (Files.exists(partial)) {
            throw new IOException("发现无法判断的上传水位临时文件，已失败关闭: " + partial);
        }
        if (Files.exists(this.file)) load();
    }

    /** 返回指定 poll_id 的不可变水位快照。 */
    public synchronized Optional<PollWatermark> get(String pollId) {
        PollDocument poll = polls.get(requireText(pollId, "poll_id"));
        return poll == null ? Optional.empty() : Optional.of(snapshotOf(poll));
    }

    /** 返回所有 poll_id 的不可变快照，供后续上传器、健康检查和测试读取。 */
    public synchronized Map<String, PollWatermark> snapshot() {
        Map<String, PollWatermark> result = new LinkedHashMap<>();
        polls.forEach((key, value) -> result.put(key, snapshotOf(value)));
        return Collections.unmodifiableMap(result);
    }

    /**
     * 记录一个已经可靠提交的 ACK。
     * <p>
     * 相同 ACK 重复调用不会写文件；乱序 ACK 只进入 sparse_acks，连续缺口补齐后自动折叠。
     * 本方法只更新水位，不操作 Outbox 文件。
     * </p>
     */
    public synchronized PollWatermark acknowledge(UploadAcknowledgement acknowledgement) throws IOException {
        UploadAcknowledgement ack = Objects.requireNonNull(acknowledgement, "acknowledgement 不能为空");
        String pollId = requireText(ack.pollId(), "poll_id");
        Map<String, PollDocument> candidate = deepCopy(polls);
        PollDocument poll = candidate.computeIfAbsent(pollId, key -> newPoll(key));

        var inferred = ack.inferredLastBatchSequence();
        Integer inferredLast = inferred.isPresent() ? inferred.getAsInt() : null;
        validateKnownLast(poll, ack.batchSequence(), inferredLast);

        if (poll.completed) {
            if (ack.batchSequence() > poll.lastBatchSequence) {
                throw new IllegalArgumentException("completed poll 不接受超出最后批次的 ACK: " + pollId);
            }
            return snapshotOf(polls.get(pollId));
        }

        boolean changed = false;
        if (inferredLast != null && poll.lastBatchSequence == null) {
            if (poll.sparseAcks.stream().anyMatch(sequence -> sequence > inferredLast)) {
                throw new IllegalArgumentException("最后批次序号小于已经记录的 sparse ACK: " + pollId);
            }
            poll.lastBatchSequence = inferredLast;
            changed = true;
        }

        AckEvidence evidence = AckEvidence.from(ack);
        AckEvidence existing = poll.ackEvidence.get(ack.batchSequence());
        if (existing != null && !existing.equals(evidence)) {
            throw new IllegalArgumentException("同一 poll_id 和 batch_sequence 收到不同 ACK: "
                    + pollId + "/" + ack.batchSequence());
        }

        if (ack.batchSequence() <= poll.continuousSequence) {
            if (existing == null) throw new IllegalStateException("连续 ACK 缺少批次证据: " + pollId);
            if (!changed) return snapshotOf(polls.get(pollId));
        } else if (existing != null) {
            if (!changed) return snapshotOf(polls.get(pollId));
        } else {
            poll.sparseAcks.add(ack.batchSequence());
            poll.sparseAcks.sort(Integer::compareTo);
            poll.ackEvidence.put(ack.batchSequence(), evidence);
            changed = true;
        }

        foldContinuous(poll);
        if (poll.lastBatchSequence != null && poll.continuousSequence == poll.lastBatchSequence) {
            poll.completed = true;
            // completed 只保留连续边界，不保留任何逐批 ACK 历史。
            poll.sparseAcks.clear();
            poll.ackEvidence.clear();
        }
        poll.updatedAt = clock.instant();

        validatePoll(pollId, poll);
        save(candidate);
        polls.clear();
        polls.putAll(candidate);
        return snapshotOf(polls.get(pollId));
    }

    public Path file() {
        return file;
    }

    private void load() throws IOException {
        if (!Files.isRegularFile(file)) throw new IOException("上传水位路径不是普通文件: " + file);
        try {
            StateFile state = json.readValue(Files.readAllBytes(file), StateFile.class);
            if (state == null || !SCHEMA_VERSION.equals(state.schemaVersion)) {
                throw new IOException("不支持或缺失的上传水位 Schema: "
                        + (state == null ? null : state.schemaVersion));
            }
            if (state.polls == null) throw new IOException("上传水位 polls 不能为空");
            Map<String, PollDocument> loaded = new LinkedHashMap<>();
            for (var entry : state.polls.entrySet()) {
                String pollId = requireText(entry.getKey(), "polls key");
                PollDocument poll = Objects.requireNonNull(entry.getValue(), "poll 状态不能为空: " + pollId);
                validatePoll(pollId, poll);
                loaded.put(pollId, copyOf(poll));
            }
            polls.putAll(loaded);
        } catch (Exception e) {
            polls.clear();
            throw new IOException("上传水位文件损坏或不兼容，已失败关闭: " + file, e);
        }
    }

    private void save(Map<String, PollDocument> candidate) throws IOException {
        StateFile state = new StateFile();
        state.schemaVersion = SCHEMA_VERSION;
        state.polls.putAll(candidate);
        byte[] bytes = json.writeValueAsBytes(state);

        if (Files.exists(partial)) {
            throw new IOException("上传水位临时文件已存在，拒绝覆盖: " + partial);
        }
        try {
            try (FileChannel channel = FileChannel.open(partial,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }
            mover.move(partial, file);
        } catch (IOException e) {
            try {
                Files.deleteIfExists(partial);
            } catch (IOException cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            throw e;
        }
    }

    private static void moveAtomically(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            throw new IOException("上传水位所在文件系统不支持同文件系统原子替换: " + target, e);
        }
    }

    private static void validateKnownLast(PollDocument poll, int sequence, Integer inferredLast) {
        if (poll.lastBatchSequence != null && sequence > poll.lastBatchSequence) {
            throw new IllegalArgumentException("batch_sequence 超出已知最后批次序号");
        }
        if (inferredLast != null && poll.lastBatchSequence != null
                && !inferredLast.equals(poll.lastBatchSequence)) {
            throw new IllegalArgumentException("同一 poll_id 的最后批次序号不一致");
        }
        if (inferredLast != null && inferredLast < poll.continuousSequence) {
            throw new IllegalArgumentException("最后批次序号不能小于 continuous_sequence");
        }
    }

    private static void foldContinuous(PollDocument poll) {
        int next = poll.continuousSequence + 1;
        while (poll.ackEvidence.containsKey(next)) {
            poll.sparseAcks.remove(Integer.valueOf(next));
            poll.continuousSequence = next;
            next++;
        }
    }

    private static PollDocument newPoll(String pollId) {
        PollDocument poll = new PollDocument();
        poll.schemaVersion = SCHEMA_VERSION;
        poll.pollId = pollId;
        poll.continuousSequence = -1;
        poll.completed = false;
        return poll;
    }

    private static void validatePoll(String mapKey, PollDocument poll) throws IOException {
        if (!SCHEMA_VERSION.equals(poll.schemaVersion)) {
            throw new IOException("poll 使用了不兼容的 schema_version: " + mapKey);
        }
        String pollId;
        try {
            pollId = requireText(poll.pollId, "poll_id");
        } catch (IllegalArgumentException e) {
            throw new IOException(e.getMessage(), e);
        }
        if (!mapKey.equals(pollId)) throw new IOException("polls key 与 poll_id 不一致: " + mapKey);
        if (poll.continuousSequence == null || poll.continuousSequence < -1) {
            throw new IOException("continuous_sequence 必须大于等于 -1: " + mapKey);
        }
        if (poll.sparseAcks == null || poll.ackEvidence == null) {
            throw new IOException("sparse ACK 状态不能为空: " + mapKey);
        }
        if (poll.completed == null) throw new IOException("completed 不能为空: " + mapKey);
        if (poll.updatedAt == null) throw new IOException("updated_at 不能为空: " + mapKey);
        if (poll.lastBatchSequence != null && poll.lastBatchSequence < 0) {
            throw new IOException("last_batch_sequence 不能小于 0: " + mapKey);
        }

        List<Integer> sorted = new ArrayList<>(new TreeSet<>(poll.sparseAcks));
        if (!sorted.equals(poll.sparseAcks)) throw new IOException("sparse_acks 必须严格升序且不能重复: " + mapKey);
        Set<Integer> sparseSet = Set.copyOf(poll.sparseAcks);
        for (Integer sequence : poll.sparseAcks) {
            if (sequence == null || sequence <= poll.continuousSequence) {
                throw new IOException("sparse ACK 必须位于 continuous_sequence 之后: " + mapKey);
            }
            if (poll.lastBatchSequence != null && sequence > poll.lastBatchSequence) {
                throw new IOException("sparse ACK 超出最后批次序号: " + mapKey);
            }
        }

        long expectedEvidence = (long) poll.continuousSequence + 1L + poll.sparseAcks.size();
        if (poll.completed) expectedEvidence = 0;
        if (poll.ackEvidence.size() != expectedEvidence) {
            throw new IOException("ack_evidence 与连续/稀疏 ACK 范围不一致: " + mapKey);
        }
        for (var entry : poll.ackEvidence.entrySet()) {
            Integer sequence = entry.getKey();
            if (sequence == null || sequence < 0
                    || (sequence > poll.continuousSequence && !sparseSet.contains(sequence))) {
                throw new IOException("ack_evidence 包含未确认序号: " + mapKey);
            }
            AckEvidence evidence = entry.getValue();
            if (evidence == null) throw new IOException("缺少 ACK 证据: " + mapKey + "/" + sequence);
            evidence.validate(sequence, poll.lastBatchSequence);
        }

        if (poll.lastBatchSequence == null) {
            if (poll.completed) throw new IOException("未知最后批次时不能 completed: " + mapKey);
        } else {
            if (poll.continuousSequence > poll.lastBatchSequence) {
                throw new IOException("continuous_sequence 超出最后批次: " + mapKey);
            }
            boolean shouldComplete = poll.continuousSequence == poll.lastBatchSequence;
            if (poll.completed != shouldComplete) throw new IOException("completed 与连续水位不一致: " + mapKey);
        }
        if (poll.completed && (!poll.sparseAcks.isEmpty() || !poll.ackEvidence.isEmpty())) {
            throw new IOException("completed poll 不得保留逐批 ACK: " + mapKey);
        }
    }

    private static PollWatermark snapshotOf(PollDocument poll) {
        return new PollWatermark(poll.schemaVersion, poll.pollId, poll.continuousSequence,
                List.copyOf(poll.sparseAcks), poll.lastBatchSequence, poll.completed, poll.updatedAt);
    }

    private static Map<String, PollDocument> deepCopy(Map<String, PollDocument> source) {
        Map<String, PollDocument> result = new LinkedHashMap<>();
        source.forEach((key, value) -> result.put(key, copyOf(value)));
        return result;
    }

    private static PollDocument copyOf(PollDocument source) {
        PollDocument target = new PollDocument();
        target.schemaVersion = source.schemaVersion;
        target.pollId = source.pollId;
        target.continuousSequence = source.continuousSequence;
        target.sparseAcks.addAll(source.sparseAcks);
        target.ackEvidence.putAll(source.ackEvidence);
        target.lastBatchSequence = source.lastBatchSequence;
        target.completed = source.completed;
        target.updatedAt = source.updatedAt;
        return target;
    }

    private static Path resolveFile(CollectorConfig config) {
        Objects.requireNonNull(config, "config 不能为空");
        if (config.storage == null) throw new IllegalArgumentException("storage 配置不能为空");
        Path root = Path.of(config.effectiveDataRoot()).toAbsolutePath().normalize();
        Path state = root.resolve(config.effectiveStateDirectory()).normalize();
        if (!state.startsWith(root) || state.equals(root)) {
            throw new IllegalArgumentException("stateDir 不能越过或等于 dataRoot");
        }
        return state.resolve(FILE_NAME);
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " 不能为空");
        return value;
    }

    /** P0-08 在确认可靠回执后可直接读取的单 poll 快照。 */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record PollWatermark(
            String schemaVersion,
            String pollId,
            int continuousSequence,
            List<Integer> sparseAcks,
            Integer lastBatchSequence,
            boolean completed,
            Instant updatedAt) {

        public PollWatermark {
            sparseAcks = List.copyOf(sparseAcks);
        }
    }

    @FunctionalInterface
    interface AtomicMover {
        void move(Path source, Path target) throws IOException;
    }

    /** 仅为尚未折叠的稀疏 ACK 保存最小证据；连续或 completed 后立即清除。 */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = false)
    public record AckEvidence(String batchId, String contentSha256,
                              @JsonProperty("is_last_batch") boolean lastBatch,
                              Integer batchCount) {
        static AckEvidence from(UploadAcknowledgement ack) {
            return new AckEvidence(ack.batchId(), ack.contentSha256(), ack.lastBatch(), ack.batchCount());
        }

        void validate(int sequence, Integer knownLast) throws IOException {
            if (batchId == null || batchId.isBlank()) throw new IOException("sparse ACK batch_id 不能为空");
            if (contentSha256 == null || !contentSha256.matches("[0-9a-f]{64}")) {
                throw new IOException("sparse ACK content_sha256 无效");
            }
            if (batchCount != null) {
                if (batchCount < 1 || sequence >= batchCount) throw new IOException("sparse ACK batch_count 无效");
                if (lastBatch && sequence != batchCount - 1) throw new IOException("sparse ACK 末批信息不一致");
                if (knownLast != null && knownLast != batchCount - 1) throw new IOException("sparse ACK batch_count 与最后批次不一致");
            }
            if (lastBatch && (knownLast == null || knownLast != sequence)) {
                throw new IOException("sparse ACK is_last_batch 与最后批次不一致");
            }
        }
    }

    /** JSON 中每个 poll_id 的状态。包装类型用于识别缺失字段并失败关闭。 */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = false)
    public static final class PollDocument {
        public String schemaVersion;
        public String pollId;
        public Integer continuousSequence;
        public List<Integer> sparseAcks = new ArrayList<>();
        public Map<Integer, AckEvidence> ackEvidence = new LinkedHashMap<>();
        public Integer lastBatchSequence;
        public Boolean completed;
        public Instant updatedAt;
    }

    /** upload-watermarks.json 的版本化根结构。 */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = false)
    public static final class StateFile {
        public String schemaVersion;
        public Map<String, PollDocument> polls = new LinkedHashMap<>();
    }
}

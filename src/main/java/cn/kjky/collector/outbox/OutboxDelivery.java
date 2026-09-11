package cn.kjky.collector.outbox;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/** Outbox 批次的可变投递元数据；与不可变 batch.json/outbox.json 分开保存。 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public record OutboxDelivery(
        String schemaVersion,
        String batchId,
        State state,
        int attemptCount,
        @JsonProperty("last_error") @JsonAlias("last_error_category")
        ErrorCategory lastErrorCategory,
        Instant nextRetryAt,
        Instant createdAt,
        Instant updatedAt,
        List<String> childBatchIds,
        List<String> committedChildBatchIds) {

    public static final String SCHEMA_VERSION = "1.0";

    public enum State {
        PENDING,
        RETRY,
        BLOCKED,
        /** 413 父批已由确定性子批替代，父批本身不再直接发送。 */
        SPLIT
    }

    /** 只保存有限分类，禁止把中控响应正文、凭据或任意异常原文写入 PVC。 */
    public enum ErrorCategory {
        NETWORK,
        TIMEOUT,
        RATE_LIMITED,
        SERVER_ERROR,
        AUTHENTICATION,
        VALIDATION,
        CONFLICT,
        PAYLOAD_TOO_LARGE,
        UNKNOWN
    }

    public OutboxDelivery {
        if (!SCHEMA_VERSION.equals(schemaVersion)) throw new IllegalArgumentException("不支持的 delivery schema_version");
        if (batchId == null || batchId.isBlank()) throw new IllegalArgumentException("batch_id 不能为空");
        Objects.requireNonNull(state, "state 不能为空");
        if (attemptCount < 0) throw new IllegalArgumentException("attempt_count 不能小于 0");
        Objects.requireNonNull(createdAt, "created_at 不能为空");
        Objects.requireNonNull(updatedAt, "updated_at 不能为空");
        childBatchIds = childBatchIds == null ? List.of() : List.copyOf(childBatchIds);
        committedChildBatchIds = committedChildBatchIds == null ? List.of() : List.copyOf(committedChildBatchIds);
        if (state == State.RETRY && (lastErrorCategory == null || nextRetryAt == null)) {
            throw new IllegalArgumentException("RETRY 必须包含错误分类和 next_retry_at");
        }
        if (state == State.BLOCKED && lastErrorCategory == null) {
            throw new IllegalArgumentException("BLOCKED 必须包含错误分类");
        }
        if (state != State.RETRY && nextRetryAt != null) {
            throw new IllegalArgumentException("只有 RETRY 可以包含 next_retry_at");
        }
        if (state == State.SPLIT) {
            if (lastErrorCategory != ErrorCategory.PAYLOAD_TOO_LARGE) {
                throw new IllegalArgumentException("SPLIT 必须记录 PAYLOAD_TOO_LARGE");
            }
            if (childBatchIds.size() < 2 || new LinkedHashSet<>(childBatchIds).size() != childBatchIds.size()) {
                throw new IllegalArgumentException("SPLIT 必须包含至少两个不重复的 child_batch_ids");
            }
            if (new LinkedHashSet<>(committedChildBatchIds).size() != committedChildBatchIds.size()
                    || !childBatchIds.containsAll(committedChildBatchIds)) {
                throw new IllegalArgumentException("committed_child_batch_ids 必须是 child_batch_ids 的不重复子集");
            }
        } else if (!childBatchIds.isEmpty() || !committedChildBatchIds.isEmpty()) {
            throw new IllegalArgumentException("只有 SPLIT 可以包含父子批次状态");
        }
    }

    public static OutboxDelivery pending(String batchId, Instant now) {
        return new OutboxDelivery(SCHEMA_VERSION, batchId, State.PENDING, 0,
                null, null, now, now, List.of(), List.of());
    }

    public OutboxDelivery retry(ErrorCategory category, Instant nextRetry, Instant now) {
        return new OutboxDelivery(SCHEMA_VERSION, batchId, State.RETRY, attemptCount + 1,
                Objects.requireNonNull(category, "错误分类不能为空"),
                Objects.requireNonNull(nextRetry, "nextRetry 不能为空"), createdAt, now,
                List.of(), List.of());
    }

    public OutboxDelivery blocked(ErrorCategory category, Instant now) {
        return new OutboxDelivery(SCHEMA_VERSION, batchId, State.BLOCKED, attemptCount + 1,
                Objects.requireNonNull(category, "错误分类不能为空"), null, createdAt, now,
                List.of(), List.of());
    }

    /** 将收到 413 的父批次冻结为不再直接发送的拆分状态。 */
    public OutboxDelivery split(List<String> children, Instant now) {
        return new OutboxDelivery(SCHEMA_VERSION, batchId, State.SPLIT, attemptCount + 1,
                ErrorCategory.PAYLOAD_TOO_LARGE, null, createdAt, now,
                List.copyOf(children), List.of());
    }

    /** 幂等记录一个子批已经得到可靠提交回执。 */
    public OutboxDelivery childCommitted(String childBatchId, Instant now) {
        if (state != State.SPLIT) throw new IllegalStateException("只有 SPLIT 父批可以确认子批");
        if (!childBatchIds.contains(childBatchId)) throw new IllegalArgumentException("子批不属于当前父批");
        if (committedChildBatchIds.contains(childBatchId)) return this;
        List<String> committed = new java.util.ArrayList<>(committedChildBatchIds);
        committed.add(childBatchId);
        return new OutboxDelivery(SCHEMA_VERSION, batchId, State.SPLIT, attemptCount,
                lastErrorCategory, null, createdAt, now, childBatchIds, committed);
    }

    public boolean splitComplete() {
        return state == State.SPLIT && committedChildBatchIds.size() == childBatchIds.size();
    }
}

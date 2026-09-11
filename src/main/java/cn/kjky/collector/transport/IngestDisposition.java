package cn.kjky.collector.transport;

/** 采集端根据 HTTP 状态和结构化回执得到的后续处理结论。 */
public enum IngestDisposition {
    /** 已可靠提交，可以更新上传水位后删除本地批次。 */
    COMMITTED,
    /** 暂时性失败，应保持同一 batch_id 稍后重试。 */
    RETRYABLE,
    /** 请求或 Schema 错误，批次应留在 pending 并标记 BLOCKED。 */
    BLOCKED,
    /** 认证或授权失败，应暂停上传器并告警。 */
    AUTHENTICATION_FAILED,
    /** 同一 batch_id 对应不同内容，必须停止并人工处理。 */
    CONFLICT,
    /** 请求超过中控限制，按已冻结的拆批策略处理。 */
    PAYLOAD_TOO_LARGE,
    /** 2xx 或回执无法证明可靠提交，不得删除批次。 */
    UNKNOWN_RESPONSE
}

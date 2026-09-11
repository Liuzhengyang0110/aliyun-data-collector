package cn.kjky.collector.transport;

import cn.kjky.collector.contract.IngestResponse;

/** 一次 cc-ingest 调用的脱敏结果，不保存原始响应正文。 */
public record IngestResult(
        int httpStatus,
        IngestDisposition disposition,
        IngestResponse response,
        Long retryAfterSeconds) {

    /** 只有可靠提交或明确的已提交重复批次才返回 true。 */
    public boolean committed() {
        return disposition == IngestDisposition.COMMITTED;
    }
}

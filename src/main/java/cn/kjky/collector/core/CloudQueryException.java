package cn.kjky.collector.core;

import java.io.IOException;

/** 只保存状态、稳定错误码和 Retry-After 的脱敏 HTTP 云查询异常。 */
public final class CloudQueryException extends IOException {
    private final QueryErrorCategory category;
    private final int httpStatus;
    private final String safeCode;
    private final String retryAfter;

    public CloudQueryException(QueryErrorCategory category, int httpStatus, String safeCode, String retryAfter) {
        super("云查询失败: " + safeCode);
        this.category = category;
        this.httpStatus = httpStatus;
        this.safeCode = safeCode;
        this.retryAfter = retryAfter;
    }

    public QueryErrorCategory category() { return category; }
    public int httpStatus() { return httpStatus; }
    public String safeCode() { return safeCode; }
    public String retryAfter() { return retryAfter; }

    /** 将 HTTP 状态转为统一分类；调用方不得传入响应正文。 */
    public static CloudQueryException forHttpStatus(int status, String retryAfter) {
        QueryErrorCategory category;
        if (status == 401 || status == 403) category = QueryErrorCategory.AUTHENTICATION;
        else if (status == 429) category = QueryErrorCategory.RATE_LIMITED;
        else if (status == 408 || status == 500 || status == 502 || status == 503 || status == 504) {
            category = QueryErrorCategory.RETRYABLE;
        } else category = QueryErrorCategory.NON_RETRYABLE;
        return new CloudQueryException(category, status, "HTTP_" + status, retryAfter);
    }
}

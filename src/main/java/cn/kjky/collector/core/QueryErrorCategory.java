package cn.kjky.collector.core;

/** 云查询失败的稳定分类；不得与上传、控制或状态上报的重试状态混用。 */
public enum QueryErrorCategory {
    RETRYABLE,
    NON_RETRYABLE,
    AUTHENTICATION,
    RATE_LIMITED
}

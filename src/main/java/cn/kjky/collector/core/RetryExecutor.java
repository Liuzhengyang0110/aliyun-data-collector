package cn.kjky.collector.core;

import java.util.concurrent.Callable;

/**
 * API 调用重试器。
 * 只重试网络、超时、限流和服务端错误，权限或参数错误会立即返回给调用者。
 */
public final class RetryExecutor {
    /** 首次调用失败后最多重试的次数。 */
    private final int maxRetries;
    /** 指数退避的基础等待毫秒数。 */
    private final long baseMillis;

    /**
     * 创建重试器。
     *
     * @param maxRetries 最大重试次数，不包含第一次调用
     * @param baseMillis 第一次重试前的基础等待时间
     */
    public RetryExecutor(int maxRetries, long baseMillis) {
        this.maxRetries = maxRetries;
        this.baseMillis = baseMillis;
    }

    /**
     * 执行可能失败的操作，并对可恢复异常进行指数退避重试。
     *
     * @param operation 操作名称，仅用于最终错误信息，不包含敏感数据
     * @param call 实际 API 调用或 I/O 操作
     * @param <T> 调用返回值类型
     * @return 成功调用的返回值
     * @throws Exception 不可重试，或达到最大重试次数后的最后一个异常
     */
    public <T> T execute(String operation, Callable<T> call) throws Exception {
        // last 保存最近一次异常，理论上仅用于循环后的防御性异常分支。
        Exception last = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                return call.call();
            } catch (Exception e) {
                last = e;
                if (!isRetryable(e) || attempt == maxRetries) throw e;
                // attempt 控制 1、2、4... 倍退避；单次等待上限为 30 秒。
                Thread.sleep(Math.min(baseMillis * (1L << Math.min(attempt, 8)), 30_000L));
            }
        }
        throw new IllegalStateException(operation + " 重试失败", last);
    }

    /**
     * 判断异常是否适合重试。
     *
     * @param e SDK、HTTP 或 I/O 调用抛出的异常
     * @return true 表示异常可能是暂时性的；false 表示应立即失败
     */
    private boolean isRetryable(Exception e) {
        if (e instanceof com.aliyun.openservices.log.exception.LogException le) {
            // status 是 SLS 返回的 HTTP 状态码；0 通常表示尚未获得 HTTP 响应。
            int status = le.GetHttpCode();
            return status == 0 || status == 408 || status == 429 || status >= 500;
        }
        if (e instanceof com.aliyuncs.exceptions.ClientException ce) {
            // code 是阿里云核心 SDK 的错误码，通过关键词识别暂时性错误。
            String code = ce.getErrCode();
            if (code == null) return true;
            String lower = code.toLowerCase();
            return lower.contains("timeout") || lower.contains("throttl") || lower.contains("server") || lower.contains("network");
        }
        return e instanceof java.io.IOException || e instanceof java.net.http.HttpTimeoutException;
    }
}

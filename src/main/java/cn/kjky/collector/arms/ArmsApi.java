package cn.kjky.collector.arms;

import java.util.Map;

/**
 * ARMS 访问抽象。
 * RPC 和 Dataset 两种实现使用同一接口，采集编排层不需要了解签名或 HTTP 细节。
 */
public interface ArmsApi extends AutoCloseable {
    /**
     * 调用一个 ARMS 只读动作。
     *
     * @param action API 动作名称，例如 ListTraceApps 或 QueryMetric
     * @param parameters 动作的查询/表单参数，不包含公共签名参数
     * @return 原始响应正文、HTTP 状态码和 Request ID
     * @throws Exception SDK、网络、鉴权或服务端处理失败
     */
    ApiResponse call(String action, Map<String, String> parameters) throws Exception;

    /** 默认无需释放资源；持有连接池的实现会覆盖该方法。 */
    @Override default void close() throws Exception {}

    /**
     * ARMS API 的最小通用响应。
     *
     * @param body 原始响应正文
     * @param httpStatus HTTP 状态码
     * @param requestId 服务端请求 ID，响应未提供时为 null
     */
    record ApiResponse(String body, int httpStatus, String requestId) {}
}

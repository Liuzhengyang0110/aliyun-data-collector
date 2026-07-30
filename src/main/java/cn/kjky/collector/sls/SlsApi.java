package cn.kjky.collector.sls;

import java.util.List;

/**
 * SLS 只读访问抽象。
 * SDK 实现和测试替身共享该接口，使分页编排逻辑可以离线测试。
 */
public interface SlsApi extends AutoCloseable {
    /**
     * 分页列出 Project 下的 Logstore。
     *
     * @param project SLS Project 名称
     * @param offset 起始偏移量
     * @param size 本页最大数量
     * @return Logstore 名称、服务端总数和 Request ID
     * @throws Exception SDK、鉴权、网络或服务端错误
     */
    ListResult listLogstores(String project, int offset, int size) throws Exception;

    /**
     * 查询一个时间范围内的一页日志。
     *
     * @param project SLS Project 名称
     * @param logstore Logstore 名称
     * @param from 开始 Epoch 秒（包含）
     * @param to 结束 Epoch 秒（包含）
     * @param topic Topic 筛选，空字符串表示不限
     * @param query SLS 查询表达式
     * @param offset 分页偏移量
     * @param line 本页最大日志数
     * @param reverse 是否倒序
     * @return 查询状态、日志、原始结果和请求元数据
     * @throws Exception SDK、鉴权、查询或网络错误
     */
    QueryResult getLogs(String project, String logstore, int from, int to, String topic,
                        String query, long offset, long line, boolean reverse) throws Exception;

    /** 默认无需释放资源；SDK 实现会关闭连接池。 */
    @Override default void close() throws Exception {}

    /**
     * Logstore 列表页。
     *
     * @param logstores 本页 Logstore 名称
     * @param total 服务端报告的总数
     * @param requestId 请求 ID
     */
    record ListResult(List<String> logstores, int total, String requestId) {}

    /**
     * 一页 SLS 查询结果。
     *
     * @param completed 服务端是否完成本次查询
     * @param logs 已解码日志列表
     * @param rawQueryResult SDK 提供的原始聚合查询结果
     * @param requestId 请求 ID
     * @param scanBytes 扫描字节数
     * @param elapsedMillis 服务端查询耗时毫秒数
     */
    record QueryResult(boolean completed, List<LogRecord> logs, String rawQueryResult, String requestId,
                       long scanBytes, long elapsedMillis) {}

    /**
     * 一条 SLS 日志。
     *
     * @param source 日志来源地址
     * @param time Epoch 秒
     * @param timeNsPart 纳秒补充部分
     * @param contents 有序键值列表，可保留重复键
     */
    record LogRecord(String source, int time, int timeNsPart, List<Content> contents) {}

    /**
     * 日志中的一个键值。
     *
     * @param key 字段名
     * @param value 字段值
     */
    record Content(String key, String value) {}
}

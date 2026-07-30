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

    /** Logstore 列表页；普通不可变类用于兼容 Java 8。 */
    final class ListResult {
        private final List<String> logstores;
        private final int total;
        private final String requestId;

        public ListResult(List<String> logstores, int total, String requestId) {
            this.logstores = logstores;
            this.total = total;
            this.requestId = requestId;
        }

        public List<String> logstores() { return logstores; }
        public int total() { return total; }
        public String requestId() { return requestId; }
        public List<String> getLogstores() { return logstores; }
        public int getTotal() { return total; }
        public String getRequestId() { return requestId; }
    }

    /** 一页 SLS 查询结果。 */
    final class QueryResult {
        private final boolean completed;
        private final List<LogRecord> logs;
        private final String rawQueryResult;
        private final String requestId;
        private final long scanBytes;
        private final long elapsedMillis;

        public QueryResult(boolean completed, List<LogRecord> logs, String rawQueryResult, String requestId,
                           long scanBytes, long elapsedMillis) {
            this.completed = completed;
            this.logs = logs;
            this.rawQueryResult = rawQueryResult;
            this.requestId = requestId;
            this.scanBytes = scanBytes;
            this.elapsedMillis = elapsedMillis;
        }

        public boolean completed() { return completed; }
        public List<LogRecord> logs() { return logs; }
        public String rawQueryResult() { return rawQueryResult; }
        public String requestId() { return requestId; }
        public long scanBytes() { return scanBytes; }
        public long elapsedMillis() { return elapsedMillis; }
        public boolean isCompleted() { return completed; }
        public List<LogRecord> getLogs() { return logs; }
        public String getRawQueryResult() { return rawQueryResult; }
        public String getRequestId() { return requestId; }
        public long getScanBytes() { return scanBytes; }
        public long getElapsedMillis() { return elapsedMillis; }
    }

    /** 一条 SLS 日志。 */
    final class LogRecord {
        private final String source;
        private final int time;
        private final int timeNsPart;
        private final List<Content> contents;

        public LogRecord(String source, int time, int timeNsPart, List<Content> contents) {
            this.source = source;
            this.time = time;
            this.timeNsPart = timeNsPart;
            this.contents = contents;
        }

        public String getSource() { return source; }
        public int getTime() { return time; }
        public int getTimeNsPart() { return timeNsPart; }
        public List<Content> getContents() { return contents; }
    }

    /** 日志中的一个键值。 */
    final class Content {
        private final String key;
        private final String value;

        public Content(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() { return key; }
        public String getValue() { return value; }
    }
}

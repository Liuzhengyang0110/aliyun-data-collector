package cn.kjky.collector.sls;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.config.SecretResolver;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogContent;
import com.aliyun.openservices.log.common.QueriedLog;
import com.aliyun.openservices.log.request.GetLogsRequest;
import com.aliyun.openservices.log.response.GetLogsResponse;
import com.aliyun.openservices.log.response.ListLogStoresResponse;

import java.util.ArrayList;
import java.util.List;

/** 基于阿里云 SLS Java SDK 的只读适配器。 */
public final class SlsSdkApi implements SlsApi {
    /** SLS SDK 客户端，内部管理 HTTP 连接。 */
    private final Client client;

    /**
     * 创建 SLS 客户端并装载 AccessKey/可选 STS Token。
     *
     * @param config SLS Endpoint 和凭据环境变量名
     * @param secrets 环境变量凭据读取器
     */
    public SlsSdkApi(CollectorConfig.SlsConfig config, SecretResolver secrets) {
        this.client = new Client(config.endpoint, secrets.required(config.accessKeyIdEnv), secrets.required(config.accessKeySecretEnv));
        // token 为空表示使用长期 AccessKey；非空时启用 STS 临时凭据。
        String token = secrets.optional(config.stsTokenEnv);
        if (token != null) client.setSecurityToken(token);
    }

    /**
     * 调用 SDK 的 ListLogStores。
     *
     * @param project Project 名称
     * @param offset 列表偏移量
     * @param size 本页最大数量
     * @return 与 SDK 响应解耦的列表结果
     * @throws Exception SLS SDK 调用失败
     */
    @Override
    public ListResult listLogstores(String project, int offset, int size) throws Exception {
        // response 是 SDK 原生返回对象，随后转换为内部不可变记录。
        ListLogStoresResponse response = client.ListLogStores(project, offset, size, "");
        return new ListResult(List.copyOf(response.GetLogStores()), response.GetTotal(), response.GetRequestId());
    }

    /**
     * 调用 SDK 的 GetLogs，并保留日志字段原始顺序和重复键。
     *
     * @param project Project 名称
     * @param logstore Logstore 名称
     * @param from 开始 Epoch 秒
     * @param to 结束 Epoch 秒
     * @param topic Topic 筛选
     * @param query 查询表达式
     * @param offset 分页偏移量
     * @param line 每页数量
     * @param reverse 是否倒序
     * @return SDK 无关的查询结果
     * @throws Exception SLS SDK 调用失败
     */
    @Override
    public QueryResult getLogs(String project, String logstore, int from, int to, String topic,
                               String query, long offset, long line, boolean reverse) throws Exception {
        // request 完整描述一个只读日志查询页。
        GetLogsRequest request = new GetLogsRequest(project, logstore, from, to, topic, query, offset, line, reverse);
        GetLogsResponse response = client.GetLogs(request);
        // records 是最终要序列化的内部日志列表。
        List<LogRecord> records = new ArrayList<>();
        for (QueriedLog queried : response.GetLogs()) {
            // contents 使用列表而不是 Map，避免丢失字段顺序和重复字段名。
            List<Content> contents = new ArrayList<>();
            for (LogContent content : queried.GetLogItem().GetLogContents()) {
                contents.add(new Content(content.GetKey(), content.GetValue()));
            }
            records.add(new LogRecord(queried.GetSource(), queried.GetLogItem().GetTime(),
                    queried.GetLogItem().GetTimeNsPart(), contents));
        }
        return new QueryResult(response.IsCompleted(), records, response.getRawQueryResult(), response.GetRequestId(),
                response.GetScanBytes(), response.getElapsedMilliSecond());
    }

    /** 关闭 SLS SDK HTTP 连接池。 */
    @Override public void close() { client.shutdown(); }
}

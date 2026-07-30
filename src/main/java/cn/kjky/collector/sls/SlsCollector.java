package cn.kjky.collector.sls;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.core.RetryExecutor;
import cn.kjky.collector.core.TimeWindow;
import cn.kjky.collector.output.CheckpointStore;
import cn.kjky.collector.output.OutputSession;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * SLS 采集编排器。
 * 负责 Project/Logstore 循环、列表分页、日志 offset 分页、完成状态轮询、
 * 输出信封组装和断点记录。
 */
public final class SlsCollector {
    /** 完整配置，主要读取 sls 和 runtime 节点。 */
    private final CollectorConfig config;
    /** SLS SDK 适配器或测试替身。 */
    private final SlsApi api;
    /** 暂时性错误重试器。 */
    private final RetryExecutor retry;
    /** 输出会话；仅 discover 场景可为 null。 */
    private final OutputSession output;
    /** 断点存储；仅 discover 场景可为 null。 */
    private final CheckpointStore checkpoint;

    /**
     * 创建 SLS 采集器。
     *
     * @param config 完整采集配置
     * @param api SLS 只读 API
     * @param retry 重试执行器
     * @param output 输出会话；仅 discover 时可为 null
     * @param checkpoint 断点存储；仅 discover 时可为 null
     */
    public SlsCollector(CollectorConfig config, SlsApi api, RetryExecutor retry, OutputSession output, CheckpointStore checkpoint) {
        this.config = config;
        this.api = api;
        this.retry = retry;
        this.output = output;
        this.checkpoint = checkpoint;
    }

    /**
     * 分页列出配置中每个 Project 的全部 Logstore。
     *
     * @return 以 Project 名称为键的发现结果
     * @throws Exception API、鉴权或网络失败
     */
    public Map<String, SlsApi.ListResult> discover() throws Exception {
        // result 按 YAML 中 Project 顺序保存，便于现场阅读。
        Map<String, SlsApi.ListResult> result = new LinkedHashMap<>();
        for (var project : config.sls.projects) {
            // offset 是下一页起始位置；all 累积当前 Project 的所有名称。
            int offset = 0;
            java.util.List<String> all = new java.util.ArrayList<>();
            // requestId 保存最后一页请求 ID，用于探测结果定位。
            String requestId = null;
            int total;
            do {
                // current 创建不可变副本，供 lambda 调用使用。
                int current = offset;
                // page 是一次 ListLogStores 返回的列表页。
                SlsApi.ListResult page = retry.execute("SLS ListLogStores", () -> api.listLogstores(project.project, current, 100));
                all.addAll(page.logstores());
                requestId = page.requestId();
                total = page.total();
                offset += page.logstores().size();
                if (page.logstores().isEmpty()) break;
            } while (offset < total);
            result.put(project.project, new SlsApi.ListResult(all, all.size(), requestId));
        }
        return result;
    }

    /**
     * 采集一个逻辑时间窗口内的所有 Project/Logstore。
     *
     * @param window 当前左闭右开时间窗口
     * @throws Exception 时间转换或任一 Logstore 查询失败
     */
    public void collect(TimeWindow window) throws Exception {
        // fromLong、toLong 先使用 long 校验范围，再转换为旧版 SDK 要求的 int。
        long fromLong = window.start().getEpochSecond();
        // SLS v1 使用秒级闭区间；逻辑结束时间减 1 秒可避免相邻窗口重复查询边界秒。
        long toLong = window.end().getEpochSecond() - 1;
        if (fromLong < Integer.MIN_VALUE || fromLong > Integer.MAX_VALUE || toLong > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("SLS Java SDK v1 的秒级时间超出 int 范围");
        }
        // from、to 是实际传给 SLS SDK 的 Epoch 秒闭区间。
        int from = (int) fromLong;
        int to = (int) toLong;
        for (var project : config.sls.projects) {
            for (var logstore : project.logstores) collectLogstore(project.project, logstore, window, from, to);
        }
    }

    /**
     * 按 offset 分页采集一个 Logstore，并把结果包装为带查询证据的 JSON。
     *
     * @param project Project 名称
     * @param logstore 当前 Logstore 任务配置
     * @param window 逻辑窗口，用于文件命名和 checkpoint
     * @param from SLS 查询开始 Epoch 秒（包含）
     * @param to SLS 查询结束 Epoch 秒（包含）
     * @throws Exception 查询、序列化、落盘或 checkpoint 更新失败
     */
    private void collectLogstore(String project, CollectorConfig.SlsLogstore logstore, TimeWindow window,
                                 int from, int to) throws Exception {
        // doneKey 表示整个 Logstore 窗口已完成。
        String doneKey = "sls|" + project + "|" + logstore.logstore + "|" + window.start() + "|DONE";
        if (checkpoint.contains(doneKey)) return;
        // offset 是下一页在查询结果中的起始位置。
        long offset = 0;
        while (true) {
            // key 精确标识当前 offset 页，恢复时用于跳过成功页。
            String key = "sls|" + project + "|" + logstore.logstore + "|" + window.start() + "|" + offset;
            if (checkpoint.contains(key)) { offset += logstore.pageSize; continue; }
            // result 是轮询到 completed=true 后的一页日志。
            SlsApi.QueryResult result = poll(project, logstore, from, to, offset);
            // envelope 同时保存查询条件、服务端元数据和日志正文，便于追溯。
            Map<String, Object> envelope = new LinkedHashMap<>();
            envelope.put("source", "sls");
            envelope.put("project", project);
            envelope.put("logstore", logstore.logstore);
            envelope.put("fromEpochSecond", from);
            envelope.put("toEpochSecond", to);
            envelope.put("topic", logstore.topic);
            envelope.put("query", logstore.query);
            envelope.put("offset", offset);
            envelope.put("line", logstore.pageSize);
            envelope.put("reverse", logstore.reverse);
            envelope.put("completed", result.completed());
            envelope.put("requestId", result.requestId());
            envelope.put("scanBytes", result.scanBytes());
            envelope.put("elapsedMillis", result.elapsedMillis());
            envelope.put("rawQueryResult", result.rawQueryResult());
            envelope.put("logs", result.logs());
            // body 是最终写入 raw/sls 的 UTF-8 JSON 字节。
            byte[] body = output.json().writeValueAsBytes(envelope);
            output.write("sls", logstore.recordType, window, body, result.logs().size(), result.requestId());
            // 先完成文件和 manifest，再标记 checkpoint，避免断点指向不存在的文件。
            checkpoint.mark(key);
            // count 用于判断是否还有下一页；不足 pageSize 即为最后一页。
            int count = result.logs().size();
            if (count < logstore.pageSize || count == 0) {
                checkpoint.mark(doneKey);
                break;
            }
            offset += count;
        }
    }

    /**
     * 重复查询同一页，直到 SLS 返回 completed=true。
     * 每一次实际 API 调用内部仍会使用 RetryExecutor 处理暂时性异常。
     *
     * @param project Project 名称
     * @param logstore Logstore 查询配置
     * @param from 开始 Epoch 秒
     * @param to 结束 Epoch 秒
     * @param offset 当前分页偏移量
     * @return 已完成的一页查询结果
     * @throws Exception API 失败或超过最大轮询次数
     */
    private SlsApi.QueryResult poll(String project, CollectorConfig.SlsLogstore logstore,
                                    int from, int to, long offset) throws Exception {
        // last 保存最近一次未完成响应，循环结束时用于表达查询状态。
        SlsApi.QueryResult last = null;
        for (int attempt = 1; attempt <= config.runtime.maxPollAttempts; attempt++) {
            // currentOffset 是 lambda 使用的有效 final 副本。
            long currentOffset = offset;
            last = retry.execute("SLS GetLogs", () -> api.getLogs(project, logstore.logstore, from, to,
                    logstore.topic, logstore.query, currentOffset, logstore.pageSize, logstore.reverse));
            if (last.completed()) return last;
            Thread.sleep(config.runtime.pollIntervalMillis);
        }
        throw new IllegalStateException("SLS 查询在最大轮询次数内未完成: " + project + "/" + logstore.logstore);
    }
}

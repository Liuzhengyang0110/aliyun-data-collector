package cn.kjky.collector.arms;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.core.RetryExecutor;
import cn.kjky.collector.core.TimeWindow;
import cn.kjky.collector.output.CheckpointStore;
import cn.kjky.collector.output.OutputSession;
import com.fasterxml.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ARMS 采集编排器。
 * <p>
 * 根据配置选择 RPC 或 Dataset 模式，负责时间窗内的任务循环、分页、
 * Trace 详情查询、断点判断和原始响应落盘。
 * </p>
 */
public final class ArmsCollector {
    /** 代码层允许调用的只读动作白名单，防止配置透传出写操作。 */
    private static final List<String> ALLOWED_ACTIONS = Collections.unmodifiableList(Arrays.asList(
            "ListTraceApps", "SearchTracesByPage", "GetTrace",
            "QueryMetric", "QueryMetricByPage", "QueryDataset"));
    /** 全局配置，主要使用 arms 和 runtime 节点。 */
    private final CollectorConfig config;
    /** RPC 或 Dataset API 实现。 */
    private final ArmsApi api;
    /** 对暂时性 API 错误执行退避重试。 */
    private final RetryExecutor retry;
    /** 负责 raw 文件、manifest 和运行日志。discover 场景可以为 null。 */
    private final OutputSession output;
    /** 负责判断和记录请求完成状态。discover 场景可以为 null。 */
    private final CheckpointStore checkpoint;

    /**
     * 创建 ARMS 采集器。
     *
     * @param config 完整采集配置
     * @param api ARMS RPC 或 Dataset 客户端
     * @param retry 重试执行器
     * @param output 输出会话；仅调用 discover 时可为 null
     * @param checkpoint 断点存储；仅调用 discover 时可为 null
     */
    public ArmsCollector(CollectorConfig config, ArmsApi api, RetryExecutor retry, OutputSession output, CheckpointStore checkpoint) {
        this.config = config;
        this.api = api;
        this.retry = retry;
        this.output = output;
        this.checkpoint = checkpoint;
    }

    /**
     * 查询当前凭据可见的 ARMS 应用。
     *
     * @return ListTraceApps 的原始响应
     * @throws Exception API、鉴权或网络失败
     */
    public ArmsApi.ApiResponse discover() throws Exception {
        ensureAllowed("ListTraceApps");
        return retry.execute("ARMS ListTraceApps",
                () -> api.call("ListTraceApps", Collections.<String, String>emptyMap()));
    }

    /**
     * 采集一个时间窗口内的全部 ARMS 任务。
     *
     * @param window 当前逻辑窗口；开始包含、结束不包含
     * @throws Exception 任一任务失败时抛出，由上层记录错误并停止本次命令
     */
    public void collect(TimeWindow window) throws Exception {
        if ("DATASET".equalsIgnoreCase(config.arms.mode)) collectDatasets(window);
        else {
            for (CollectorConfig.TraceQuery query : config.arms.traceQueries) collectTraces(query, window);
            for (CollectorConfig.MetricQuery query : config.arms.metricQueries) collectMetric(query, window);
        }
    }

    /**
     * 分页搜索 Trace，保存每页原始响应，并可继续采集每条 Trace 详情。
     *
     * @param q 当前链路任务，包含 PID、筛选项和 recordType
     * @param w 当前时间窗口
     * @throws Exception 查询、解析、落盘或 checkpoint 更新失败
     */
    private void collectTraces(CollectorConfig.TraceQuery q, TimeWindow w) throws Exception {
        // doneKey 表示整个“任务 + 时间窗”完成，存在时无需再逐页查找。
        String doneKey = "arms|trace|" + q.name + "|" + w.start() + "|DONE";
        if (checkpoint.contains(doneKey)) return;
        // page 是 ARMS 从 1 开始的页码。
        int page = 1;
        while (true) {
            // key 精确标识当前页；恢复时可以跳过已经成功的页面。
            String key = "arms|trace|" + q.name + "|" + w.start() + "|" + page;
            if (checkpoint.contains(key)) { page++; continue; }
            // p 是本页 SearchTracesByPage 的 RPC 参数。
            Map<String, String> p = new LinkedHashMap<>();
            p.put("StartTime", Long.toString(w.start().toEpochMilli()));
            // ARMS 使用毫秒闭区间，逻辑结束时间减 1 毫秒可避免相邻窗口重复。
            p.put("EndTime", Long.toString(w.end().toEpochMilli() - 1));
            p.put("PageNumber", Integer.toString(page));
            p.put("PageSize", Integer.toString(config.arms.pageSize));
            put(p, "Pid", q.pid); put(p, "ServiceName", q.serviceName); put(p, "OperationName", q.operationName);
            put(p, "TraceId", q.traceId);
            if (q.minDuration != null) p.put("MinDuration", q.minDuration.toString());
            if (q.maxDuration != null) p.put("MaxDuration", q.maxDuration.toString());
            p.putAll(q.parameters);
            ensureAllowed("SearchTracesByPage");
            // response 必须先原样落盘，解析只用于分页和后续 Trace 详情查询。
            ArmsApi.ApiResponse response = retry.execute("ARMS SearchTracesByPage", () -> api.call("SearchTracesByPage", p));
            // root 是临时 JSON 树，用于兼容不同版本的响应嵌套层级。
            JsonNode root = output.json().readTree(response.body());
            // traceIds 收集本页所有 TraceID，并去除重复值。
            List<String> traceIds = findTextValues(root, "TraceID", "TraceId", "traceId");
            // count 优先取 TraceInfos 数组大小，找不到时用 TraceID 数量估算。
            long count = findArraySize(root, "TraceInfos");
            if (count < 0) count = traceIds.size();
            output.write("arms", q.recordType, w, response.body().getBytes(StandardCharsets.UTF_8), count, response.requestId());
            if (config.arms.collectTraceDetails) {
                for (String traceId : traceIds) collectTraceDetail(q, w, traceId, page);
            }
            // 只有原始页和所有详情都成功落盘后才标记该页完成。
            checkpoint.mark(key);
            // 不足一整页表示已经到达最后一页。
            if (count < config.arms.pageSize || count == 0) {
                checkpoint.mark(doneKey);
                break;
            }
            page++;
        }
    }

    /**
     * 根据 TraceID 查询并保存完整 Span 列表。
     *
     * @param q 所属链路任务，用于 checkpoint 名称和 recordType
     * @param w 所属时间窗口，用于文件名
     * @param traceId 当前 Trace 唯一标识
     * @param page 该 Trace 来自的搜索页码，用于生成稳定 checkpoint 键
     * @throws Exception 查询、解析、落盘或 checkpoint 更新失败
     */
    private void collectTraceDetail(CollectorConfig.TraceQuery q, TimeWindow w, String traceId, int page) throws Exception {
        // key 将任务、窗口、页码和 TraceID 组合，避免恢复时重复查询详情。
        String key = "arms|detail|" + q.name + "|" + w.start() + "|" + page + "|" + traceId;
        if (checkpoint.contains(key)) return;
        ensureAllowed("GetTrace");
        // detail 是 GetTrace 的原始 JSON 响应。
        ArmsApi.ApiResponse detail = retry.execute("ARMS GetTrace",
                () -> api.call("GetTrace", Collections.singletonMap("TraceID", traceId)));
        // spans 仅用于 manifest 的记录数，无法识别时最终记录为 0。
        long spans = findArraySize(output.json().readTree(detail.body()), "Spans");
        output.write("arms", q.recordType, w, detail.body().getBytes(StandardCharsets.UTF_8), Math.max(spans, 0), detail.requestId());
        checkpoint.mark(key);
    }

    /**
     * 查询并保存一个指标任务在当前窗口内的数据。
     *
     * @param q 指标名、Measures、Filters、Action 和 recordType
     * @param w 当前时间窗口
     * @throws Exception 查询、落盘或 checkpoint 更新失败
     */
    private void collectMetric(CollectorConfig.MetricQuery q, TimeWindow w) throws Exception {
        // 每个指标任务在一个时间窗只执行一次。
        String key = "arms|metric|" + q.name + "|" + w.start();
        if (checkpoint.contains(key)) return;
        // p 按 RPC 数组规范生成 Measures.1、Filters.1.Key/Value 等参数。
        Map<String, String> p = new LinkedHashMap<>();
        p.put("StartTime", Long.toString(w.start().toEpochMilli()));
        p.put("EndTime", Long.toString(w.end().toEpochMilli() - 1));
        p.put("Metric", q.metric);
        p.put("IntervalInSec", Integer.toString(q.interval));
        p.put("Limit", Integer.toString(q.limit));
        for (int i = 0; i < q.measures.size(); i++) p.put("Measures." + (i + 1), q.measures.get(i));
        for (int i = 0; i < q.dimensions.size(); i++) p.put("Dimensions." + (i + 1), q.dimensions.get(i));
        for (int i = 0; i < q.customFilters.size(); i++) p.put("CustomFilters." + (i + 1), q.customFilters.get(i));
        // filterIndex 从 1 开始，符合阿里云 RPC 结构化数组编码规则。
        int filterIndex = 1;
        for (Map.Entry<String, String> filter : q.filters.entrySet()) {
            p.put("Filters." + filterIndex + ".Key", filter.getKey());
            p.put("Filters." + filterIndex + ".Value", filter.getValue());
            filterIndex++;
        }
        // parameters 最后合并，允许现场版本覆盖程序生成的同名参数。
        p.putAll(q.parameters);
        ensureAllowed(q.action);
        ArmsApi.ApiResponse response = retry.execute("ARMS " + q.action, () -> api.call(q.action, p));
        output.write("arms", q.recordType, w, response.body().getBytes(StandardCharsets.UTF_8), estimateRecords(response.body()), response.requestId());
        checkpoint.mark(key);
    }

    /**
     * 依次执行当前窗口中的所有 Dataset 任务。
     *
     * @param w 当前时间窗口
     * @throws Exception HTTP 查询、落盘或 checkpoint 更新失败
     */
    private void collectDatasets(TimeWindow w) throws Exception {
        for (CollectorConfig.DatasetQuery q : config.arms.dataset.queries) {
            // key 标识“Dataset 任务 + 窗口”，成功后恢复流程会直接跳过。
            String key = "arms|dataset|" + q.name + "|" + w.start();
            if (checkpoint.contains(key)) continue;
            // p 先复制现场额外参数，再补充程序管理的 Dataset 标准参数。
            Map<String, String> p = new LinkedHashMap<>(q.parameters);
            p.put("datasetId", q.datasetId);
            p.put("minTime", Long.toString(w.start().toEpochMilli()));
            p.put("maxTime", Long.toString(w.end().toEpochMilli() - 1));
            p.put("intervalInSec", Integer.toString(q.intervalInSec));
            p.put("measures", String.join(",", q.measures));
            ArmsApi.ApiResponse response = retry.execute("ARMS QueryDataset", () -> api.call("QueryDataset", p));
            output.write("arms", q.recordType, w, response.body().getBytes(StandardCharsets.UTF_8), estimateRecords(response.body()), response.requestId());
            checkpoint.mark(key);
        }
    }

    /**
     * 尝试从常见响应结构中估算记录数，仅用于 manifest 展示。
     * 解析失败不会影响原始响应保存。
     *
     * @param body ARMS 原始 JSON 响应
     * @return 识别出的数组长度；无法判断时返回 -1
     */
    private long estimateRecords(String body) {
        try {
            // root 是响应 JSON 根节点。
            JsonNode root = output.json().readTree(body);
            if (root.isArray()) return root.size();
            for (String name : Arrays.asList("Data", "data", "Records", "records")) {
                JsonNode n = root.get(name);
                if (n != null && n.isArray()) return n.size();
            }
        } catch (Exception ignored) {}
        return -1;
    }

    /**
     * 递归查找指定字段名对应的所有文本值。
     *
     * @param node 搜索起始 JSON 节点
     * @param names 允许的字段名，匹配时忽略大小写
     * @return 去重后的文本值列表
     */
    private List<String> findTextValues(JsonNode node, String... names) {
        // result 是递归遍历期间的可变结果容器。
        List<String> result = new ArrayList<>();
        walk(node, result, Arrays.asList(names));
        return result.stream().distinct().collect(Collectors.toList());
    }

    /**
     * 深度遍历 JSON，将命中字段的文本值加入结果列表。
     *
     * @param node 当前递归节点
     * @param values 共享的结果列表
     * @param names 目标字段名列表
     */
    private void walk(JsonNode node, List<String> values, List<String> names) {
        if (node == null) return;
        if (node.isObject()) node.fields().forEachRemaining(e -> {
            if (names.stream().anyMatch(n -> n.equalsIgnoreCase(e.getKey())) && e.getValue().isValueNode()) values.add(e.getValue().asText());
            else walk(e.getValue(), values, names);
        });
        else if (node.isArray()) node.forEach(n -> walk(n, values, names));
    }

    /**
     * 递归查找第一个指定名称的数组并返回大小。
     *
     * @param node 搜索起始 JSON 节点
     * @param field 目标数组字段名，忽略大小写
     * @return 数组大小；未找到时返回 -1
     */
    private long findArraySize(JsonNode node, String field) {
        if (node == null) return -1;
        if (node.isObject()) {
            // it 遍历当前对象的所有字段；找不到时继续递归子节点。
            Iterator<Map.Entry<String, JsonNode>> it = node.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                if (field.equalsIgnoreCase(e.getKey()) && e.getValue().isArray()) return e.getValue().size();
                long child = findArraySize(e.getValue(), field);
                if (child >= 0) return child;
            }
        } else if (node.isArray()) for (JsonNode child : node) { long size = findArraySize(child, field); if (size >= 0) return size; }
        return -1;
    }

    /**
     * 仅在值非空时加入请求参数。
     *
     * @param map 目标参数 Map
     * @param name 参数名
     * @param value 参数值
     */
    private void put(Map<String, String> map, String name, String value) {
        if (value != null && !value.trim().isEmpty()) map.put(name, value);
    }

    /**
     * 强制执行 ARMS 只读动作白名单。
     *
     * @param action 即将调用的 API 动作
     * @throws SecurityException 动作不在只读白名单中
     */
    private void ensureAllowed(String action) { if (!ALLOWED_ACTIONS.contains(action)) throw new SecurityException("禁止的 ARMS 动作: " + action); }
}

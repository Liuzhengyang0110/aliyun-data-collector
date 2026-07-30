package cn.kjky.collector.service;

import cn.kjky.collector.arms.ArmsApi;
import cn.kjky.collector.arms.ArmsCollector;
import cn.kjky.collector.arms.ArmsDatasetApi;
import cn.kjky.collector.arms.ArmsRpcApi;
import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.config.SecretResolver;
import cn.kjky.collector.core.RetryExecutor;
import cn.kjky.collector.core.TimeWindow;
import cn.kjky.collector.core.WindowPlanner;
import cn.kjky.collector.output.CheckpointStore;
import cn.kjky.collector.output.OutputSession;
import cn.kjky.collector.sls.SlsApi;
import cn.kjky.collector.sls.SlsCollector;
import cn.kjky.collector.sls.SlsSdkApi;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 顶层业务编排器。
 * 命令行入口通过该类执行资源发现、权限探测、预演和正式采集。
 */
public final class CollectorEngine {
    /** 当前命令使用的完整配置。 */
    private final CollectorConfig config;
    /** ARMS 和 SLS 共用的重试策略。 */
    private final RetryExecutor retry;
    /** 创建 API 客户端时使用的环境变量凭据读取器。 */
    private final SecretResolver secrets = new SecretResolver();
    /** 用于命令行输出格式化 JSON。 */
    private final ObjectMapper json = new ObjectMapper().registerModule(new JavaTimeModule()).enable(SerializationFeature.INDENT_OUTPUT);

    /**
     * 根据运行配置创建编排器。
     *
     * @param config 已校验的采集配置
     */
    public CollectorEngine(CollectorConfig config) {
        this.config = config;
        this.retry = new RetryExecutor(config.runtime.maxRetries, config.runtime.retryBaseMillis);
    }

    /**
     * 发现 ARMS 应用和配置中各 SLS Project 的 Logstore。
     *
     * @return 格式化 JSON 字符串，供命令行打印
     * @throws Exception 任一已启用数据源发现失败
     */
    public String discover() throws Exception {
        // result 同时容纳 ARMS JSON 树和 SLS 结构化列表。
        Map<String, Object> result = new LinkedHashMap<>();
        if (config.arms.enabled) try (ArmsApi api = armsApi()) {
            if ("RPC".equalsIgnoreCase(config.arms.mode)) {
                var response = new ArmsCollector(config, api, retry, null, null).discover();
                result.put("arms", json.readTree(response.body()));
            } else result.put("arms", "DATASET 模式没有应用发现接口，请按现场文档填写 datasetId");
        }
        if (config.sls.enabled) try (SlsApi api = new SlsSdkApi(config.sls, secrets)) {
            result.put("sls", new SlsCollector(config, api, retry, null, null).discover());
        }
        return json.writeValueAsString(result);
    }

    /**
     * 执行最小只读连通性和权限探测。
     * RPC 模式复用 discover；Dataset 模式读取最近 60 秒但不落盘。
     *
     * @return 格式化探测结果 JSON
     * @throws Exception 配置、鉴权、网络或 API 失败
     */
    public String probe() throws Exception {
        if (!config.arms.enabled || "RPC".equalsIgnoreCase(config.arms.mode)) return discover();
        if (config.arms.dataset.queries.isEmpty()) throw new IllegalArgumentException("Dataset 模式至少需要一个查询任务才能探测");
        // q 是第一个 Dataset 查询任务，用于构造最小探测请求。
        var q = config.arms.dataset.queries.get(0);
        // end 是探测时刻；查询范围固定为 end 前 60 秒。
        Instant end = Instant.now();
        // parameters 是最终表单参数，不修改原配置中的 Map。
        Map<String, String> parameters = new LinkedHashMap<>(q.parameters);
        parameters.put("datasetId", q.datasetId);
        parameters.put("minTime", Long.toString(end.minusSeconds(60).toEpochMilli()));
        parameters.put("maxTime", Long.toString(end.toEpochMilli()));
        parameters.put("intervalInSec", Integer.toString(q.intervalInSec));
        parameters.put("measures", String.join(",", q.measures));
        // result 只返回状态和 Request ID，不把探测数据正文打印到终端。
        Map<String, Object> result = new LinkedHashMap<>();
        try (ArmsApi api = armsApi()) {
            ArmsApi.ApiResponse response = retry.execute("ARMS Dataset probe", () -> api.call("QueryDataset", parameters));
            result.put("arms", Map.of("mode", "DATASET", "httpStatus", response.httpStatus(), "requestId", response.requestId() == null ? "" : response.requestId()));
        }
        if (config.sls.enabled) try (SlsApi api = new SlsSdkApi(config.sls, secrets)) {
            result.put("sls", new SlsCollector(config, api, retry, null, null).discover());
        }
        return json.writeValueAsString(result);
    }

    /**
     * 生成不访问网络的采集计划。
     *
     * @param start 整体采集开始时刻
     * @param end 整体采集结束时刻
     * @return 包含分窗数量、任务名和输出目录的格式化 JSON
     * @throws Exception 时间范围或 JSON 序列化失败
     */
    public String dryRun(Instant start, Instant end) throws Exception {
        // windows 是按 runtime.windowSeconds 拆分后的全部逻辑窗口。
        List<TimeWindow> windows = new WindowPlanner().split(start, end, config.runtime.windowSeconds);
        // plan 是展示给现场人员确认的只读计划摘要。
        Map<String, Object> plan = new LinkedHashMap<>();
        plan.put("start", start);
        plan.put("end", end);
        plan.put("windowSeconds", config.runtime.windowSeconds);
        plan.put("windowCount", windows.size());
        plan.put("armsMode", config.arms.enabled ? config.arms.mode : "disabled");
        plan.put("armsTraceTasks", config.arms.traceQueries.stream().map(q -> q.name).toList());
        plan.put("armsMetricTasks", config.arms.metricQueries.stream().map(q -> q.name).toList());
        plan.put("armsDatasetTasks", config.arms.dataset.queries.stream().map(q -> q.name).toList());
        plan.put("slsLogstores", config.sls.projects.stream().flatMap(p -> p.logstores.stream().map(l -> p.project + "/" + l.logstore)).toList());
        plan.put("outputRoot", java.nio.file.Path.of(config.outputRoot).toAbsolutePath().normalize().toString());
        return json.writeValueAsString(plan);
    }

    /**
     * 执行正式采集或断点续采。
     * collect 和 resume 共用该方法，因为每次执行都会自动读取 checkpoint。
     *
     * @param start 整体采集开始时刻
     * @param end 整体采集结束时刻
     * @throws Exception 任一窗口查询、落盘或清单更新失败
     */
    public void collect(Instant start, Instant end) throws Exception {
        // windows 决定执行顺序；当前实现按时间串行，减少现场接口压力。
        List<TimeWindow> windows = new WindowPlanner().split(start, end, config.runtime.windowSeconds);
        // output 会恢复已有 manifest；checkpoint 会恢复已完成请求。
        OutputSession output = new OutputSession(config, start, end);
        CheckpointStore checkpoint = new CheckpointStore(output.root(), output.json());
        output.appendRunLog("INFO", "开始采集，窗口数=" + windows.size());
        try (ArmsApi arms = config.arms.enabled ? armsApi() : null;
             SlsApi sls = config.sls.enabled ? new SlsSdkApi(config.sls, secrets) : null) {
            // 未启用的数据源对应采集器为 null，窗口循环中直接跳过。
            ArmsCollector armsCollector = arms == null ? null : new ArmsCollector(config, arms, retry, output, checkpoint);
            SlsCollector slsCollector = sls == null ? null : new SlsCollector(config, sls, retry, output, checkpoint);
            for (TimeWindow window : windows) {
                try {
                    // 每个窗口固定先 ARMS 后 SLS，便于根据日志定位中断位置。
                    if (armsCollector != null) armsCollector.collect(window);
                    if (slsCollector != null) slsCollector.collect(window);
                    output.appendRunLog("INFO", "完成时间窗 " + window.start() + " - " + window.end());
                } catch (Exception e) {
                    output.error("collect " + window.start() + " - " + window.end(), e);
                    output.appendRunLog("ERROR", e.getClass().getSimpleName() + ": " + e.getMessage());
                    throw e;
                }
            }
            output.saveManifest();
            output.appendRunLog("INFO", "采集完成，文件数=" + output.manifest().files.size());
        }
    }

    /**
     * 根据 arms.mode 创建对应 API 实现。
     *
     * @return DATASET 模式返回 ArmsDatasetApi，否则返回 ArmsRpcApi
     */
    private ArmsApi armsApi() {
        if ("DATASET".equalsIgnoreCase(config.arms.mode)) return new ArmsDatasetApi(config.arms, secrets, config.runtime.requestTimeoutSeconds);
        return new ArmsRpcApi(config.arms, secrets);
    }
}

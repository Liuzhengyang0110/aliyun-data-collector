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
import cn.kjky.collector.outbox.OutboxStore;
import cn.kjky.collector.pipeline.CollectionTask;
import cn.kjky.collector.pipeline.ProductionEventSink;
import cn.kjky.collector.runtime.CollectionWatermarkStore;
import cn.kjky.collector.runtime.CollectionSource;
import cn.kjky.collector.sls.SlsApi;
import cn.kjky.collector.sls.SlsCollector;
import cn.kjky.collector.sls.SlsSdkApi;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

/**
 * 顶层业务编排器。
 * 命令行入口通过该类执行资源发现、权限探测、预演和正式采集。
 */
public final class CollectorEngine {
    /** 当前命令使用的完整配置。 */
    private final CollectorConfig config;
    /** ARMS 查询独立重试器，不与 SLS/上传/控制/状态共享退避状态。 */
    private final RetryExecutor armsRetry;
    /** SLS 查询独立重试器，不与 ARMS/上传/控制/状态共享退避状态。 */
    private final RetryExecutor slsRetry;
    /** 创建 API 客户端时使用的环境变量凭据读取器。 */
    private final SecretResolver secrets = new SecretResolver();
    /** 用于命令行输出格式化 JSON。 */
    private final ObjectMapper json = new ObjectMapper().registerModule(new JavaTimeModule()).enable(SerializationFeature.INDENT_OUTPUT);
    /** 常驻 ARMS/SLS 共享输出状态的初始化锁。 */
    private final Object daemonSessionLock = new Object();
    /** 生产模式的 Outbox、任务级水位和事件管道初始化锁。 */
    private final Object productionPipelineLock = new Object();
    /** 常驻模式共享会话，避免两个数据源各自恢复并覆盖同一 manifest。 */
    private OutputSession daemonOutput;
    /** 常驻模式共享页面级 checkpoint。 */
    private CheckpointStore daemonCheckpoint;
    /** 仅 raw 关闭的生产采集路径使用；延迟创建避免只读命令触碰 PVC。 */
    private ProductionPipeline productionPipeline;
    /** daemon 的采集和上传循环必须共享同一实例，保证进程内批次认领互斥。 */
    private OutboxStore daemonOutbox;
    /** 每个生产查询窗口开始前复核物理容量与逻辑配额；非 daemon 调用默认放行。 */
    private volatile Predicate<CollectionSource> productionCollectionGate = ignored -> true;

    /**
     * 根据运行配置创建编排器。
     *
     * @param config 已校验的采集配置
     */
    public CollectorEngine(CollectorConfig config) {
        this.config = config;
        this.armsRetry = RetryExecutor.forCloud(
                "DATASET".equalsIgnoreCase(config.arms.mode) ? "ARMS_DATASET" : "ARMS_RPC", config.runtime);
        this.slsRetry = RetryExecutor.forCloud("SLS", config.runtime);
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
                var response = new ArmsCollector(config, api, armsRetry, null, null).discover();
                result.put("arms", json.readTree(response.body()));
            } else result.put("arms", "DATASET 模式没有应用发现接口，请按现场文档填写 datasetId");
        }
        if (config.sls.enabled) try (SlsApi api = new SlsSdkApi(config.sls, secrets)) {
            result.put("sls", new SlsCollector(config, api, slsRetry, null, null).discover());
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
            ArmsApi.ApiResponse response = armsRetry.execute("ARMS Dataset probe", () -> api.call("QueryDataset", parameters));
            result.put("arms", Map.of("mode", "DATASET", "httpStatus", response.httpStatus(), "requestId", response.requestId() == null ? "" : response.requestId()));
        }
        if (config.sls.enabled) try (SlsApi api = new SlsSdkApi(config.sls, secrets)) {
            result.put("sls", new SlsCollector(config, api, slsRetry, null, null).discover());
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
        plan.put("outputRoot", java.nio.file.Path.of(config.effectiveDataRoot()).toAbsolutePath().normalize().toString());
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
    public synchronized void collect(Instant start, Instant end) throws Exception {
        if (!config.rawPersistenceEnabled()) {
            collectProductionRange(start, end);
            return;
        }
        OutputSession output = new OutputSession(config, start, end);
        CheckpointStore checkpoint = new CheckpointStore(output.root(), output.json());
        collectSelected(start, end, config.arms.enabled, config.sls.enabled, output, checkpoint);
    }

    /**
     * 只采集指定数据源，供 ARMS/SLS 独立常驻调度器调用。
     * 两个数据源共享线程安全的输出会话和 checkpoint，但 API 查询可以并行执行，
     * 避免较慢的 ARMS 查询长期阻塞 SLS 的轮询周期。
     */
    public void collectSource(CollectionSource source, Instant start, Instant end) throws Exception {
        if (source == CollectionSource.ARMS && !config.arms.enabled) throw new IllegalStateException("ARMS 未启用");
        if (source == CollectionSource.SLS && !config.sls.enabled) throw new IllegalStateException("SLS 未启用");
        if (!config.rawPersistenceEnabled()) {
            collectProductionSourceRange(source, start, end);
            return;
        }
        DaemonSession session = daemonSession(start, end);
        collectSelected(start, end, source == CollectionSource.ARMS, source == CollectionSource.SLS,
                session.output(), session.checkpoint());
    }

    /**
     * 生产 daemon 按任务独立水位执行一个数据源轮询。
     * 任务首次起点由 CollectionWatermarkStore 固定，之后只有 pending 批次全部就绪才会前进。
     */
    public void collectProductionSource(CollectionSource source, Instant queryEnd,
                                        int initialLookbackSeconds) throws Exception {
        if (config.rawPersistenceEnabled()) {
            throw new IllegalStateException("开发 raw 模式不应使用生产任务级轮询");
        }
        if (initialLookbackSeconds < 1) throw new IllegalArgumentException("initialLookbackSeconds 必须大于 0");
        ProductionPipeline pipeline = productionPipeline();
        if (source == CollectionSource.ARMS) collectProductionArmsFromWatermarks(pipeline, queryEnd, initialLookbackSeconds);
        else collectProductionSlsFromWatermarks(pipeline, queryEnd, initialLookbackSeconds);
    }

    /**
     * 返回 daemon 采集与上传共用的 Outbox。上传循环不得自行创建第二个 Store，
     * 否则两个实例各自维护的认领集合无法保证同一进程内互斥。
     */
    public OutboxStore daemonOutbox() throws Exception {
        synchronized (productionPipelineLock) {
            if (daemonOutbox == null) daemonOutbox = new OutboxStore(config);
            return daemonOutbox;
        }
    }

    /** 返回生产采集链路正在使用的任务级水位 Store，供只读状态快照复用。 */
    public CollectionWatermarkStore productionWatermarks() throws Exception {
        return productionPipeline().watermarks();
    }

    /** 由生产 daemon 在启动前绑定双重容量门禁，不包含任何远程控制逻辑。 */
    public void configureProductionCollectionGate(BooleanSupplier gate) {
        Objects.requireNonNull(gate, "gate 不能为空");
        this.productionCollectionGate = ignored -> gate.getAsBoolean();
    }

    /** 绑定支持 ARMS/SLS 分源暂停的组合门禁。 */
    public void configureProductionCollectionGate(Predicate<CollectionSource> gate) {
        this.productionCollectionGate = Objects.requireNonNull(gate, "gate 不能为空");
    }

    /** 执行选择后的数据源集合，只有全部窗口成功才正常返回。 */
    private void collectSelected(Instant start, Instant end, boolean collectArms, boolean collectSls,
                                 OutputSession output, CheckpointStore checkpoint) throws Exception {
        // windows 决定执行顺序；当前实现按时间串行，减少现场接口压力。
        List<TimeWindow> windows = new WindowPlanner().split(start, end, config.runtime.windowSeconds);
        output.appendRunLog("INFO", "开始采集，窗口数=" + windows.size());
        try (ArmsApi arms = collectArms ? armsApi() : null;
             SlsApi sls = collectSls ? new SlsSdkApi(config.sls, secrets) : null) {
            // 未启用的数据源对应采集器为 null，窗口循环中直接跳过。
            ArmsCollector armsCollector = arms == null ? null : new ArmsCollector(config, arms, armsRetry, output, checkpoint);
            SlsCollector slsCollector = sls == null ? null : new SlsCollector(config, sls, slsRetry, output, checkpoint);
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
            output.appendRunLog("INFO", "采集完成，文件数=" + output.fileCount());
        }
    }

    /** 执行手动 production collect/resume 指定的时间范围，不读取旧页面 checkpoint。 */
    private void collectProductionRange(Instant start, Instant end) throws Exception {
        ProductionPipeline pipeline = productionPipeline();
        if (config.arms.enabled) collectProductionArms(pipeline, start, end);
        if (config.sls.enabled) collectProductionSls(pipeline, start, end);
    }

    /** 供兼容 SourceCollector 接口的生产路径使用；start/end 已由调用方决定。 */
    private void collectProductionSourceRange(CollectionSource source, Instant start, Instant end) throws Exception {
        ProductionPipeline pipeline = productionPipeline();
        if (source == CollectionSource.ARMS) collectProductionArms(pipeline, start, end);
        else collectProductionSls(pipeline, start, end);
    }

    /** 在指定范围运行所有 ARMS 任务。 */
    private void collectProductionArms(ProductionPipeline pipeline, Instant start, Instant end) throws Exception {
        try (ArmsApi api = armsApi()) {
            ArmsCollector collector = new ArmsCollector(config, api, armsRetry, null, null, pipeline.sink());
            if ("DATASET".equalsIgnoreCase(config.arms.mode)) {
                for (var query : config.arms.dataset.queries) {
                    runProductionTask(CollectionSource.ARMS, CollectionTask.armsDatasetKey(query.name), start, end,
                            window -> collector.collectDatasetTask(query, window));
                }
            } else {
                for (var query : config.arms.traceQueries) {
                    runProductionTask(CollectionSource.ARMS, CollectionTask.armsTraceKey(query.name), start, end,
                            window -> collector.collectTraceTask(query, window));
                }
                for (var query : config.arms.metricQueries) {
                    runProductionTask(CollectionSource.ARMS, CollectionTask.armsMetricKey(query.name), start, end,
                            window -> collector.collectMetricTask(query, window));
                }
            }
        }
    }

    /** 在指定范围运行所有 SLS Project/Logstore 任务。 */
    private void collectProductionSls(ProductionPipeline pipeline, Instant start, Instant end) throws Exception {
        try (SlsApi api = new SlsSdkApi(config.sls, secrets)) {
            SlsCollector collector = new SlsCollector(config, api, slsRetry, null, null, pipeline.sink());
            for (var project : config.sls.projects) {
                for (var logstore : project.logstores) {
                    runProductionTask(CollectionSource.SLS, CollectionTask.slsLogKey(project.project, logstore.logstore), start, end,
                            window -> collector.collectLogstoreTask(project.project, logstore, window));
                }
            }
        }
    }

    /** 根据各自 CollectionWatermarkStore 中的起点运行 ARMS 任务。 */
    private void collectProductionArmsFromWatermarks(ProductionPipeline pipeline, Instant end,
                                                      int initialLookbackSeconds) throws Exception {
        try (ArmsApi api = armsApi()) {
            ArmsCollector collector = new ArmsCollector(config, api, armsRetry, null, null, pipeline.sink());
            if ("DATASET".equalsIgnoreCase(config.arms.mode)) {
                for (var query : config.arms.dataset.queries) {
                    runProductionTaskFromWatermark(pipeline, CollectionTask.armsDatasetKey(query.name), end,
                            initialLookbackSeconds, window -> collector.collectDatasetTask(query, window));
                }
            } else {
                for (var query : config.arms.traceQueries) {
                    runProductionTaskFromWatermark(pipeline, CollectionTask.armsTraceKey(query.name), end,
                            initialLookbackSeconds, window -> collector.collectTraceTask(query, window));
                }
                for (var query : config.arms.metricQueries) {
                    runProductionTaskFromWatermark(pipeline, CollectionTask.armsMetricKey(query.name), end,
                            initialLookbackSeconds, window -> collector.collectMetricTask(query, window));
                }
            }
        }
    }

    /** 根据各自 CollectionWatermarkStore 中的起点运行 SLS 任务。 */
    private void collectProductionSlsFromWatermarks(ProductionPipeline pipeline, Instant end,
                                                     int initialLookbackSeconds) throws Exception {
        try (SlsApi api = new SlsSdkApi(config.sls, secrets)) {
            SlsCollector collector = new SlsCollector(config, api, slsRetry, null, null, pipeline.sink());
            for (var project : config.sls.projects) {
                for (var logstore : project.logstores) {
                    runProductionTaskFromWatermark(pipeline, CollectionTask.slsLogKey(project.project, logstore.logstore), end,
                            initialLookbackSeconds, window -> collector.collectLogstoreTask(project.project, logstore, window));
                }
            }
        }
    }

    /** 对一个任务执行指定范围内的逻辑窗口。 */
    private void runProductionTask(CollectionSource source, String taskKey, Instant start, Instant end,
                                   ProductionTaskCollector collector) throws Exception {
        if (!start.isBefore(end)) return;
        for (TimeWindow window : new WindowPlanner().split(start, end, config.runtime.windowSeconds)) {
            if (!productionCollectionGate.test(source)) {
                throw new IOException("人工暂停、容量 CRITICAL 或本地配置无效，未开始新的查询窗口");
            }
            collector.collect(window);
        }
    }

    /** 首轮先锚定任务起点，随后按同一任务的最新成功窗口继续。 */
    private void runProductionTaskFromWatermark(ProductionPipeline pipeline, String taskKey, Instant end,
                                                int initialLookbackSeconds,
                                                ProductionTaskCollector collector) throws Exception {
        Instant start = pipeline.watermarks().startFor(taskKey, end.minusSeconds(initialLookbackSeconds));
        CollectionSource source = taskKey.startsWith("ARMS/") ? CollectionSource.ARMS : CollectionSource.SLS;
        runProductionTask(source, taskKey, start, end, collector);
    }

    /** 延迟创建生产 Outbox 和水位；所有写入均位于 dataRoot/outbox 与 dataRoot/state。 */
    private ProductionPipeline productionPipeline() throws Exception {
        synchronized (productionPipelineLock) {
            if (productionPipeline == null) {
                OutboxStore outbox = daemonOutbox();
                CollectionWatermarkStore watermarks = new CollectionWatermarkStore(
                        outbox.stateRoot().resolve("collection-watermarks.json"));
                productionPipeline = new ProductionPipeline(outbox, watermarks,
                        new ProductionEventSink(config, outbox, watermarks));
            }
            return productionPipeline;
        }
    }

    @FunctionalInterface
    private interface ProductionTaskCollector {
        void collect(TimeWindow window) throws Exception;
    }

    /** 已初始化的生产持久化组件；上传端尚未读取这些 pending 批次。 */
    private record ProductionPipeline(OutboxStore outbox, CollectionWatermarkStore watermarks,
                                      ProductionEventSink sink) {}

    /** 创建或复用常驻模式共享输出会话。 */
    private DaemonSession daemonSession(Instant start, Instant end) throws Exception {
        synchronized (daemonSessionLock) {
            if (daemonOutput == null) {
                daemonOutput = new OutputSession(config, start, end);
                daemonCheckpoint = new CheckpointStore(daemonOutput.root(), daemonOutput.json());
            } else {
                daemonOutput.extendRange(start, end);
            }
            return new DaemonSession(daemonOutput, daemonCheckpoint);
        }
    }

    /** 常驻模式下必须成对共享的输出对象。 */
    private record DaemonSession(OutputSession output, CheckpointStore checkpoint) {}

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

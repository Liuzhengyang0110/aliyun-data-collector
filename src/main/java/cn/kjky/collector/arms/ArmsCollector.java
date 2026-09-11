package cn.kjky.collector.arms;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.core.RetryExecutor;
import cn.kjky.collector.core.TimeWindow;
import cn.kjky.collector.normalize.ArmsDatasetNormalizer;
import cn.kjky.collector.normalize.ArmsMetricNormalizer;
import cn.kjky.collector.normalize.ArmsTraceDetailNormalizer;
import cn.kjky.collector.normalize.ArmsTraceSearchNormalizer;
import cn.kjky.collector.normalize.NormalizationContext;
import cn.kjky.collector.output.CheckpointStore;
import cn.kjky.collector.output.OutputSession;
import cn.kjky.collector.pipeline.CollectionEventSink;
import cn.kjky.collector.pipeline.CollectionTask;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * ARMS 查询编排器。
 * <p>开发模式可继续写 raw 和页面 checkpoint；生产模式通过 {@link CollectionEventSink}
 * 直接把标准事件写入 Outbox，且不创建或依赖 raw/manifest/checkpoint。</p>
 */
public final class ArmsCollector {
    private static final List<String> ALLOWED_ACTIONS = List.of(
            "ListTraceApps", "SearchTracesByPage", "GetTrace", "QueryMetric", "QueryMetricByPage", "QueryDataset");

    private final CollectorConfig config;
    private final ArmsApi api;
    private final RetryExecutor retry;
    private final OutputSession output;
    private final CheckpointStore checkpoint;
    private final CollectionEventSink pipeline;
    private final ObjectMapper json = new ObjectMapper();

    /** 保持原有开发/离线调用方式。 */
    public ArmsCollector(CollectorConfig config, ArmsApi api, RetryExecutor retry,
                         OutputSession output, CheckpointStore checkpoint) {
        this(config, api, retry, output, checkpoint, null);
    }

    /**
     * 生产调用传入 pipeline；开发调试时可同时传入 output 作为 raw tee，
     * 但不得同时使用页面 checkpoint，避免标准事件尚未进入 pending 就跳过页面。
     */
    public ArmsCollector(CollectorConfig config, ArmsApi api, RetryExecutor retry,
                         OutputSession output, CheckpointStore checkpoint, CollectionEventSink pipeline) {
        this.config = config;
        this.api = api;
        this.retry = retry;
        this.output = output;
        this.checkpoint = checkpoint;
        this.pipeline = pipeline;
        if (pipeline != null && checkpoint != null) {
            throw new IllegalArgumentException("标准事件管道不能混用页面 CheckpointStore");
        }
    }

    /** 查询当前凭据可见的 ARMS 应用。 */
    public ArmsApi.ApiResponse discover() throws Exception {
        ensureAllowed("ListTraceApps");
        return retry.execute("ARMS ListTraceApps", () -> api.call("ListTraceApps", Map.of()));
    }

    /** 采集一个时间窗口内的全部已配置 ARMS 任务。 */
    public void collect(TimeWindow window) throws Exception {
        if ("DATASET".equalsIgnoreCase(config.arms.mode)) {
            for (var query : config.arms.dataset.queries) collectDatasetTask(query, window);
        } else {
            for (var query : config.arms.traceQueries) collectTraceTask(query, window);
            for (var query : config.arms.metricQueries) collectMetricTask(query, window);
        }
    }

    /** 采集一个 ARMS Trace 任务；Trace 搜索和详情共用同一生产任务水位。 */
    public void collectTraceTask(CollectorConfig.TraceQuery query, TimeWindow window) throws Exception {
        String doneKey = "arms|trace|" + query.name + "|" + window.start() + "|DONE";
        if (done(doneKey)) return;
        String taskKey = CollectionTask.armsTraceKey(query.name);
        CollectionEventSink.TaskSession session = open(taskKey, "ARMS", query.recordType, window);
        int page = 1;
        while (true) {
            String pageKey = "arms|trace|" + query.name + "|" + window.start() + "|" + page;
            if (done(pageKey)) {
                page++;
                continue;
            }
            Map<String, String> parameters = new LinkedHashMap<>();
            parameters.put("StartTime", Long.toString(window.start().toEpochMilli()));
            parameters.put("EndTime", Long.toString(window.end().toEpochMilli() - 1));
            parameters.put("PageNumber", Integer.toString(page));
            parameters.put("PageSize", Integer.toString(config.arms.pageSize));
            put(parameters, "Pid", query.pid);
            put(parameters, "ServiceName", query.serviceName);
            put(parameters, "OperationName", query.operationName);
            put(parameters, "TraceId", query.traceId);
            if (query.minDuration != null) parameters.put("MinDuration", query.minDuration.toString());
            if (query.maxDuration != null) parameters.put("MaxDuration", query.maxDuration.toString());
            parameters.putAll(query.parameters);

            ensureAllowed("SearchTracesByPage");
            ArmsApi.ApiResponse response = retry.execute("ARMS SearchTracesByPage", () -> api.call("SearchTracesByPage", parameters));
            JsonNode root = json.readTree(response.body());
            List<String> traceIds = findTextValues(root, "TraceID", "TraceId", "traceId");
            long count = findArraySize(root, "TraceInfos");
            if (count < 0) count = traceIds.size();
            writeRaw("arms", query.recordType, window, response.body(), count, response.requestId());
            if (session != null) {
                session.acceptAll(new ArmsTraceSearchNormalizer().normalize(root,
                        armsContext("TRACE_SEARCH", taskKey, query.recordType, window, response.requestId(), query.normalization)).events());
            }
            if (config.arms.collectTraceDetails) {
                for (String traceId : traceIds) collectTraceDetail(query, window, traceId, page, taskKey, session);
            }
            mark(pageKey);
            if (count < config.arms.pageSize || count == 0) {
                mark(doneKey);
                break;
            }
            page++;
        }
        finish(session);
    }

    /** 采集一个 ARMS 指标任务。 */
    public void collectMetricTask(CollectorConfig.MetricQuery query, TimeWindow window) throws Exception {
        String checkpointKey = "arms|metric|" + query.name + "|" + window.start();
        if (done(checkpointKey)) return;
        String taskKey = CollectionTask.armsMetricKey(query.name);
        CollectionEventSink.TaskSession session = open(taskKey, "ARMS", query.recordType, window);
        Map<String, String> parameters = new LinkedHashMap<>();
        parameters.put("StartTime", Long.toString(window.start().toEpochMilli()));
        parameters.put("EndTime", Long.toString(window.end().toEpochMilli() - 1));
        parameters.put("Metric", query.metric);
        parameters.put("IntervalInSec", Integer.toString(query.interval));
        parameters.put("Limit", Integer.toString(query.limit));
        for (int i = 0; i < query.measures.size(); i++) parameters.put("Measures." + (i + 1), query.measures.get(i));
        for (int i = 0; i < query.dimensions.size(); i++) parameters.put("Dimensions." + (i + 1), query.dimensions.get(i));
        for (int i = 0; i < query.customFilters.size(); i++) parameters.put("CustomFilters." + (i + 1), query.customFilters.get(i));
        int filterIndex = 1;
        for (var filter : query.filters.entrySet()) {
            parameters.put("Filters." + filterIndex + ".Key", filter.getKey());
            parameters.put("Filters." + filterIndex + ".Value", filter.getValue());
            filterIndex++;
        }
        parameters.putAll(query.parameters);
        ensureAllowed(query.action);
        ArmsApi.ApiResponse response = retry.execute("ARMS " + query.action, () -> api.call(query.action, parameters));
        writeRaw("arms", query.recordType, window, response.body(), estimateRecords(response.body()), response.requestId());
        if (session != null) {
            session.acceptAll(new ArmsMetricNormalizer().normalize(json.readTree(response.body()),
                    armsContext("METRIC", taskKey, query.recordType, window, response.requestId(), query.normalization)).events());
        }
        mark(checkpointKey);
        finish(session);
    }

    /** 采集一个 ARMS Dataset 任务。 */
    public void collectDatasetTask(CollectorConfig.DatasetQuery query, TimeWindow window) throws Exception {
        String checkpointKey = "arms|dataset|" + query.name + "|" + window.start();
        if (done(checkpointKey)) return;
        String taskKey = CollectionTask.armsDatasetKey(query.name);
        CollectionEventSink.TaskSession session = open(taskKey, "ARMS", query.recordType, window);
        Map<String, String> parameters = new LinkedHashMap<>(query.parameters);
        parameters.put("datasetId", query.datasetId);
        parameters.put("minTime", Long.toString(window.start().toEpochMilli()));
        parameters.put("maxTime", Long.toString(window.end().toEpochMilli() - 1));
        parameters.put("intervalInSec", Integer.toString(query.intervalInSec));
        parameters.put("measures", String.join(",", query.measures));
        ensureAllowed("QueryDataset");
        ArmsApi.ApiResponse response = retry.execute("ARMS QueryDataset", () -> api.call("QueryDataset", parameters));
        writeRaw("arms", query.recordType, window, response.body(), estimateRecords(response.body()), response.requestId());
        if (session != null) {
            session.acceptAll(new ArmsDatasetNormalizer().normalize(json.readTree(response.body()),
                    armsContext("DATASET", taskKey, query.recordType, window, response.requestId(), query.normalization)).events());
        }
        mark(checkpointKey);
        finish(session);
    }

    private void collectTraceDetail(CollectorConfig.TraceQuery query, TimeWindow window, String traceId, int page,
                                    String taskKey, CollectionEventSink.TaskSession session) throws Exception {
        String checkpointKey = "arms|detail|" + query.name + "|" + window.start() + "|" + page + "|" + traceId;
        if (done(checkpointKey)) return;
        ensureAllowed("GetTrace");
        ArmsApi.ApiResponse detail = retry.execute("ARMS GetTrace", () -> api.call("GetTrace", Map.of("TraceID", traceId)));
        JsonNode root = json.readTree(detail.body());
        long spans = findArraySize(root, "Spans");
        writeRaw("arms", query.recordType, window, detail.body(), Math.max(spans, 0), detail.requestId());
        if (session != null) {
            session.acceptAll(new ArmsTraceDetailNormalizer().normalize(root,
                    armsContext("TRACE_DETAIL", taskKey, query.recordType, window, detail.requestId(), query.normalization)).events());
        }
        mark(checkpointKey);
    }

    private CollectionEventSink.TaskSession open(String taskKey, String sourceType, String recordType,
                                                  TimeWindow window) throws Exception {
        return pipeline == null ? null : pipeline.open(new CollectionTask(taskKey, sourceType, recordType, window.start(), window.end()));
    }

    private void finish(CollectionEventSink.TaskSession session) throws Exception {
        if (session != null) session.finish();
    }

    private NormalizationContext armsContext(String kind, String taskKey, String recordType, TimeWindow window,
                                             String requestId, CollectorConfig.NormalizationConfig normalization) {
        return NormalizationContext.arms(kind, taskKey, recordType, window.start(), window.end(), requestId,
                normalization == null ? List.of() : normalization.payloadFields,
                normalization == null ? List.of() : normalization.identityFields);
    }

    private void writeRaw(String collectorId, String recordType, TimeWindow window, String body,
                          long count, String requestId) throws Exception {
        if (output != null) output.write(collectorId, recordType, window,
                body.getBytes(StandardCharsets.UTF_8), count, requestId);
    }

    private boolean done(String key) throws Exception {
        return checkpoint != null && checkpoint.contains(key);
    }

    private void mark(String key) throws Exception {
        if (checkpoint != null) checkpoint.mark(key);
    }

    private long estimateRecords(String body) {
        try {
            JsonNode root = json.readTree(body);
            if (root.isArray()) return root.size();
            for (String name : List.of("Data", "data", "Records", "records")) {
                JsonNode node = root.get(name);
                if (node != null && node.isArray()) return node.size();
            }
        } catch (Exception ignored) {
            // 开发 raw 模式下估算失败不应阻断已取得的原始响应保存。
        }
        return -1;
    }

    private List<String> findTextValues(JsonNode node, String... names) {
        List<String> result = new ArrayList<>();
        walk(node, result, List.of(names));
        return result.stream().distinct().toList();
    }

    private void walk(JsonNode node, List<String> values, List<String> names) {
        if (node == null) return;
        if (node.isObject()) node.fields().forEachRemaining(entry -> {
            if (names.stream().anyMatch(name -> name.equalsIgnoreCase(entry.getKey())) && entry.getValue().isValueNode()) {
                values.add(entry.getValue().asText());
            } else walk(entry.getValue(), values, names);
        });
        else if (node.isArray()) node.forEach(child -> walk(child, values, names));
    }

    private long findArraySize(JsonNode node, String field) {
        if (node == null) return -1;
        if (node.isObject()) {
            var iterator = node.fields();
            while (iterator.hasNext()) {
                var entry = iterator.next();
                if (field.equalsIgnoreCase(entry.getKey()) && entry.getValue().isArray()) return entry.getValue().size();
                long child = findArraySize(entry.getValue(), field);
                if (child >= 0) return child;
            }
        } else if (node.isArray()) {
            for (JsonNode child : node) {
                long size = findArraySize(child, field);
                if (size >= 0) return size;
            }
        }
        return -1;
    }

    private void put(Map<String, String> map, String name, String value) {
        if (value != null && !value.isBlank()) map.put(name, value);
    }

    private void ensureAllowed(String action) {
        if (!ALLOWED_ACTIONS.contains(action)) throw new SecurityException("禁止的 ARMS 动作: " + action);
    }
}

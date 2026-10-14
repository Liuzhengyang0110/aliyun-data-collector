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
import cn.kjky.collector.pagination.JsonPage;
import cn.kjky.collector.pagination.PageGuard;
import cn.kjky.collector.pagination.QuerySummary;
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
            "ListTraceApps", "SearchTracesByPage", "GetTrace", "QueryMetric", "QueryDataset");

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
        return call("ListTraceApps", Map.of(), "ARMS/DISCOVER");
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
        query = json.convertValue(query, CollectorConfig.TraceQuery.class);
        final int pageSize = config.arms.pageSize;
        final boolean details = config.arms.collectTraceDetails;
        String doneKey = "arms|trace|" + query.name + "|" + window.start() + "|DONE";
        if (done(doneKey)) return;
        String taskKey = CollectionTask.armsTraceKey(query.name);
        try (QuerySummary summary = new QuerySummary(taskKey, window)) {
            CollectionEventSink.TaskSession session = open(taskKey, "ARMS", query.recordType, window);
            PageGuard guard = new PageGuard(config.pagination);
            int page = 1;
            while (true) {
                String pageKey = "arms|trace|" + query.name + "|" + window.start() + "|" + page;
                // Replay incomplete windows from page one: old checkpoints cannot prove pagination completeness.
                Map<String, String> parameters = new LinkedHashMap<>();
                put(parameters, "Pid", query.pid);
                put(parameters, "ServiceName", query.serviceName);
                put(parameters, "OperationName", query.operationName);
                put(parameters, "TraceId", query.traceId);
                if (query.minDuration != null) parameters.put("MinDuration", query.minDuration.toString());
                if (query.maxDuration != null) parameters.put("MaxDuration", query.maxDuration.toString());
                parameters.putAll(query.parameters);
                parameters.put("StartTime", Long.toString(window.start().toEpochMilli()));
                parameters.put("EndTime", Long.toString(window.end().toEpochMilli() - 1));
                parameters.put("PageNumber", Integer.toString(page));
                parameters.put("PageSize", Integer.toString(pageSize));
                ensureAllowed("SearchTracesByPage");
                ArmsApi.ApiResponse response = call("SearchTracesByPage", parameters, taskKey);
                summary.request(response.requestId());
                JsonNode root = JsonPage.parse(response.body());
                JsonNode records = JsonPage.records(root, true);
                List<String> traceIds = findTextValues(records, "TraceID", "TraceId", "traceId");
                int count = records.size();
                summary.page(count, response.requestId());
                JsonPage.verifyPageSize(root, pageSize);
                var metadata = JsonPage.metadata(root, query.pagination);
                if (metadata.nextCursor() != null && !metadata.nextCursor().isEmpty())
                    throw PageGuard.failure("TRACE_CURSOR_UNSUPPORTED");
                boolean complete = guard.accept(records, page, pageSize, metadata);
                writeRaw("arms", query.recordType, window, response.body(), count, response.requestId());
                if (session != null) {
                    session.acceptAll(new ArmsTraceSearchNormalizer().normalize(root,
                            armsContext("TRACE_SEARCH", taskKey, query.recordType, window, response.requestId(), query.normalization)).events());
                }
                if (details) {
                    for (String traceId : traceIds) collectTraceDetail(query, window, traceId, page, taskKey, session, summary);
                }
                mark(pageKey);
                if (complete) break;
                page++;
            }
            finish(session);
            mark(doneKey);
            summary.complete();
        }
    }

    /** 采集一个 ARMS 指标任务。 */
    public void collectMetricTask(CollectorConfig.MetricQuery query, TimeWindow window) throws Exception {
        if (!"QueryMetric".equals(query.action)) throw PageGuard.failure("QUERY_METRIC_BY_PAGE_UNSUPPORTED");
        query = json.convertValue(query, CollectorConfig.MetricQuery.class);
        String checkpointKey = "arms|metric|" + query.name + "|" + window.start();
        if (done(checkpointKey)) return;
        String taskKey = CollectionTask.armsMetricKey(query.name);
        try (QuerySummary summary = new QuerySummary(taskKey, window)) {
            CollectionEventSink.TaskSession session = open(taskKey, "ARMS", query.recordType, window);
            Map<String, String> parameters = new LinkedHashMap<>();
            parameters.put("Metric", query.metric);
            parameters.put("IntervalInSec", Integer.toString(query.interval));
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
            parameters.put("StartTime", Long.toString(window.start().toEpochMilli()));
            parameters.put("EndTime", Long.toString(window.end().toEpochMilli() - 1));
            parameters.put("Limit", Integer.toString(query.limit));
            ensureAllowed(query.action);
            ArmsApi.ApiResponse response = call(query.action, parameters, taskKey);
            summary.request(response.requestId());
            JsonNode root = JsonPage.parse(response.body());
            JsonNode records = JsonPage.records(root, false);
            summary.page(records.size(), response.requestId());
            if (!new PageGuard(config.pagination).accept(records, 1, query.limit, JsonPage.metadata(root, query.pagination)))
                throw PageGuard.failure("METRIC_TRUNCATED");
            writeRaw("arms", query.recordType, window, response.body(), records.size(), response.requestId());
            if (session != null) {
                session.acceptAll(new ArmsMetricNormalizer().normalize(root,
                        armsContext("METRIC", taskKey, query.recordType, window, response.requestId(), query.normalization)).events());
            }
            finish(session);
            mark(checkpointKey);
            summary.complete();
        }
    }

    /** 采集一个 ARMS Dataset 任务。 */
    public void collectDatasetTask(CollectorConfig.DatasetQuery query, TimeWindow window) throws Exception {
        query = json.convertValue(query, CollectorConfig.DatasetQuery.class);
        String checkpointKey = "arms|dataset|" + query.name + "|" + window.start();
        if (done(checkpointKey)) return;
        String taskKey = CollectionTask.armsDatasetKey(query.name);
        try (QuerySummary summary = new QuerySummary(taskKey, window)) {
            CollectionEventSink.TaskSession session = open(taskKey, "ARMS", query.recordType, window);
            boolean cursorMode = "CURSOR".equals(query.pagination.strategy);
            if (cursorMode && (query.pagination.cursorParameter == null || query.pagination.nextCursorPointer == null))
                throw PageGuard.failure("INVALID_CURSOR_MAPPING");
            PageGuard guard = new PageGuard(config.pagination);
            Map<String, String> parameters = new LinkedHashMap<>(query.parameters);
            parameters.put("datasetId", query.datasetId);
            parameters.put("minTime", Long.toString(window.start().toEpochMilli()));
            parameters.put("maxTime", Long.toString(window.end().toEpochMilli() - 1));
            parameters.put("intervalInSec", Integer.toString(query.intervalInSec));
            parameters.put("measures", String.join(",", query.measures));
            ensureAllowed("QueryDataset");
            if (cursorMode) parameters.remove(query.pagination.cursorParameter);
            int page = 1;
            while (true) {
                ArmsApi.ApiResponse response = call("QueryDataset", parameters, taskKey);
                summary.request(response.requestId());
                JsonNode root = JsonPage.parse(response.body());
                JsonNode records = JsonPage.records(root, false);
                summary.page(records.size(), response.requestId());
                var metadata = JsonPage.metadata(root, query.pagination);
                boolean complete = guard.accept(records, page, 0, metadata);
                if (!complete && (!cursorMode || metadata.nextCursor() == null || metadata.nextCursor().isEmpty()))
                    throw PageGuard.failure("DATASET_TRUNCATED_WITHOUT_CURSOR");
                writeRaw("arms", query.recordType, window, response.body(), records.size(), response.requestId());
                if (session != null) {
                    session.acceptAll(new ArmsDatasetNormalizer().normalize(root,
                            armsContext("DATASET", taskKey, query.recordType, window, response.requestId(), query.normalization)).events());
                }
                if (complete) break;
                parameters.put(query.pagination.cursorParameter, metadata.nextCursor());
                page++;
            }
            finish(session);
            mark(checkpointKey);
            summary.complete();
        }
    }

    private void collectTraceDetail(CollectorConfig.TraceQuery query, TimeWindow window, String traceId, int page,
                                    String taskKey, CollectionEventSink.TaskSession session, QuerySummary summary) throws Exception {
        String checkpointKey = "arms|detail|" + query.name + "|" + window.start() + "|" + page + "|" + traceId;
        if (done(checkpointKey)) return;
        ensureAllowed("GetTrace");
        ArmsApi.ApiResponse detail = call("GetTrace", Map.of("TraceID", traceId), taskKey);
        summary.request(detail.requestId());
        JsonNode root = JsonPage.parse(detail.body());
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

    private ArmsApi.ApiResponse call(String action, Map<String, String> parameters, String taskKey) throws Exception {
        try {
            Map<String, String> request = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(parameters));
            ArmsApi.ApiResponse response = retry.execute(taskKey + "/" + action, () -> api.call(action, request));
            if (response == null || response.httpStatus() < 200 || response.httpStatus() >= 300)
                throw PageGuard.failure("API_STATUS");
            return response;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedException("ARMS_QUERY_INTERRUPTED");
        } catch (Exception e) {
            // SDK exceptions may embed entire responses or credentials; do not attach the cause.
            throw PageGuard.failure("ARMS_QUERY_FAILED");
        }
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

package cn.kjky.collector.sls;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.core.RetryExecutor;
import cn.kjky.collector.core.TimeWindow;
import cn.kjky.collector.normalize.NormalizationContext;
import cn.kjky.collector.normalize.SlsLogNormalizer;
import cn.kjky.collector.output.CheckpointStore;
import cn.kjky.collector.output.OutputSession;
import cn.kjky.collector.pipeline.CollectionEventSink;
import cn.kjky.collector.pipeline.CollectionTask;
import cn.kjky.collector.pagination.PageGuard;
import cn.kjky.collector.pagination.QuerySummary;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * SLS 查询编排器。
 * <p>生产模式不保存原始查询信封，也不写页面 checkpoint；每个 Project/Logstore 的成功窗口
 * 在全部标准事件进入 pending 后才由生产事件管道推进其独立水位。</p>
 */
public final class SlsCollector {
    private final CollectorConfig config;
    private final SlsApi api;
    private final RetryExecutor retry;
    private final OutputSession output;
    private final CheckpointStore checkpoint;
    private final CollectionEventSink pipeline;
    private final SlsLogNormalizer normalizer = new SlsLogNormalizer();

    /** 保持原有开发/离线调用方式。 */
    public SlsCollector(CollectorConfig config, SlsApi api, RetryExecutor retry,
                        OutputSession output, CheckpointStore checkpoint) {
        this(config, api, retry, output, checkpoint, null);
    }

    /**
     * 生产调用传入 pipeline；开发调试时可同时传入 output 作为 raw tee，
     * 但不得同时使用页面 checkpoint，避免标准事件尚未进入 pending 就跳过页面。
     */
    public SlsCollector(CollectorConfig config, SlsApi api, RetryExecutor retry,
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

    /** 分页列出配置中每个 Project 的全部 Logstore。 */
    public Map<String, SlsApi.ListResult> discover() throws Exception {
        Map<String, SlsApi.ListResult> result = new LinkedHashMap<>();
        for (var project : config.sls.projects) {
            PageGuard guard = new PageGuard(config.pagination);
            int offset = 0;
            List<String> all = new java.util.ArrayList<>();
            String requestId = null;
            int total;
            do {
                int current = offset;
                SlsApi.ListResult page = retry.execute("SLS/DISCOVER/" + project.project,
                        () -> api.listLogstores(project.project, current, 100));
                boolean complete = guard.accept(new ObjectMapper().valueToTree(page.logstores()), current, 100,
                        new PageGuard.Metadata((long) page.total(), null, null, null, null));
                all.addAll(page.logstores());
                requestId = page.requestId();
                total = page.total();
                offset += page.logstores().size();
                if (complete) break;
            } while (offset < total);
            result.put(project.project, new SlsApi.ListResult(all, all.size(), requestId));
        }
        return result;
    }

    /** 采集一个时间窗口内的全部 SLS Project/Logstore。 */
    public void collect(TimeWindow window) throws Exception {
        for (var project : config.sls.projects) {
            for (var logstore : project.logstores) collectLogstoreTask(project.project, logstore, window);
        }
    }

    /** 采集一个 SLS Project/Logstore 任务。 */
    public void collectLogstoreTask(String project, CollectorConfig.SlsLogstore logstore,
                                    TimeWindow window) throws Exception {
        // Deep copy once: concurrent configuration changes cannot alter a running window or page size.
        logstore = new ObjectMapper().convertValue(logstore, CollectorConfig.SlsLogstore.class);
        long fromLong = window.start().getEpochSecond();
        long toLong = window.end().getEpochSecond() - 1;
        if (fromLong < Integer.MIN_VALUE || fromLong > Integer.MAX_VALUE || toLong < Integer.MIN_VALUE || toLong > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("SLS Java SDK v1 的秒级时间超出 int 范围");
        }
        int from = (int) fromLong;
        int to = (int) toLong;
        String doneKey = "sls|" + project + "|" + logstore.logstore + "|" + window.start() + "|DONE";
        if (done(doneKey)) return;

        String taskKey = CollectionTask.slsLogKey(project, logstore.logstore);
        try (QuerySummary summary = new QuerySummary(taskKey, window)) {
            CollectionEventSink.TaskSession session = pipeline == null ? null
                    : pipeline.open(new CollectionTask(taskKey, "SLS", logstore.recordType, window.start(), window.end()));
            PageGuard guard = new PageGuard(config.pagination);
            long offset = 0;
            while (true) {
                String checkpointKey = "sls|" + project + "|" + logstore.logstore + "|" + window.start() + "|" + offset;
                // An incomplete checkpointed window is re-queried; never skip completeness guards.
                SlsApi.QueryResult result = poll(project, logstore, from, to, offset, summary);
                if (result.logs() == null) throw PageGuard.failure("INVALID_RECORDS");
                summary.page(result.logs().size(), result.requestId());
                boolean complete = guard.accept(new ObjectMapper().valueToTree(result.logs()), offset, logstore.pageSize,
                        new PageGuard.Metadata(result.total(), result.offset(), result.hasMore(), null, null));
                writeRaw(project, logstore, window, from, to, offset, result);
                if (session != null) {
                    NormalizationContext context = NormalizationContext.sls(taskKey, logstore.recordType,
                            window.start(), window.end(), result.requestId(), fields(logstore.normalization, true),
                            fields(logstore.normalization, false));
                    for (SlsApi.LogRecord record : result.logs()) session.acceptAll(normalizer.normalize(record, context).events());
                }
                mark(checkpointKey);
                int count = result.logs().size();
                if (complete) break;
                long nextOffset = Math.addExact(offset, count);
                if (nextOffset <= offset) throw PageGuard.failure("OFFSET_NOT_ADVANCING");
                offset = nextOffset;
            }
            if (session != null) session.finish();
            mark(doneKey);
            summary.complete();
        }
    }

    private SlsApi.QueryResult poll(String project, CollectorConfig.SlsLogstore logstore,
                                    int from, int to, long offset, QuerySummary summary) throws Exception {
        SlsApi.QueryResult last = null;
        for (int attempt = 1; attempt <= config.runtime.maxPollAttempts; attempt++) {
            long currentOffset = offset;
            try {
                last = retry.execute(CollectionTask.slsLogKey(project, logstore.logstore) + "/GetLogs",
                        () -> api.getLogs(project, logstore.logstore, from, to,
                        logstore.topic, logstore.query, currentOffset, logstore.pageSize, logstore.reverse));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedException("SLS_QUERY_INTERRUPTED");
            } catch (Exception e) { throw PageGuard.failure("SLS_QUERY_FAILED"); }
            if (last == null) throw PageGuard.failure("INVALID_RESPONSE");
            summary.request(last.requestId());
            if (last.completed()) return last;
            Thread.sleep(config.runtime.pollIntervalMillis);
        }
        throw PageGuard.failure("SLS_QUERY_INCOMPLETE");
    }

    private void writeRaw(String project, CollectorConfig.SlsLogstore logstore, TimeWindow window,
                          int from, int to, long offset, SlsApi.QueryResult result) throws Exception {
        if (output == null) return;
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
        output.write("sls", logstore.recordType, window, output.json().writeValueAsBytes(envelope),
                result.logs().size(), result.requestId());
    }

    private boolean done(String key) throws Exception {
        return checkpoint != null && checkpoint.contains(key);
    }

    private void mark(String key) throws Exception {
        if (checkpoint != null) checkpoint.mark(key);
    }

    private List<String> fields(CollectorConfig.NormalizationConfig normalization, boolean payload) {
        if (normalization == null) return List.of();
        return payload ? normalization.payloadFields : normalization.identityFields;
    }
}

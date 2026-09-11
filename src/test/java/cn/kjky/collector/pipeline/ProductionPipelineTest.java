package cn.kjky.collector.pipeline;

import cn.kjky.collector.arms.ArmsApi;
import cn.kjky.collector.arms.ArmsCollector;
import cn.kjky.collector.batch.BatchBuilder;
import cn.kjky.collector.batch.BatchIdGenerator;
import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.contract.StandardEvent;
import cn.kjky.collector.core.RetryExecutor;
import cn.kjky.collector.core.TimeWindow;
import cn.kjky.collector.outbox.OutboxStore;
import cn.kjky.collector.output.OutputSession;
import cn.kjky.collector.normalize.NormalizationContext;
import cn.kjky.collector.normalize.SlsLogNormalizer;
import cn.kjky.collector.runtime.CollectionWatermarkStore;
import cn.kjky.collector.sls.SlsApi;
import cn.kjky.collector.sls.SlsCollector;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** P0-10 采集侧串联与 P0-04 目标水位的端到端单元测试。 */
class ProductionPipelineTest {
    private static final Instant START = Instant.parse("2026-09-03T08:00:00Z");
    private static final Instant END = START.plusSeconds(60);
    private static final Clock CLOCK = Clock.fixed(END.plusSeconds(1), ZoneOffset.UTC);
    @TempDir Path temp;

    @Test
    void productionArmsCollectionWritesPendingWithoutRawOrManifest() throws Exception {
        CollectorConfig config = production(temp.resolve("production"));
        CollectorConfig.MetricQuery query = metric("cpu");
        config.arms.metricQueries.add(query);
        OutboxStore outbox = new OutboxStore(config);
        CollectionWatermarkStore watermarks = watermarks(outbox);
        config.upload.maxRecords = 1;
        ProductionEventSink sink = new ProductionEventSink(config, outbox, watermarks);

        ArmsApi api = (action, parameters) -> new ArmsApi.ApiResponse(
                "{\"Data\":[{\"Timestamp\":\"2026-09-03T08:00:10Z\",\"Value\":1}]}", 200, "metric-request");
        new ArmsCollector(config, api, new RetryExecutor(0, 1), null, null, sink)
                .collectMetricTask(query, new TimeWindow(START, END));

        assertThat(outbox.pendingBatches()).hasSize(1);
        assertThat(outbox.pendingBatches().get(0).batch().events()).hasSize(1);
        assertThat(watermarks.get(CollectionTask.armsMetricKey("cpu"))).get()
                .extracting(CollectionWatermarkStore.Watermark::windowEnd).isEqualTo(END);
        assertThat(temp.resolve("production/raw")).doesNotExist();
        assertThat(temp.resolve("production/manifest")).doesNotExist();
        assertThat(temp.resolve("production/checkpoints")).doesNotExist();
    }

    @Test
    void developmentModeStillCreatesRawAndManifest() throws Exception {
        CollectorConfig config = production(temp.resolve("development"));
        config.runtime.deploymentMode = "DEVELOPMENT";
        config.storage.rawPersistence.enabled = true;

        new OutputSession(config, START, END);

        assertThat(temp.resolve("development/raw/arms")).isDirectory();
        assertThat(temp.resolve("development/raw/sls")).isDirectory();
        assertThat(temp.resolve("development/manifest")).isDirectory();
    }

    @Test
    void productionSlsCollectionWritesPendingAndItsOwnTaskWatermark() throws Exception {
        CollectorConfig config = production(temp.resolve("production-sls"));
        CollectorConfig.SlsLogstore logstore = new CollectorConfig.SlsLogstore();
        logstore.logstore = "access";
        logstore.recordType = "flow_record";
        OutboxStore outbox = new OutboxStore(config);
        CollectionWatermarkStore watermarks = watermarks(outbox);
        ProductionEventSink sink = new ProductionEventSink(config, outbox, watermarks);
        SlsApi api = new SlsApi() {
            @Override public ListResult listLogstores(String project, int offset, int size) {
                return new ListResult(List.of("access"), 1, "list");
            }
            @Override public QueryResult getLogs(String project, String store, int from, int to, String topic,
                                                 String query, long offset, long line, boolean reverse) {
                LogRecord record = new LogRecord("10.0.0.1", Math.toIntExact(START.plusSeconds(5).getEpochSecond()), 0,
                        List.of(new Content("message", "request completed"), new Content("status", "200")));
                return new QueryResult(true, List.of(record), null, "sls-request", 128, 1);
            }
        };

        new SlsCollector(config, api, new RetryExecutor(0, 1), null, null, sink)
                .collectLogstoreTask("gateway", logstore, new TimeWindow(START, END));

        assertThat(outbox.pendingBatches()).hasSize(1);
        assertThat(watermarks.get(CollectionTask.slsLogKey("gateway", "access"))).get()
                .extracting(CollectionWatermarkStore.Watermark::windowEnd).isEqualTo(END);
    }

    @Test
    void restartAfterPendingBeforeWatermarkReusesStableIdsAndDoesNotSkipWindow() throws Exception {
        CollectorConfig config = production(temp.resolve("crash"));
        CollectionTask task = slsTask("payment", "access");
        StandardEvent event = normalizedSlsEvent(task, "same-event");
        assertThat(normalizedSlsEvent(task, "same-event").eventId()).isEqualTo(event.eventId());
        OutboxStore firstOutbox = new OutboxStore(config);
        CollectionWatermarkStore firstWatermarks = watermarks(firstOutbox);
        ProductionEventSink crashing = new ProductionEventSink(config, new BatchBuilder(config.upload),
                new BatchIdGenerator(), built -> firstOutbox.enqueue(built.batch(), built.parentBatchId()),
                firstWatermarks, CLOCK, ignored -> { throw new IOException("模拟水位写入前进程崩溃"); });

        CollectionEventSink.TaskSession first = crashing.open(task);
        first.accept(event);
        assertThatThrownBy(first::finish).isInstanceOf(IOException.class).hasMessageContaining("进程崩溃");
        String originalBatchId = firstOutbox.pendingBatches().get(0).batch().batchId();
        String originalPollId = firstOutbox.pendingBatches().get(0).batch().pollId();
        assertThat(firstWatermarks.get(task.taskKey())).isEmpty();

        OutboxStore restartedOutbox = new OutboxStore(config);
        CollectionWatermarkStore restartedWatermarks = watermarks(restartedOutbox);
        ProductionEventSink restarted = new ProductionEventSink(config, restartedOutbox, restartedWatermarks);
        CollectionEventSink.TaskSession replay = restarted.open(task);
        replay.accept(event);
        replay.finish();

        assertThat(restartedOutbox.pendingBatches()).hasSize(1);
        assertThat(restartedOutbox.pendingBatches().get(0).batch().batchId()).isEqualTo(originalBatchId);
        assertThat(restartedOutbox.pendingBatches().get(0).batch().pollId()).isEqualTo(originalPollId);
        assertThat(restartedOutbox.pendingBatches().get(0).batch().events().get(0).eventId()).isEqualTo(event.eventId());
        assertThat(restartedWatermarks.get(task.taskKey())).isPresent();
    }

    @Test
    void onlyAdvancesAfterOutboxWriteAndEmptyWindowCanAdvance() throws Exception {
        CollectorConfig config = production(temp.resolve("ordering"));
        CollectionTask task = slsTask("payment", "access");
        OutboxStore outbox = new OutboxStore(config);
        CollectionWatermarkStore watermarks = watermarks(outbox);
        ProductionEventSink failing = new ProductionEventSink(config, new BatchBuilder(config.upload),
                new BatchIdGenerator(), ignored -> { throw new IOException("模拟 Outbox 写入失败"); },
                watermarks, CLOCK, ignored -> { });

        CollectionEventSink.TaskSession failed = failing.open(task);
        failed.accept(event("cannot-persist"));
        assertThatThrownBy(failed::finish).isInstanceOf(IOException.class).hasMessageContaining("Outbox 写入失败");
        assertThat(watermarks.get(task.taskKey())).isEmpty();
        assertThat(outbox.pendingBatches()).isEmpty();

        CollectionTask emptyTask = new CollectionTask(CollectionTask.armsDatasetKey("empty"), "ARMS",
                "flow_feature", START, END);
        new ProductionEventSink(config, outbox, watermarks).open(emptyTask).finish();
        assertThat(outbox.pendingBatches()).isEmpty();
        assertThat(watermarks.get(emptyTask.taskKey())).isPresent();
    }

    @Test
    void conversionAndPaginationFailuresLeaveWatermarkUnchanged() throws Exception {
        CollectorConfig config = production(temp.resolve("collector-failures"));
        CollectorConfig.MetricQuery invalid = metric("invalid-data");
        config.arms.metricQueries.add(invalid);
        config.upload.maxRecords = 1;
        OutboxStore outbox = new OutboxStore(config);
        CollectionWatermarkStore watermarks = watermarks(outbox);
        ProductionEventSink sink = new ProductionEventSink(config, outbox, watermarks);

        ArmsApi invalidResponse = (action, parameters) -> new ArmsApi.ApiResponse("{\"Data\":{}}", 200, "invalid");
        assertThatThrownBy(() -> new ArmsCollector(config, invalidResponse, new RetryExecutor(0, 1), null, null, sink)
                .collectMetricTask(invalid, new TimeWindow(START, END))).isInstanceOf(Exception.class);
        assertThat(watermarks.get(CollectionTask.armsMetricKey("invalid-data"))).isEmpty();
        assertThat(outbox.pendingBatches()).isEmpty();

        CollectorConfig.TraceQuery trace = new CollectorConfig.TraceQuery();
        trace.name = "page-failure";
        trace.recordType = "topology_edge";
        config.arms.pageSize = 2;
        config.arms.collectTraceDetails = false;
        AtomicInteger calls = new AtomicInteger();
        ArmsApi secondPageFails = (action, parameters) -> {
            if (calls.incrementAndGet() == 1) {
                return new ArmsApi.ApiResponse("{\"TraceInfos\":[{\"TraceID\":\"t-1\"},{\"TraceID\":\"t-2\"}]}", 200, "first-page");
            }
            throw new IOException("模拟第二页失败");
        };
        assertThatThrownBy(() -> new ArmsCollector(config, secondPageFails, new RetryExecutor(0, 1), null, null, sink)
                .collectTraceTask(trace, new TimeWindow(START, END))).isInstanceOf(Exception.class);
        assertThat(outbox.pendingBatches()).hasSize(1);
        assertThat(watermarks.get(CollectionTask.armsTraceKey("page-failure"))).isEmpty();
    }

    @Test
    void armsAndSlsTasksKeepIndependentWatermarks() throws Exception {
        CollectorConfig config = production(temp.resolve("independent"));
        OutboxStore outbox = new OutboxStore(config);
        CollectionWatermarkStore watermarks = watermarks(outbox);
        ProductionEventSink sink = new ProductionEventSink(config, outbox, watermarks);
        CollectionTask arms = new CollectionTask(CollectionTask.armsMetricKey("latency"), "ARMS", "flow_feature", START, END);
        CollectionTask sls = slsTask("gateway", "access");

        CollectionEventSink.TaskSession armsSession = sink.open(arms);
        armsSession.accept(event("arms-event"));
        armsSession.finish();
        CollectionEventSink.TaskSession slsSession = sink.open(sls);
        slsSession.accept(event("sls-event"));
        slsSession.finish();

        assertThat(watermarks.snapshot()).containsKeys(arms.taskKey(), sls.taskKey()).hasSize(2);
        assertThat(outbox.pendingBatches()).hasSize(2);
    }

    private CollectorConfig production(Path root) {
        CollectorConfig config = new CollectorConfig();
        config.projectCode = "project";
        config.envCode = "test";
        config.batchId = "batch";
        config.caseId = "case";
        config.collector.collectorId = "collector-test";
        config.collector.siteId = "site-test";
        config.collector.collectorType = "ALIYUN";
        config.runtime.deploymentMode = "PRODUCTION";
        config.storage.dataRoot = root.toString();
        config.storage.rawPersistence.enabled = false;
        config.upload.maxRecords = 1_000;
        config.upload.maxCompressedBytes = 1_000_000;
        config.upload.maxRequestBytes = 1_000_000;
        config.upload.maxWaitMillis = 5_000;
        return config;
    }

    private CollectionWatermarkStore watermarks(OutboxStore outbox) throws Exception {
        return new CollectionWatermarkStore(outbox.stateRoot().resolve("collection-watermarks.json"));
    }

    private CollectorConfig.MetricQuery metric(String name) {
        CollectorConfig.MetricQuery query = new CollectorConfig.MetricQuery();
        query.name = name;
        query.recordType = "flow_feature";
        query.metric = "cpu";
        query.measures.add("value");
        return query;
    }

    private CollectionTask slsTask(String project, String logstore) {
        return new CollectionTask(CollectionTask.slsLogKey(project, logstore), "SLS", "flow_record", START, END);
    }

    private StandardEvent event(String id) {
        var payload = new ObjectMapper().createObjectNode().put("message", id);
        return StandardEvent.create(id, "flow_record", START.plusSeconds(1), null, null,
                "service", "instance", "sls/test/" + id, payload);
    }

    private StandardEvent normalizedSlsEvent(CollectionTask task, String message) {
        SlsApi.LogRecord record = new SlsApi.LogRecord("10.0.0.2", Math.toIntExact(START.plusSeconds(2).getEpochSecond()), 0,
                List.of(new SlsApi.Content("message", message), new SlsApi.Content("request_id", "request-001")));
        return new SlsLogNormalizer().normalize(record, NormalizationContext.sls(task.taskKey(), task.recordType(),
                task.windowStart(), task.windowEnd(), "request-001", List.of(), List.of())).events().get(0);
    }
}

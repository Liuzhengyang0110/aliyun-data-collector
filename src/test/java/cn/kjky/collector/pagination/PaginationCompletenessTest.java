package cn.kjky.collector.pagination;

import cn.kjky.collector.arms.*;
import cn.kjky.collector.config.*;
import cn.kjky.collector.core.*;
import cn.kjky.collector.outbox.OutboxStore;
import cn.kjky.collector.pipeline.*;
import cn.kjky.collector.runtime.CollectionWatermarkStore;
import cn.kjky.collector.sls.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;

/** Offline protocol simulations only; no assertion about undocumented site behavior. */
class PaginationCompletenessTest {
    static final TimeWindow WINDOW = new TimeWindow(Instant.parse("2026-09-19T00:00:00Z"), Instant.parse("2026-09-19T00:01:00Z"));
    @TempDir Path temp;
    CollectorConfig config;
    OutboxStore outbox;
    CollectionWatermarkStore watermarks;
    ProductionEventSink sink;
    void setup() throws Exception {
        config = new CollectorConfig();
        config.projectCode = "p"; config.envCode = "e"; config.batchId = "b"; config.caseId = "c";
        config.collector.collectorId = "collector"; config.collector.siteId = "site";
        config.storage.dataRoot = temp.toString(); config.storage.rawPersistence.enabled = false;
        config.runtime.deploymentMode = "PRODUCTION";
        config.arms.enabled = true; config.arms.endpoint = "local"; config.arms.regionId = "r"; config.arms.version = "v";
        config.arms.pageSize = 2; config.arms.collectTraceDetails = false;
        config.runtime.maxPollAttempts = 2; config.runtime.pollIntervalMillis = 0;
        config.upload.maxRecords = 1;
        outbox = new OutboxStore(config);
        watermarks = new CollectionWatermarkStore(outbox.stateRoot().resolve("collection-watermarks.json"));
        sink = new ProductionEventSink(config, outbox, watermarks);
    }
    CollectorConfig.TraceQuery trace() {
        var q = new CollectorConfig.TraceQuery(); q.name = "trace"; q.recordType = "topology_edge"; return q;
    }
    CollectorConfig.DatasetQuery dataset() {
        var q = new CollectorConfig.DatasetQuery(); q.name = "dataset"; q.recordType = "flow_feature"; q.datasetId = "test"; return q;
    }
    CollectorConfig.SlsLogstore logstore() {
        var q = new CollectorConfig.SlsLogstore(); q.logstore = "logs"; q.recordType = "flow_record"; q.pageSize = 2; return q;
    }
    ArmsCollector arms(ArmsApi api) { return new ArmsCollector(config, api, new RetryExecutor(0, 0), null, null, sink); }
    static ArmsApi.ApiResponse response(String body) { return new ArmsApi.ApiResponse(body, 200, "request-secret-marker"); }
    static String traces(int start, int count, String meta) {
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) rows.add("{\"TraceID\":\"t" + (start + i) + "\"}");
        return "{\"TraceInfos\":[" + String.join(",", rows) + "]" + meta + "}";
    }
    void assertTraceFailed(ArmsApi api, String error) throws Exception {
        assertThatThrownBy(() -> arms(api).collectTraceTask(trace(), WINDOW)).hasMessageContaining(error);
        assertThat(watermarks.get(CollectionTask.armsTraceKey("trace"))).isEmpty();
    }

    @ParameterizedTest @ValueSource(ints = {0, 1, 2, 3, 4})
    void traceCountFallbackIncludingFullPageAndEmptyFinalPage(int total) throws Exception {
        setup(); AtomicInteger calls = new AtomicInteger();
        arms((a, p) -> {
            int start = (Integer.parseInt(p.get("PageNumber")) - 1) * 2;
            calls.incrementAndGet();
            return response(traces(start, Math.min(2, total - start), ""));
        }).collectTraceTask(trace(), WINDOW);
        assertThat(calls).hasValue(total / 2 + 1);
        assertThat(watermarks.get(CollectionTask.armsTraceKey("trace"))).get()
                .extracting(CollectionWatermarkStore.Watermark::windowEnd).isEqualTo(WINDOW.end());
    }

    @Test void traceExactTotalAvoidsExtraPage() throws Exception {
        setup(); AtomicInteger calls = new AtomicInteger();
        arms((a, p) -> { calls.incrementAndGet(); return response(traces(0, 2, ",\"TotalCount\":2,\"PageNumber\":1")); })
                .collectTraceTask(trace(), WINDOW);
        assertThat(calls).hasValue(1);
    }

    @ParameterizedTest @ValueSource(strings = {",\"TotalCount\":1", ",\"TotalCount\":3,\"HasMore\":false", ",\"PageNumber\":0", ",\"HasMore\":\"false\""})
    void inconsistentOrMalformedMetadataFails(String meta) throws Exception {
        setup(); assertTraceFailed((a,p) -> response(traces(0,2,meta)), "PAGINATION_");
    }
    @Test void changingTotalFailsAndKeepsPublishedPending() throws Exception {
        setup();
        assertTraceFailed((a,p) -> response(traces(Integer.parseInt(p.get("PageNumber"))*2,2,
                ",\"TotalCount\":" + (p.get("PageNumber").equals("1") ? "4" : "5"))), "TOTAL_CHANGED");
        assertThat(outbox.pendingBatches()).isNotEmpty();
    }
    @Test void emptyPageBeforeReportedTotalFails() throws Exception {
        setup(); assertTraceFailed((a,p) -> response(traces(0,p.get("PageNumber").equals("1")?2:0,",\"TotalCount\":3")), "EMPTY_PAGE_WITH_MORE");
    }
    @Test void repeatedResponseFailsEvenWithDifferentRequestIds() throws Exception {
        setup(); assertTraceFailed((a,p) -> new ArmsApi.ApiResponse(traces(0,2,""),200,p.get("PageNumber")), "REPEATED_PAGE");
    }
    @Test void responsePageNumberNotAdvancingFails() throws Exception {
        setup(); assertTraceFailed((a,p) -> response(traces(Integer.parseInt(p.get("PageNumber"))*2,2,",\"PageNumber\":1")), "POSITION_NOT_ADVANCING");
    }
    @Test void pageLimitFailsWithoutCallingAnotherPage() throws Exception {
        setup(); config.pagination.maxPages = 1; AtomicInteger calls = new AtomicInteger();
        assertTraceFailed((a,p) -> { calls.incrementAndGet(); return response(traces(0,2,"")); }, "MAX_PAGES");
        assertThat(calls).hasValue(1);
    }
    @Test void recordLimitFailsClosed() throws Exception {
        setup(); config.pagination.maxRecords = 1;
        assertTraceFailed((a,p) -> response(traces(0,2,"")), "MAX_RECORDS");
    }
    @Test void middlePageFailureDoesNotAdvanceWatermarkOrLeakSdkResponse() throws Exception {
        setup();
        assertTraceFailed((a,p) -> {
            if (p.get("PageNumber").equals("1")) return response(traces(0,2,""));
            throw new IOException("AccessKeySecret=private-response-marker");
        }, "ARMS_QUERY_FAILED");
        assertThat(outbox.pendingBatches()).isNotEmpty();
    }
    @Test void configuredTraceMetadataSupportsShortIntermediatePages() throws Exception {
        setup(); var q = trace(); q.pagination.totalPointer = "/meta/count"; q.pagination.positionPointer = "/meta/index";
        AtomicInteger calls = new AtomicInteger();
        arms((a,p) -> { int n = calls.incrementAndGet(); return response(traces(n,1,",\"meta\":{\"count\":2,\"index\":"+n+"}")); })
                .collectTraceTask(q,WINDOW);
        assertThat(calls).hasValue(2);
    }

    @Test void queryMetricByPageRejectedAtValidationAndRuntime() throws Exception {
        setup(); var q = new CollectorConfig.MetricQuery(); q.name="metric"; q.recordType="flow_feature";
        q.metric="cpu"; q.measures.add("value"); q.action="QueryMetricByPage"; config.arms.metricQueries.add(q);
        assertThat(new ConfigValidator().validate(config,false)).anyMatch(e -> e.contains("QueryMetricByPage") && e.contains("禁止"));
        assertThatThrownBy(() -> arms((a,p)-> { throw new AssertionError("must not call API"); }).collectMetricTask(q,WINDOW))
                .hasMessageContaining("UNSUPPORTED");
    }
    @Test void metricFullLimitWithoutCompletionEvidenceFails() throws Exception {
        setup(); var q = new CollectorConfig.MetricQuery(); q.name="metric"; q.recordType="flow_feature"; q.metric="cpu"; q.limit=1;
        assertThatThrownBy(() -> arms((a,p)->response("{\"Data\":[{\"value\":1}]}")).collectMetricTask(q,WINDOW))
                .hasMessageContaining("METRIC_TRUNCATED");
        assertThat(watermarks.snapshot()).isEmpty();
    }
    @ParameterizedTest @ValueSource(strings = {"{\"Data\":[{\"value\":1}],\"truncated\":true}", "{\"Data\":[{\"value\":1}]}", "{\"Data\":[],\"hasMore\":true}"})
    void datasetTruncationOrUnknownCompletenessFails(String body) throws Exception {
        setup(); assertThatThrownBy(() -> arms((a,p)->response(body)).collectDatasetTask(dataset(),WINDOW)).hasMessageContaining("PAGINATION_");
        assertThat(watermarks.snapshot()).isEmpty();
    }
    @Test void datasetEmptyWithExplicitEvidenceAdvances() throws Exception {
        setup(); arms((a,p)->response("{\"Data\":[],\"total\":0}")).collectDatasetTask(dataset(),WINDOW);
        assertThat(watermarks.get(CollectionTask.armsDatasetKey("dataset"))).get()
                .extracting(CollectionWatermarkStore.Watermark::windowEnd).isEqualTo(WINDOW.end());
    }
    CollectorConfig.DatasetQuery cursorQuery() {
        var q = dataset(); q.pagination.strategy="CURSOR"; q.pagination.nextCursorPointer="/paging/continuation";
        q.pagination.cursorParameter="fixtureContinuation"; return q;
    }
    @Test void datasetUsesOnlyExplicitConfiguredCursor() throws Exception {
        setup(); AtomicInteger calls=new AtomicInteger();
        arms((a,p)-> {
            int n=calls.incrementAndGet();
            assertThat(p.get("fixtureContinuation")).isEqualTo(n==1?null:"opaque");
            assertThat(p.get("minTime")).isEqualTo(Long.toString(WINDOW.start().toEpochMilli()));
            return response("{\"Data\":[{\"value\":"+n+"}],\"paging\":{\"continuation\":"+(n==1?"\"opaque\"":"null")+"}}");
        }).collectDatasetTask(cursorQuery(),WINDOW);
        assertThat(calls).hasValue(2); assertThat(watermarks.snapshot()).hasSize(1);
    }
    @Test void datasetRepeatedCursorFailsDespiteChangingRecords() throws Exception {
        setup(); AtomicInteger calls=new AtomicInteger();
        assertThatThrownBy(() -> arms((a,p)->response("{\"Data\":[{\"value\":"+calls.incrementAndGet()+"}],\"paging\":{\"continuation\":\"opaque\"}}"))
                .collectDatasetTask(cursorQuery(),WINDOW)).hasMessageContaining("CURSOR_NOT_ADVANCING");
        assertThat(calls).hasValue(2); assertThat(watermarks.snapshot()).isEmpty();
    }
    @Test void missingConfiguredCursorFails() throws Exception {
        setup(); assertThatThrownBy(() -> arms((a,p)->response("{\"Data\":[],\"total\":0}"))
                .collectDatasetTask(cursorQuery(),WINDOW)).hasMessageContaining("MISSING_METADATA");
        assertThat(watermarks.snapshot()).isEmpty();
    }

    interface Logs { SlsApi.QueryResult get(long offset) throws Exception; }
    SlsApi slsApi(Logs logs) { return new SlsApi() {
        public ListResult listLogstores(String p,int o,int s) { return new ListResult(List.of(),0,"r"); }
        public QueryResult getLogs(String p,String l,int f,int t,String topic,String q,long offset,long line,boolean reverse) throws Exception { return logs.get(offset); }
    }; }
    SlsApi.QueryResult logs(int first,int count,Long total,Long offset,Boolean more) {
        List<SlsApi.LogRecord> rows=new ArrayList<>();
        for(int i=0;i<count;i++) rows.add(new SlsApi.LogRecord("local",(int)WINDOW.start().getEpochSecond(),0,List.of(new SlsApi.Content("message","row-"+(first+i)))));
        return new SlsApi.QueryResult(true,rows,null,"r",0,0,total,offset,more);
    }
    void collectSls(SlsApi api,CollectorConfig.SlsLogstore q) throws Exception {
        new SlsCollector(config,api,new RetryExecutor(0,0),null,null,sink).collectLogstoreTask("project",q,WINDOW);
    }
    @ParameterizedTest @ValueSource(ints={0,1,2,3,4})
    void slsCountFallbackAndEmptyFinalPage(int total) throws Exception {
        setup(); AtomicInteger calls=new AtomicInteger();
        collectSls(slsApi(o->{calls.incrementAndGet();return logs((int)o,Math.min(2,total-(int)o),null,null,null);}),logstore());
        assertThat(calls).hasValue(total/2+1);
        assertThat(watermarks.get(CollectionTask.slsLogKey("project","logs"))).get()
                .extracting(CollectionWatermarkStore.Watermark::windowEnd).isEqualTo(WINDOW.end());
    }
    @ParameterizedTest @ValueSource(strings={"offset","repeat","total","empty","exception","incomplete","limit"})
    void slsUnsafePaginationNeverAdvances(String mode) throws Exception {
        setup(); if(mode.equals("limit")) config.pagination.maxPages=2;
        assertThatThrownBy(()->collectSls(slsApi(o->{
            if(o==0) return logs(0,2,mode.equals("total")?4L:null,0L,null);
            return switch(mode) {
                case "offset" -> logs(2,1,null,0L,null);
                case "repeat" -> logs(0,2,null,o,null);
                case "total" -> logs(2,2,5L,o,null);
                case "empty" -> logs(2,0,3L,o,null);
                case "exception" -> throw new IOException("private-response-marker");
                case "incomplete" -> new SlsApi.QueryResult(false,List.of(),null,"r",0,0);
                default -> logs((int)o,2,null,o,null);
            };
        }),logstore())).hasMessageContaining("PAGINATION_");
        assertThat(watermarks.snapshot()).isEmpty();
        assertThat(outbox.pendingBatches()).isNotEmpty();
    }
    @Test void slsFreezesParametersWhileNewWritesAndConfigChangesOccur() throws Exception {
        setup(); var q=logstore(); q.query="original"; q.topic="topic"; AtomicInteger calls=new AtomicInteger();
        SlsApi api=new SlsApi() {
            public ListResult listLogstores(String p,int o,int n) { return null; }
            public QueryResult getLogs(String p,String l,int f,int t,String topic,String query,long offset,long line,boolean reverse) {
                assertThat(l).isEqualTo("logs"); assertThat(query).isEqualTo("original"); assertThat(topic).isEqualTo("topic");
                assertThat(line).isEqualTo(2); assertThat(reverse).isFalse();
                assertThat(f).isEqualTo((int)WINDOW.start().getEpochSecond()); assertThat(t).isEqualTo((int)WINDOW.end().getEpochSecond()-1);
                calls.incrementAndGet(); q.query="changed"; q.topic="changed"; q.pageSize=1; q.logstore="changed"; q.reverse=true;
                // New writes beyond t never enter the simulated fixed window.
                return logs((int)offset,offset==0?2:1,null,offset,null);
            }
        };
        collectSls(api,q); assertThat(calls).hasValue(2); assertThat(watermarks.snapshot()).hasSize(1);
    }
    @Test void safeSummaryAndMalformedJsonDoNotLeakBodyOrRequestId() throws Exception {
        setup(); ByteArrayOutputStream bytes=new ByteArrayOutputStream(); PrintStream old=System.out;
        try (PrintStream capture=new PrintStream(bytes,true,StandardCharsets.UTF_8)) {
            System.setOut(capture);
            Throwable error=catchThrowable(()->arms((a,p)->response("{private-response-marker AccessKeySecret=secret" )).collectTraceTask(trace(),WINDOW));
            assertThat(error).hasMessage("PAGINATION_INVALID_JSON").hasNoCause();
            arms((a,p)->response(traces(0,0,""))).collectTraceTask(trace(),WINDOW);
        } finally { System.setOut(old); }
        String text=bytes.toString(StandardCharsets.UTF_8);
        assertThat(text).contains("source_key","window_start","window_end","page_count","record_count","request_id_sha256_sample","\"complete\":false","\"complete\":true")
                .doesNotContain("private-response-marker","request-secret-marker","AccessKeySecret");
    }
    @Test void invalidLimitsMappingsAndReservedParametersRejected() throws Exception {
        setup(); config.pagination.maxPages=0; config.pagination.maxRecords=0; var q=trace();
        q.pagination.totalPointer="bad"; q.parameters.put("PageNumber","1"); config.arms.traceQueries.add(q);
        assertThat(new ConfigValidator().validate(config,false)).anyMatch(e->e.contains("maxPages"))
                .anyMatch(e->e.contains("maxRecords")).anyMatch(e->e.contains("JSON Pointer")).anyMatch(e->e.contains("受保护"));
    }
    @Test void repeatedCanonicalPageIgnoresObjectFieldOrdering() {
        PageGuard guard=new PageGuard(new CollectorConfig.PaginationLimits());
        guard.accept(JsonPage.parse("[{\"a\":1,\"b\":2}]"),1,1,PageGuard.Metadata.empty());
        assertThatThrownBy(()->guard.accept(JsonPage.parse("[{\"b\":2,\"a\":1}]"),2,1,PageGuard.Metadata.empty()))
                .hasMessageContaining("REPEATED_PAGE");
    }
    @Test void serverPageSizeClampCannotCausePrematureSuccess() throws Exception {
        setup(); assertTraceFailed((a,p)->response(traces(0,1,",\"PageSize\":1")),"PAGE_SIZE_MISMATCH");
    }
    @ParameterizedTest @ValueSource(strings={"{\"Data\":[],\"hasMore\":true,\"hasMore\":false}","{\"Data\":[]} {\"truncated\":true}"})
    void ambiguousJsonCannotHideTruncation(String body) {
        assertThatThrownBy(()->JsonPage.parse(body)).hasMessage("PAGINATION_INVALID_JSON").hasNoCause();
    }
    @Test void slsErrorsAreBodyFreeWithoutNestedSdkCause() throws Exception {
        setup(); Throwable error=catchThrowable(()->collectSls(slsApi(o->{throw new IOException("private-response-marker credentials");}),logstore()));
        assertThat(error).hasMessage("PAGINATION_SLS_QUERY_FAILED").hasNoCause();
        assertThat(watermarks.snapshot()).isEmpty();
    }
    @Test void explicitHasMoreEndsAnExactlyFullTracePage() throws Exception {
        setup(); AtomicInteger calls=new AtomicInteger();
        arms((a,p)->{calls.incrementAndGet();return response(traces(0,2,",\"hasMore\":false"));}).collectTraceTask(trace(),WINDOW);
        assertThat(calls).hasValue(1); assertThat(watermarks.snapshot()).hasSize(1);
    }
    @Test void reorderedSameRecordsDoNotCountAsProgress() {
        PageGuard guard=new PageGuard(new CollectorConfig.PaginationLimits());
        guard.accept(JsonPage.parse("[{\"a\":1},{\"a\":2}]"),0,2,PageGuard.Metadata.empty());
        assertThatThrownBy(()->guard.accept(JsonPage.parse("[{\"a\":2},{\"a\":1}]"),2,2,PageGuard.Metadata.empty()))
                .hasMessageContaining("REPEATED_PAGE");
    }
    @ParameterizedTest @ValueSource(strings={"trace","sls"})
    void incompleteRawCheckpointsCannotSkipCompletenessChecks(String source) throws Exception {
        setup(); config.runtime.deploymentMode="DEVELOPMENT"; config.storage.rawPersistence.enabled=true;
        var output=new cn.kjky.collector.output.OutputSession(config,WINDOW.start(),WINDOW.end());
        var checkpoints=new cn.kjky.collector.output.CheckpointStore(output.root(),output.json());
        AtomicInteger calls=new AtomicInteger();
        if(source.equals("trace")) {
            checkpoints.mark("arms|trace|trace|"+WINDOW.start()+"|1");
            ArmsApi api=(a,p)->{calls.incrementAndGet();assertThat(p.get("PageNumber")).isEqualTo("1");return response(traces(0,1,""));};
            new ArmsCollector(config,api,new RetryExecutor(0,0),output,checkpoints).collectTraceTask(trace(),WINDOW);
        } else {
            checkpoints.mark("sls|project|logs|"+WINDOW.start()+"|0");
            SlsApi api=slsApi(o->{calls.incrementAndGet();assertThat(o).isZero();return logs(0,1,null,null,null);});
            new SlsCollector(config,api,new RetryExecutor(0,0),output,checkpoints).collectLogstoreTask("project",logstore(),WINDOW);
        }
        assertThat(calls).hasValue(1);
        assertThat(output.manifest().files).hasSize(1);
    }
}

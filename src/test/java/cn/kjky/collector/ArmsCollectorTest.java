package cn.kjky.collector;

import cn.kjky.collector.arms.ArmsApi;
import cn.kjky.collector.arms.ArmsCollector;
import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.core.RetryExecutor;
import cn.kjky.collector.core.TimeWindow;
import cn.kjky.collector.output.CheckpointStore;
import cn.kjky.collector.output.OutputSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ArmsCollectorTest {
    @TempDir Path temp;

    @Test
    void storesSearchDetailAndMetricResponses() throws Exception {
        CollectorConfig c = CoreTest.base();
        c.outputRoot = temp.toString();
        c.arms.enabled = true;
        c.arms.pageSize = 100;
        CollectorConfig.TraceQuery trace = new CollectorConfig.TraceQuery();
        trace.name = "trace-task";
        trace.recordType = "topology_edge";
        c.arms.traceQueries.add(trace);
        CollectorConfig.MetricQuery metric = new CollectorConfig.MetricQuery();
        metric.name = "metric-task";
        metric.recordType = "flow_feature";
        metric.metric = "m1";
        metric.measures.add("value");
        c.arms.metricQueries.add(metric);

        Instant start = Instant.parse("2026-07-27T01:00:00Z");
        Instant end = start.plusSeconds(60);
        OutputSession output = new OutputSession(c, start, end);
        FakeArms api = new FakeArms();
        new ArmsCollector(c, api, new RetryExecutor(0, 1), output,
                new CheckpointStore(output.root(), output.json())).collect(new TimeWindow(start, end));

        assertThat(api.actions).containsExactly("SearchTracesByPage", "GetTrace", "QueryMetric");
        assertThat(output.manifest().files).hasSize(3);
        assertThat(output.manifest().files).extracting(e -> e.recordType)
                .containsExactly("topology_edge", "topology_edge", "flow_feature");
    }

    static class FakeArms implements ArmsApi {
        final List<String> actions = new ArrayList<>();
        @Override public ApiResponse call(String action, Map<String, String> parameters) {
            actions.add(action);
            if ("SearchTracesByPage".equals(action)) {
                return new ApiResponse("{\"PageBean\":{\"TraceInfos\":[{\"TraceID\":\"t1\"}]}}", 200, "r1");
            }
            if ("GetTrace".equals(action)) {
                return new ApiResponse("{\"Spans\":[{\"SpanId\":\"s1\"}]}", 200, "r2");
            }
            if ("QueryMetric".equals(action)) {
                return new ApiResponse("{\"Data\":[{\"value\":1}]}", 200, "r3");
            }
            throw new AssertionError(action);
        }
    }
}

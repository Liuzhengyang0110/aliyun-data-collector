package cn.kjky.collector;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.core.RetryExecutor;
import cn.kjky.collector.core.TimeWindow;
import cn.kjky.collector.output.CheckpointStore;
import cn.kjky.collector.output.OutputSession;
import cn.kjky.collector.service.OutputValidator;
import cn.kjky.collector.sls.SlsApi;
import cn.kjky.collector.sls.SlsCollector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SlsCollectorTest {
    @TempDir Path temp;

    @Test
    void paginatesWritesManifestAndResumesWithoutDuplicateCalls() throws Exception {
        CollectorConfig c = CoreTest.base();
        c.outputRoot = temp.toString();
        c.sls.enabled = true;
        var project = new CollectorConfig.SlsProject();
        project.project = "p1";
        var logstore = new CollectorConfig.SlsLogstore();
        logstore.logstore = "l1";
        logstore.recordType = "flow_record";
        logstore.pageSize = 2;
        project.logstores.add(logstore);
        c.sls.projects.add(project);
        Instant start = Instant.parse("2026-07-27T01:00:00Z");
        Instant end = start.plusSeconds(60);
        TimeWindow window = new TimeWindow(start, end);
        FakeSls api = new FakeSls();

        OutputSession output = new OutputSession(c, start, end);
        CheckpointStore checkpoints = new CheckpointStore(output.root(), output.json());
        new SlsCollector(c, api, new RetryExecutor(0, 1), output, checkpoints).collect(window);
        assertThat(api.calls).isEqualTo(2);
        assertThat(output.manifest().files).hasSize(2);
        assertThat(output.manifest().files.get(0).relativePath).contains("_sls_flow_record_");
        assertThat(new OutputValidator().validate(temp)).isEmpty();

        OutputSession resumed = new OutputSession(c, start, end);
        new SlsCollector(c, api, new RetryExecutor(0, 1), resumed,
                new CheckpointStore(resumed.root(), resumed.json())).collect(window);
        assertThat(api.calls).isEqualTo(2);
        assertThat(resumed.manifest().files).hasSize(2);
    }

    static class FakeSls implements SlsApi {
        int calls;
        @Override public ListResult listLogstores(String project, int offset, int size) {
            return new ListResult(List.of("l1"), 1, "list-request");
        }
        @Override public QueryResult getLogs(String project, String logstore, int from, int to, String topic,
                                             String query, long offset, long line, boolean reverse) {
            calls++;
            if (offset == 0) return new QueryResult(true, List.of(record("a"), record("b")), null, "r1", 10, 2);
            return new QueryResult(true, List.of(record("c")), null, "r2", 5, 1);
        }
        private LogRecord record(String value) {
            return new LogRecord("127.0.0.1", 1, 0, List.of(new Content("message", value)));
        }
    }
}

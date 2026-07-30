package cn.kjky.collector;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.config.ConfigValidator;
import cn.kjky.collector.core.TimeWindow;
import cn.kjky.collector.core.WindowPlanner;
import cn.kjky.collector.output.FileNaming;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CoreTest {
    @Test
    void splitsWindowsAndUsesExistingNamingConvention() {
        Instant start = Instant.parse("2026-07-27T01:00:00Z");
        Instant end = Instant.parse("2026-07-27T01:25:00Z");
        List<TimeWindow> windows = new WindowPlanner().split(start, end, 600);
        assertThat(windows).hasSize(3);
        assertThat(windows.get(2).end()).isEqualTo(end);

        CollectorConfig c = base();
        String name = new FileNaming().name(c, "arms", "topology_edge", windows.get(0), 1, "json");
        assertThat(name).isEqualTo("kjky20250400_test_b001_arms_topology_edge_20260727T010000Z_20260727T011000Z_p0001.json");
    }

    @Test
    void validatesSiteValuesWithoutReadingSecrets() {
        CollectorConfig c = base();
        c.arms.enabled = true;
        c.arms.endpoint = "arms.internal.example";
        c.arms.regionId = "cn-test";
        c.arms.version = "2019-08-08";
        CollectorConfig.TraceQuery q = new CollectorConfig.TraceQuery();
        q.name = "traces";
        q.recordType = "topology_edge";
        c.arms.traceQueries.add(q);
        assertThat(new ConfigValidator().validate(c, false)).isEmpty();
    }

    static CollectorConfig base() {
        CollectorConfig c = new CollectorConfig();
        c.projectCode = "kjky20250400";
        c.envCode = "test";
        c.batchId = "b001";
        c.caseId = "case01";
        c.outputRoot = "./target/test-output";
        return c;
    }
}

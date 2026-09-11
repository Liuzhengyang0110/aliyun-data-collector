package cn.kjky.collector.normalize;

import cn.kjky.collector.contract.StandardEvent;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/** 将 ARMS QueryMetric/QueryMetricByPage 返回的数据点转换为标准事件。 */
public final class ArmsMetricNormalizer implements EventNormalizer<JsonNode> {
    private static final List<String> RECORD_PATHS = List.of(
            "Data", "data", "Records", "records", "Result", "result", "PageBean.Records");

    @Override
    public NormalizationResult normalize(JsonNode source, NormalizationContext context) {
        if (!"METRIC".equals(context.sourceKind())) {
            throw new IllegalArgumentException("ArmsMetricNormalizer 需要 sourceKind=METRIC");
        }
        List<StandardEvent> events = new ArrayList<>();
        List<NormalizationWarning> warnings = NormalizationSupport.warningList();
        for (JsonNode record : NormalizationSupport.requiredRecords(source, RECORD_PATHS, "ARMS Metric")) {
            events.add(NormalizationSupport.event(record, context, null, null, null, List.of(),
                    NormalizationSupport.RECORD_IDS, NormalizationSupport.METRIC_PAYLOAD, warnings));
        }
        return new NormalizationResult(events, warnings);
    }
}

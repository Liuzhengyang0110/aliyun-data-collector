package cn.kjky.collector.normalize;

import cn.kjky.collector.contract.StandardEvent;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/** 将 ARMS QueryDataset 返回的每一行转换为标准事件。 */
public final class ArmsDatasetNormalizer implements EventNormalizer<JsonNode> {
    private static final List<String> RECORD_PATHS = List.of(
            "Data", "data", "Records", "records", "Result", "result", "Rows", "rows");

    @Override
    public NormalizationResult normalize(JsonNode source, NormalizationContext context) {
        if (!"DATASET".equals(context.sourceKind())) {
            throw new IllegalArgumentException("ArmsDatasetNormalizer 需要 sourceKind=DATASET");
        }
        List<StandardEvent> events = new ArrayList<>();
        List<NormalizationWarning> warnings = NormalizationSupport.warningList();
        for (JsonNode record : NormalizationSupport.requiredRecords(source, RECORD_PATHS, "ARMS Dataset")) {
            events.add(NormalizationSupport.event(record, context, null, null, null, List.of(),
                    NormalizationSupport.RECORD_IDS, NormalizationSupport.DATASET_PAYLOAD, warnings));
        }
        return new NormalizationResult(events, warnings);
    }
}

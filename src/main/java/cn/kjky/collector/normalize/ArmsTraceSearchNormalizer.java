package cn.kjky.collector.normalize;

import cn.kjky.collector.contract.StandardEvent;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/** 将 ARMS SearchTracesByPage 的每条 Trace 摘要转换为标准事件。 */
public final class ArmsTraceSearchNormalizer implements EventNormalizer<JsonNode> {
    private static final List<String> RECORD_PATHS = List.of(
            "PageBean.TraceInfos", "Data.TraceInfos", "TraceInfos", "data.traceInfos", "records");

    @Override
    public NormalizationResult normalize(JsonNode source, NormalizationContext context) {
        requireKind(context, "TRACE_SEARCH");
        List<StandardEvent> events = new ArrayList<>();
        List<NormalizationWarning> warnings = NormalizationSupport.warningList();
        for (JsonNode record : NormalizationSupport.requiredRecords(source, RECORD_PATHS, "ARMS Trace 搜索")) {
            String traceId = NormalizationSupport.firstText(record, NormalizationSupport.TRACE_IDS);
            StandardEvent event = NormalizationSupport.event(record, context, null, traceId, null,
                    traceId == null ? List.of() : List.of(traceId), NormalizationSupport.RECORD_IDS,
                    NormalizationSupport.TRACE_PAYLOAD, warnings);
            events.add(event);
            if (traceId == null) {
                warnings.add(new NormalizationWarning("TRACE_ID_MISSING",
                        "Trace 摘要缺少 Trace ID，已使用事件时间和规范 payload 生成标识",
                        event.sourceRef(), NormalizationSupport.TRACE_IDS));
            }
        }
        return new NormalizationResult(events, warnings);
    }

    private void requireKind(NormalizationContext context, String expected) {
        if (!expected.equals(context.sourceKind())) {
            throw new IllegalArgumentException("ArmsTraceSearchNormalizer 需要 sourceKind=" + expected);
        }
    }
}

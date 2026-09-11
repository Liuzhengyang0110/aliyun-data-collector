package cn.kjky.collector.normalize;

import cn.kjky.collector.contract.StandardEvent;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/** 将 ARMS GetTrace 响应中的每个 Span 转换为标准事件。 */
public final class ArmsTraceDetailNormalizer implements EventNormalizer<JsonNode> {
    private static final List<String> RECORD_PATHS = List.of(
            "Spans", "Data.Spans", "TraceInfo.Spans", "data.spans", "traceInfo.spans");
    private static final List<String> ROOT_TRACE_IDS = List.of(
            "TraceID", "TraceId", "traceId", "trace_id", "Data.TraceID", "Data.TraceId",
            "Data.traceId", "TraceInfo.TraceID", "TraceInfo.TraceId", "TraceInfo.traceId");

    @Override
    public NormalizationResult normalize(JsonNode source, NormalizationContext context) {
        requireKind(context, "TRACE_DETAIL");
        String rootTraceId = NormalizationSupport.firstText(source, ROOT_TRACE_IDS);
        List<StandardEvent> events = new ArrayList<>();
        List<NormalizationWarning> warnings = NormalizationSupport.warningList();
        for (JsonNode span : NormalizationSupport.requiredRecords(source, RECORD_PATHS, "ARMS Trace 详情")) {
            String traceId = NormalizationSupport.firstText(span, NormalizationSupport.TRACE_IDS);
            if (traceId == null) traceId = rootTraceId;
            String spanId = NormalizationSupport.firstText(span, NormalizationSupport.SPAN_IDS);
            List<String> stable = new ArrayList<>();
            if (traceId != null) stable.add(traceId);
            if (spanId != null) stable.add(spanId);
            StandardEvent event = NormalizationSupport.event(span, context, null, traceId, spanId,
                    stable, NormalizationSupport.RECORD_IDS, NormalizationSupport.SPAN_PAYLOAD, warnings);
            events.add(event);
            if (traceId == null) {
                warnings.add(new NormalizationWarning("TRACE_ID_MISSING", "Span 缺少 Trace ID",
                        event.sourceRef(), NormalizationSupport.TRACE_IDS));
            }
            if (spanId == null) {
                warnings.add(new NormalizationWarning("SPAN_ID_MISSING",
                        "Span 缺少 Span ID，已使用其余稳定字段生成标识",
                        event.sourceRef(), NormalizationSupport.SPAN_IDS));
            }
        }
        return new NormalizationResult(events, warnings);
    }

    private void requireKind(NormalizationContext context, String expected) {
        if (!expected.equals(context.sourceKind())) {
            throw new IllegalArgumentException("ArmsTraceDetailNormalizer 需要 sourceKind=" + expected);
        }
    }
}

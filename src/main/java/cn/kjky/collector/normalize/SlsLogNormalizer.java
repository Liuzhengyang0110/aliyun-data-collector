package cn.kjky.collector.normalize;

import cn.kjky.collector.contract.StandardEvent;
import cn.kjky.collector.sls.SlsApi;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.DateTimeException;
import java.time.Instant;
import java.util.List;

/** 将一条已由 SLS SDK 解码的日志转换为标准事件。 */
public final class SlsLogNormalizer implements EventNormalizer<SlsApi.LogRecord> {
    @Override
    public NormalizationResult normalize(SlsApi.LogRecord source, NormalizationContext context) {
        if (!"SLS".equals(context.sourceType()) || !"LOG".equals(context.sourceKind())) {
            throw new IllegalArgumentException("SlsLogNormalizer 需要 sourceType=SLS、sourceKind=LOG");
        }
        if (source == null) throw new NormalizationException("EMPTY_RECORD", "SLS 日志记录为空");
        if (source.timeNsPart() < 0 || source.timeNsPart() > 999_999_999) {
            throw new NormalizationException("INVALID_EVENT_TIME", "SLS timeNsPart 必须在 0..999999999");
        }

        ObjectNode record = JsonNodeFactory.instance.objectNode();
        if (source.source() != null && !source.source().isBlank()) record.put("source", source.source());
        record.put("__time__", source.time());
        record.put("__time_ns_part__", source.timeNsPart());
        if (source.contents() != null) for (SlsApi.Content content : source.contents()) {
            if (content == null || content.key() == null || content.key().isBlank()) continue;
            append(record, content.key(), content.value());
        }

        Instant eventTime;
        try {
            eventTime = Instant.ofEpochSecond(source.time(), source.timeNsPart());
        } catch (DateTimeException e) {
            throw new NormalizationException("INVALID_EVENT_TIME", "SLS 日志时间超出支持范围", e);
        }
        List<NormalizationWarning> warnings = NormalizationSupport.warningList();
        StandardEvent event = NormalizationSupport.event(record, context, eventTime, null, null, List.of(),
                NormalizationSupport.RECORD_IDS, NormalizationSupport.SLS_PAYLOAD, warnings);
        return new NormalizationResult(List.of(event), warnings);
    }

    /** 同名 SLS Content 保持原始顺序并转换为数组，避免静默覆盖。 */
    private void append(ObjectNode record, String key, String value) {
        JsonNode existing = record.get(key);
        if (existing == null) {
            record.put(key, value);
        } else if (existing.isArray()) {
            ((ArrayNode) existing).add(value);
        } else {
            ArrayNode values = JsonNodeFactory.instance.arrayNode();
            values.add(existing);
            values.add(value);
            record.set(key, values);
        }
    }
}

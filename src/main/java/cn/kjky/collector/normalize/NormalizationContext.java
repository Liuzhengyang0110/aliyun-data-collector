package cn.kjky.collector.normalize;

import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/** 单个采集任务和查询时间窗的标准化上下文。 */
public record NormalizationContext(
        String sourceType,
        String sourceKind,
        String sourceKey,
        String recordType,
        Instant windowStart,
        Instant windowEnd,
        String requestId,
        List<String> payloadFields,
        List<String> identityFields) {

    public NormalizationContext {
        sourceType = required(sourceType, "sourceType").toUpperCase(Locale.ROOT);
        if (!"ARMS".equals(sourceType) && !"SLS".equals(sourceType)) {
            throw new IllegalArgumentException("sourceType 只能是 ARMS 或 SLS");
        }
        sourceKind = required(sourceKind, "sourceKind").toUpperCase(Locale.ROOT);
        sourceKey = required(sourceKey, "sourceKey");
        recordType = required(recordType, "recordType");
        Objects.requireNonNull(windowStart, "windowStart 不能为空");
        Objects.requireNonNull(windowEnd, "windowEnd 不能为空");
        if (!windowStart.isBefore(windowEnd)) throw new IllegalArgumentException("windowStart 必须早于 windowEnd");
        payloadFields = clean(payloadFields);
        identityFields = clean(identityFields);
    }

    /** 创建 ARMS 任务上下文。 */
    public static NormalizationContext arms(String sourceKind, String sourceKey, String recordType,
                                            Instant windowStart, Instant windowEnd, String requestId,
                                            List<String> payloadFields, List<String> identityFields) {
        return new NormalizationContext("ARMS", sourceKind, sourceKey, recordType, windowStart, windowEnd,
                requestId, payloadFields, identityFields);
    }

    /** 创建 SLS Logstore 上下文。 */
    public static NormalizationContext sls(String sourceKey, String recordType,
                                           Instant windowStart, Instant windowEnd, String requestId,
                                           List<String> payloadFields, List<String> identityFields) {
        return new NormalizationContext("SLS", "LOG", sourceKey, recordType, windowStart, windowEnd,
                requestId, payloadFields, identityFields);
    }

    private static String required(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " 不能为空");
        return value.trim();
    }

    private static List<String> clean(List<String> values) {
        if (values == null || values.isEmpty()) return List.of();
        LinkedHashSet<String> result = new LinkedHashSet<>();
        for (String value : values) result.add(required(value, "标准化字段名"));
        return List.copyOf(result);
    }
}

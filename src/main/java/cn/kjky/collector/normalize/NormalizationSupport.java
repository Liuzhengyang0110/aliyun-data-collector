package cn.kjky.collector.normalize;

import cn.kjky.collector.contract.IngestJson;
import cn.kjky.collector.contract.StandardEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** 各数据源转换器共享的字段别名、时间解析、字段准入和稳定标识算法。 */
final class NormalizationSupport {
    static final List<String> TRACE_IDS = List.of("TraceID", "TraceId", "traceId", "trace_id");
    static final List<String> SPAN_IDS = List.of("SpanID", "SpanId", "spanId", "span_id");
    static final List<String> SERVICE_IDS = List.of(
            "ServiceName", "serviceName", "service_name", "service", "AppName", "appName", "app_name", "Pid", "pid");
    static final List<String> INSTANCE_IDS = List.of(
            "InstanceId", "instanceId", "instance_id", "PodName", "podName", "pod_name",
            "HostName", "hostName", "hostname", "host", "source");
    static final List<String> EVENT_TIMES = List.of(
            "StartTime", "startTime", "start_time", "Timestamp", "timestamp", "eventTime", "event_time",
            "Time", "time", "__time__");
    static final List<String> RECORD_IDS = List.of(
            "RecordId", "recordId", "record_id", "EventId", "eventId", "event_id", "RequestId", "requestId",
            "request_id", "__id__", "_id", "id", "Id", "ID");

    static final List<String> TRACE_PAYLOAD = List.of(
            "TraceID", "TraceId", "traceId", "trace_id", "StartTime", "startTime", "start_time",
            "EndTime", "endTime", "end_time", "Duration", "duration", "ServiceName", "serviceName",
            "OperationName", "operationName", "operation_name", "StatusCode", "statusCode", "status_code",
            "Success", "success", "Error", "error", "RpcType", "rpcType", "protocol");
    static final List<String> SPAN_PAYLOAD = List.of(
            "TraceID", "TraceId", "traceId", "trace_id", "SpanID", "SpanId", "spanId", "span_id",
            "ParentSpanId", "parentSpanId", "parent_span_id", "StartTime", "startTime", "start_time",
            "Duration", "duration", "ServiceName", "serviceName", "OperationName", "operationName",
            "SpanType", "spanType", "span_type", "StatusCode", "statusCode", "status_code",
            "Success", "success", "Error", "error", "Tags", "tags", "Attributes", "attributes");
    static final List<String> METRIC_PAYLOAD = List.of(
            "RecordId", "recordId", "record_id", "Timestamp", "timestamp", "Time", "time",
            "Metric", "metric", "Value", "value", "Values", "values", "Measure", "measure",
            "Measures", "measures", "Dimensions", "dimensions", "Labels", "labels");
    static final List<String> DATASET_PAYLOAD = List.of(
            "RecordId", "recordId", "record_id", "Timestamp", "timestamp", "Time", "time",
            "Value", "value", "Values", "values", "Measure", "measure", "Measures", "measures",
            "Dimensions", "dimensions", "Labels", "labels");
    static final List<String> SLS_PAYLOAD = List.of(
            "message", "method", "path", "uri", "url", "status", "status_code", "duration", "duration_ms",
            "trace_id", "traceId", "span_id", "spanId", "service_id", "serviceName", "instance_id",
            "pod_name", "host", "source", "request_id", "__time__", "__time_ns_part__");

    private static final List<String> SEMANTIC_FIELDS;
    private static final int MAX_UNKNOWN_FIELD_SAMPLES = 20;
    private static final Pattern SAFE_REFERENCE_SEGMENT = Pattern.compile("[A-Za-z0-9._:-]{1,160}");

    static {
        LinkedHashSet<String> names = new LinkedHashSet<>();
        names.addAll(TRACE_IDS);
        names.addAll(SPAN_IDS);
        names.addAll(SERVICE_IDS);
        names.addAll(INSTANCE_IDS);
        names.addAll(EVENT_TIMES);
        names.addAll(RECORD_IDS);
        SEMANTIC_FIELDS = List.copyOf(names);
    }

    private NormalizationSupport() {}

    /** 从根数组或允许的对象路径读取记录；缺少预期数组时失败关闭。 */
    static List<JsonNode> requiredRecords(JsonNode root, List<String> paths, String sourceKind) {
        if (root == null || root.isNull()) throw new NormalizationException("EMPTY_RESPONSE", sourceKind + " 响应为空");
        if (root.isArray()) return arrayValues(root);
        if (!root.isObject()) throw new NormalizationException("INVALID_ROOT", sourceKind + " 响应根节点必须是对象或数组");
        for (String path : paths) {
            JsonNode node = path(root, path);
            if (node == null || node.isMissingNode() || node.isNull()) continue;
            if (!node.isArray()) throw new NormalizationException("INVALID_RECORD_ARRAY", sourceKind + " 记录节点必须是数组: " + path);
            return arrayValues(node);
        }
        throw new NormalizationException("MISSING_RECORD_ARRAY", sourceKind + " 响应缺少预期记录数组");
    }

    private static List<JsonNode> arrayValues(JsonNode node) {
        List<JsonNode> result = new ArrayList<>();
        node.forEach(result::add);
        return result;
    }

    /** 按忽略大小写的点分路径查找节点；优先把完整路径视为顶层字段名。 */
    static JsonNode path(JsonNode root, String candidate) {
        if (root == null || !root.isObject() || candidate == null || candidate.isBlank()) return null;
        JsonNode direct = field(root, candidate);
        if (direct != null) return direct;
        JsonNode current = root;
        for (String segment : candidate.split("\\.")) {
            current = field(current, segment);
            if (current == null) return null;
        }
        return current;
    }

    /** 忽略大小写读取对象的一个直接字段。 */
    static JsonNode field(JsonNode object, String name) {
        if (object == null || !object.isObject()) return null;
        JsonNode exact = object.get(name);
        if (exact != null) return exact;
        Iterator<Map.Entry<String, JsonNode>> fields = object.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            if (entry.getKey().equalsIgnoreCase(name)) return entry.getValue();
        }
        return null;
    }

    /** 返回第一个可转换成非空文本的候选字段。 */
    static String firstText(JsonNode record, List<String> candidates) {
        for (String candidate : candidates) {
            String text = scalarText(path(record, candidate));
            if (text != null && !text.isBlank()) return text.trim();
        }
        return null;
    }

    private static String scalarText(JsonNode node) {
        if (node == null || node.isNull()) return null;
        if (node.isValueNode()) return node.asText();
        if (node.isArray()) {
            for (JsonNode child : node) if (child.isValueNode() && !child.asText().isBlank()) return child.asText();
        }
        return null;
    }

    /** 解析常见时间格式；所有候选失败时稳定回退到查询窗口起点。 */
    static ParsedTime eventTime(JsonNode record, Instant fallback) {
        for (String candidate : EVENT_TIMES) {
            JsonNode node = path(record, candidate);
            Instant parsed = parseTime(node);
            if (parsed != null) return new ParsedTime(parsed, false);
        }
        return new ParsedTime(fallback, true);
    }

    private static Instant parseTime(JsonNode node) {
        if (node == null || node.isNull() || node.isContainerNode()) return null;
        String value = node.asText();
        if (value == null || value.isBlank()) return null;
        try {
            return epoch(Long.parseLong(value.trim()));
        } catch (NumberFormatException ignored) {
            // 非整数时继续尝试标准时间文本。
        } catch (DateTimeException ignored) {
            return null;
        }
        try {
            return Instant.parse(value.trim());
        } catch (DateTimeException ignored) {
            try {
                return OffsetDateTime.parse(value.trim()).toInstant();
            } catch (DateTimeException ignoredAgain) {
                return null;
            }
        }
    }

    private static Instant epoch(long value) {
        long absolute = value == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(value);
        if (absolute < 100_000_000_000L) return Instant.ofEpochSecond(value);
        if (absolute < 100_000_000_000_000L) return Instant.ofEpochMilli(value);
        if (absolute < 100_000_000_000_000_000L) {
            return Instant.ofEpochSecond(Math.floorDiv(value, 1_000_000L), Math.floorMod(value, 1_000_000L) * 1_000L);
        }
        return Instant.ofEpochSecond(Math.floorDiv(value, 1_000_000_000L), Math.floorMod(value, 1_000_000_000L));
    }

    /**
     * 构造一条标准事件。显式稳定标识为空时，使用配置身份字段或规范 payload 回退。
     */
    static StandardEvent event(JsonNode record, NormalizationContext context, Instant explicitTime,
                               String explicitTraceId, String explicitSpanId,
                               List<String> stableParts, List<String> defaultIdentityFields,
                               List<String> defaultPayloadFields, List<NormalizationWarning> warnings) {
        if (record == null || !record.isObject()) {
            throw new NormalizationException("INVALID_RECORD", context.sourceKind() + " 的单条记录必须是对象");
        }

        ParsedTime parsedTime = explicitTime == null ? eventTime(record, context.windowStart()) : new ParsedTime(explicitTime, false);
        String traceId = textOr(explicitTraceId, firstText(record, TRACE_IDS));
        String spanId = textOr(explicitSpanId, firstText(record, SPAN_IDS));
        String serviceId = firstText(record, SERVICE_IDS);
        String instanceId = firstText(record, INSTANCE_IDS);

        PayloadSelection selection = payload(record, context.payloadFields(), defaultPayloadFields);
        List<String> identities = cleanParts(stableParts);
        boolean configuredIdentityMissing = false;
        if (identities.isEmpty()) {
            List<String> identityFields = context.identityFields().isEmpty() ? defaultIdentityFields : context.identityFields();
            identities = identityValues(record, identityFields);
            if (!context.identityFields().isEmpty() && identities.isEmpty()) {
                configuredIdentityMissing = true;
            }
        }

        String eventId = eventId(context, identities, parsedTime.value(), selection.payload());
        String sourceRef = sourceRef(context, identities, eventId);
        if (configuredIdentityMissing) {
            warnings.add(new NormalizationWarning("IDENTITY_FIELDS_MISSING",
                    "配置的身份字段在记录中均不存在，已使用规范内容生成稳定标识", sourceRef,
                    List.copyOf(context.identityFields())));
        }
        if (parsedTime.fallback()) {
            warnings.add(new NormalizationWarning("EVENT_TIME_FALLBACK",
                    "未找到可解析的事件时间，已使用查询窗口起点", sourceRef, EVENT_TIMES));
        }
        if (!selection.unknownFields().isEmpty()) {
            warnings.add(new NormalizationWarning("UNKNOWN_FIELDS_IGNORED",
                    "未准入字段已忽略，告警不包含原始字段值", sourceRef, selection.unknownFields()));
        }

        return StandardEvent.create(eventId, context.recordType(), parsedTime.value(), emptyToNull(traceId),
                emptyToNull(spanId), emptyToNull(serviceId), emptyToNull(instanceId), sourceRef, selection.payload());
    }

    private static PayloadSelection payload(JsonNode record, List<String> configured, List<String> defaults) {
        List<String> allowed = configured == null || configured.isEmpty() ? defaults : configured;
        Set<String> allowedNames = lowercase(allowed);
        Set<String> recognized = lowercase(SEMANTIC_FIELDS);
        recognized.addAll(allowedNames);
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        List<String> unknown = new ArrayList<>();

        record.fields().forEachRemaining(entry -> {
            String normalized = entry.getKey().toLowerCase(Locale.ROOT);
            if (allowedNames.contains(normalized)) payload.set(entry.getKey(), entry.getValue().deepCopy());
            else if (!recognized.contains(normalized) && unknown.size() < MAX_UNKNOWN_FIELD_SAMPLES) unknown.add(entry.getKey());
        });
        if (payload.isEmpty()) payload.put("normalization_status", "NO_ALLOWED_FIELDS");
        return new PayloadSelection(payload, List.copyOf(unknown));
    }

    private static Set<String> lowercase(List<String> values) {
        Set<String> result = new LinkedHashSet<>();
        if (values != null) for (String value : values) {
            if (value != null && !value.isBlank()) result.add(value.trim().toLowerCase(Locale.ROOT));
        }
        return result;
    }

    private static List<String> identityValues(JsonNode record, List<String> fields) {
        List<String> result = new ArrayList<>();
        if (fields == null) return result;
        for (String name : fields) {
            String value = firstText(record, List.of(name));
            if (value != null && !value.isBlank()) result.add(name.toLowerCase(Locale.ROOT) + "=" + value);
        }
        return List.copyOf(result);
    }

    private static List<String> cleanParts(List<String> parts) {
        if (parts == null) return List.of();
        List<String> result = new ArrayList<>();
        for (String part : parts) if (part != null && !part.isBlank()) result.add(part.trim());
        return List.copyOf(result);
    }

    private static String eventId(NormalizationContext context, List<String> identities,
                                  Instant eventTime, ObjectNode payload) {
        ObjectNode seed = JsonNodeFactory.instance.objectNode();
        seed.put("schema_version", "1.0");
        seed.put("source_type", context.sourceType());
        seed.put("source_kind", context.sourceKind());
        seed.put("source_key", context.sourceKey());
        ArrayNode values = seed.putArray("identity");
        identities.forEach(values::add);
        if (identities.isEmpty()) {
            seed.put("event_time", eventTime.toString());
            seed.put("payload_sha256", IngestJson.sha256Canonical(payload));
        }
        return "evt_" + IngestJson.sha256Canonical(seed);
    }

    private static String sourceRef(NormalizationContext context, List<String> identities, String eventId) {
        String identity = identities.isEmpty() ? eventId : String.join(":", identities);
        String referenceIdentity = SAFE_REFERENCE_SEGMENT.matcher(identity).matches()
                ? identity : IngestJson.sha256Canonical(identity);
        return context.sourceType().toLowerCase(Locale.ROOT) + "/"
                + context.sourceKind().toLowerCase(Locale.ROOT) + "/"
                + safeSegment(context.sourceKey()) + "/" + referenceIdentity;
    }

    private static String safeSegment(String value) {
        return SAFE_REFERENCE_SEGMENT.matcher(value).matches() ? value : IngestJson.sha256Canonical(value);
    }

    private static String textOr(String preferred, String fallback) {
        return preferred == null || preferred.isBlank() ? fallback : preferred.trim();
    }

    private static String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    static List<NormalizationWarning> warningList() {
        return new ArrayList<>();
    }

    record ParsedTime(Instant value, boolean fallback) {}
    private record PayloadSelection(ObjectNode payload, List<String> unknownFields) {}
}

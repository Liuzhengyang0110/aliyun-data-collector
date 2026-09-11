package cn.kjky.collector.testbed.cc;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.regex.Pattern;

final class BatchValidator {
    private static final Pattern SHA256 = Pattern.compile("^[0-9a-f]{64}$");
    private static final List<String> BATCH_REQUIRED = List.of(
            "schema_version", "batch_id", "poll_id", "batch_sequence", "is_last_batch",
            "collector_id", "site_id", "collector_type", "source_type", "source_key",
            "window_start", "window_end", "record_count", "content_sha256", "events");
    private static final List<String> EVENT_REQUIRED = List.of(
            "event_id", "record_type", "event_time", "source_ref", "payload_sha256", "payload");

    private final int maxRecords;

    BatchValidator(int maxRecords) {
        this.maxRecords = maxRecords;
    }

    ValidatedBatch validate(byte[] json) throws ContractException {
        final JsonNode root;
        try {
            root = JsonSupport.MAPPER.readTree(json);
        } catch (IOException e) {
            throw new ContractException("MALFORMED_JSON", "request body is not valid JSON");
        }
        if (root == null || !root.isObject()) throw new ContractException("INVALID_ROOT", "batch must be a JSON object");
        requireFields(root, BATCH_REQUIRED, "batch");
        text(root, "schema_version", 3, false);
        if (!"1.0".equals(root.path("schema_version").textValue())) {
            throw new ContractException("UNSUPPORTED_SCHEMA", "schema_version must be 1.0");
        }
        text(root, "batch_id", 128, false);
        text(root, "poll_id", 128, false);
        integer(root, "batch_sequence", 0, Integer.MAX_VALUE);
        bool(root, "is_last_batch");
        text(root, "collector_id", 128, false);
        text(root, "site_id", 128, false);
        text(root, "collector_type", 32, false);
        if (!"ALIYUN".equals(root.path("collector_type").textValue())) {
            throw new ContractException("INVALID_COLLECTOR_TYPE", "collector_type must be ALIYUN");
        }
        text(root, "source_type", 16, false);
        String sourceType = root.path("source_type").textValue();
        if (!"ARMS".equals(sourceType) && !"SLS".equals(sourceType)) {
            throw new ContractException("INVALID_SOURCE_TYPE", "source_type must be ARMS or SLS");
        }
        text(root, "source_key", 512, false);
        Instant start = instant(root, "window_start");
        Instant end = instant(root, "window_end");
        if (!start.isBefore(end)) throw new ContractException("INVALID_WINDOW", "window_start must be before window_end");
        int count = integer(root, "record_count", 1, maxRecords);
        hash(root, "content_sha256");
        JsonNode events = root.path("events");
        if (!events.isArray()) throw new ContractException("INVALID_EVENTS", "events must be an array");
        if (events.isEmpty() || events.size() > maxRecords) {
            throw new ContractException("INVALID_EVENT_COUNT", "events size is outside the configured limit");
        }
        if (events.size() != count) throw new ContractException("RECORD_COUNT_MISMATCH", "record_count does not match events size");
        for (int index = 0; index < events.size(); index++) validateEvent(events.get(index), index);
        String expected = JsonSupport.sha256Canonical(events);
        if (!expected.equals(root.path("content_sha256").textValue())) {
            throw new ContractException("CONTENT_HASH_MISMATCH", "content_sha256 does not match events");
        }
        return new ValidatedBatch(root.deepCopy(), root.path("batch_id").textValue(),
                root.path("collector_id").textValue(), root.path("content_sha256").textValue(), count);
    }

    private void validateEvent(JsonNode event, int index) throws ContractException {
        if (!event.isObject()) throw new ContractException("INVALID_EVENT", "event " + index + " must be an object");
        requireFields(event, EVENT_REQUIRED, "event " + index);
        text(event, "event_id", 128, false);
        text(event, "record_type", 128, false);
        instant(event, "event_time");
        optionalText(event, "trace_id");
        optionalText(event, "span_id");
        optionalText(event, "service_id");
        optionalText(event, "instance_id");
        text(event, "source_ref", 1024, false);
        hash(event, "payload_sha256");
        JsonNode payload = event.path("payload");
        if (!payload.isObject()) throw new ContractException("INVALID_PAYLOAD", "event payload must be an object");
        String expected = JsonSupport.sha256Canonical(payload);
        if (!expected.equals(event.path("payload_sha256").textValue())) {
            throw new ContractException("PAYLOAD_HASH_MISMATCH", "payload_sha256 does not match payload for event " + index);
        }
    }

    private static void requireFields(JsonNode object, List<String> fields, String label) throws ContractException {
        for (String field : fields) {
            if (!object.has(field) || object.get(field).isNull()) {
                throw new ContractException("MISSING_FIELD", label + " is missing " + field);
            }
        }
    }

    private static String text(JsonNode object, String field, int maximum, boolean allowEmpty) throws ContractException {
        JsonNode value = object.get(field);
        if (value == null || !value.isTextual() || (!allowEmpty && value.textValue().isBlank()) || value.textValue().length() > maximum) {
            throw new ContractException("INVALID_FIELD", field + " must be a valid string");
        }
        return value.textValue();
    }

    private static void optionalText(JsonNode object, String field) throws ContractException {
        if (object.has(field) && !object.get(field).isNull() && !object.get(field).isTextual()) {
            throw new ContractException("INVALID_FIELD", field + " must be a string when present");
        }
    }

    private static int integer(JsonNode object, String field, int minimum, int maximum) throws ContractException {
        JsonNode value = object.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToInt()
                || value.intValue() < minimum || value.intValue() > maximum) {
            throw new ContractException("INVALID_FIELD", field + " is outside its allowed integer range");
        }
        return value.intValue();
    }

    private static void bool(JsonNode object, String field) throws ContractException {
        if (!object.get(field).isBoolean()) throw new ContractException("INVALID_FIELD", field + " must be boolean");
    }

    private static Instant instant(JsonNode object, String field) throws ContractException {
        String value = text(object, field, 64, false);
        try {
            Instant parsed = Instant.parse(value);
            if (!value.endsWith("Z")) throw new DateTimeParseException("UTC Z required", value, value.length());
            return parsed;
        } catch (DateTimeParseException e) {
            throw new ContractException("INVALID_TIME", field + " must be RFC 3339 UTC");
        }
    }

    private static void hash(JsonNode object, String field) throws ContractException {
        JsonNode value = object.get(field);
        if (value == null || !value.isTextual() || !SHA256.matcher(value.textValue()).matches()) {
            throw new ContractException("INVALID_HASH", field + " must be 64 lowercase hexadecimal characters");
        }
    }

    record ValidatedBatch(JsonNode json, String batchId, String collectorId, String contentSha256, int recordCount) {
    }

    static final class ContractException extends Exception {
        private final String reasonCode;

        ContractException(String reasonCode, String message) {
            super(message);
            this.reasonCode = reasonCode;
        }

        String reasonCode() {
            return reasonCode;
        }
    }
}

package cn.kjky.collector.testbed.arms;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

/** Thread-safe, request-counted failure/data scenario selector. */
final class ArmsScenarioController {
    enum Name { NORMAL, EMPTY, PAGINATED, DUPLICATE_RECORDS, RATE_LIMITED, SERVER_ERROR, DELAY, MALFORMED_JSON, INVALID_SCHEMA }

    record Scenario(Name name, int remaining, long delayMillis, long retryAfterSeconds,
                    String targetAction, Integer pageNumber, String traceId) {
        static Scenario normal() { return new Scenario(Name.NORMAL, Integer.MAX_VALUE, 0, 1, null, null, null); }
        boolean matches(String action, Integer page, String trace) {
            return (targetAction == null || targetAction.equalsIgnoreCase(action))
                    && (pageNumber == null || pageNumber.equals(page))
                    && (traceId == null || traceId.equals(trace));
        }
    }

    private final AtomicReference<Scenario> current = new AtomicReference<>(Scenario.normal());

    Scenario configure(JsonNode node) {
        if (node == null || !node.isObject()) throw new IllegalArgumentException("scenario body must be a JSON object");
        String raw = requiredText(node, "scenario");
        Name name;
        try { name = Name.valueOf(raw.toUpperCase(Locale.ROOT)); }
        catch (IllegalArgumentException e) { throw new IllegalArgumentException("unknown scenario: " + raw); }
        if (name == Name.NORMAL) return reset();
        int times = node.path("times").asInt(1);
        long delay = node.path("delay_millis").asLong(name == Name.DELAY ? 1000 : 0);
        long retryAfter = node.path("retry_after_seconds").asLong(1);
        if (times < 1 || delay < 0 || retryAfter < 0) throw new IllegalArgumentException("times must be positive and delays non-negative");
        Scenario configured = new Scenario(name, times, delay, retryAfter,
                text(node, "target_action"), node.has("page_number") ? node.get("page_number").asInt() : null,
                text(node, "trace_id"));
        current.set(configured);
        return configured;
    }

    Scenario consume(String action, Integer page, String trace) {
        while (true) {
            Scenario value = current.get();
            if (value.name == Name.NORMAL || !value.matches(action, page, trace)) return Scenario.normal();
            Scenario next = value.remaining <= 1 ? Scenario.normal()
                    : new Scenario(value.name, value.remaining - 1, value.delayMillis, value.retryAfterSeconds,
                    value.targetAction, value.pageNumber, value.traceId);
            if (current.compareAndSet(value, next)) return value;
        }
    }

    Scenario reset() { Scenario value = Scenario.normal(); current.set(value); return value; }
    Scenario snapshot() { return current.get(); }

    private static String requiredText(JsonNode node, String name) {
        String value = text(node, name);
        if (value == null) throw new IllegalArgumentException(name + " is required");
        return value;
    }

    private static String text(JsonNode node, String name) {
        JsonNode value = node.get(name);
        return value == null || value.isNull() || value.asText().isBlank() ? null : value.asText();
    }
}

package cn.kjky.collector.testbed.cc;

import com.fasterxml.jackson.databind.JsonNode;

final class ScenarioController {
    enum Scenario {
        NORMAL,
        ALREADY_COMMITTED,
        BAD_REQUEST,
        UNAUTHORIZED,
        FORBIDDEN,
        CONFLICT,
        PAYLOAD_TOO_LARGE,
        RATE_LIMITED,
        SERVICE_UNAVAILABLE,
        DELAY,
        DROP_BEFORE_COMMIT,
        DROP_AFTER_COMMIT,
        MALFORMED_RECEIPT,
        WRONG_BATCH_ID,
        WRONG_CONTENT_HASH
    }

    record Active(Scenario scenario, int remainingRequests, long retryAfterSeconds, long delayMillis) {
    }

    private Active active = new Active(Scenario.NORMAL, 0, 1, 0);

    synchronized Active consume() {
        Active selected = active;
        if (selected.scenario() != Scenario.NORMAL) {
            int remaining = selected.remainingRequests() - 1;
            active = remaining <= 0
                    ? new Active(Scenario.NORMAL, 0, 1, 0)
                    : new Active(selected.scenario(), remaining, selected.retryAfterSeconds(), selected.delayMillis());
        }
        return selected;
    }

    synchronized Active current() {
        return active;
    }

    synchronized Active configure(JsonNode body) {
        if (body == null || !body.isObject()) throw new IllegalArgumentException("scenario body must be a JSON object");
        String raw = body.path("scenario").asText("");
        final Scenario scenario;
        try {
            scenario = Scenario.valueOf(raw);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown scenario: " + raw);
        }
        if (scenario == Scenario.NORMAL) {
            active = new Active(Scenario.NORMAL, 0, 1, 0);
            return active;
        }
        int times = body.path("times").asInt(1);
        long retryAfter = body.path("retry_after_seconds").asLong(1);
        long delay = body.path("delay_millis").asLong(0);
        if (times < 1 || times > 1_000_000) throw new IllegalArgumentException("times must be in 1..1000000");
        if (retryAfter < 0 || retryAfter > 86400) throw new IllegalArgumentException("retry_after_seconds must be in 0..86400");
        if (delay < 0 || delay > 300_000) throw new IllegalArgumentException("delay_millis must be in 0..300000");
        active = new Active(scenario, times, retryAfter, delay);
        return active;
    }

    synchronized void reset() {
        active = new Active(Scenario.NORMAL, 0, 1, 0);
    }
}

package cn.kjky.collector.testbed.cc;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** Writes one synthetic, canonical pending batch for the phase-one black-box upload smoke test. */
public final class OutboxFixtureApplication {
    private OutboxFixtureApplication() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> options = options(args);
        Path dataRoot = Path.of(required(options, "data-root")).toAbsolutePath().normalize();
        String batchId = options.getOrDefault("batch-id", "testbed-upload-batch-001");
        String collectorId = options.getOrDefault("collector-id", "testbed-collector-01");
        String now = Instant.now().toString();

        ObjectNode payload = JsonSupport.MAPPER.createObjectNode();
        payload.put("message", "synthetic testbed event");
        payload.put("sequence", 1);
        ObjectNode event = JsonSupport.MAPPER.createObjectNode();
        event.put("event_id", "testbed-event-001");
        event.put("event_time", "2026-09-02T08:00:03Z");
        event.put("instance_id", "testbed-instance-01");
        event.set("payload", payload);
        event.put("payload_sha256", JsonSupport.sha256Canonical(payload));
        event.put("record_type", "testbed_record");
        event.put("service_id", "testbed-service");
        event.put("source_ref", "testbed/phase-one/001");
        ArrayNode events = JsonSupport.MAPPER.createArrayNode().add(event);

        ObjectNode batch = JsonSupport.MAPPER.createObjectNode();
        batch.put("batch_id", batchId);
        batch.put("batch_sequence", 0);
        batch.put("collector_id", collectorId);
        batch.put("collector_type", "ALIYUN");
        batch.put("content_sha256", JsonSupport.sha256Canonical(events));
        batch.set("events", events);
        batch.put("is_last_batch", true);
        batch.put("poll_id", "testbed-upload-poll-001");
        batch.put("record_count", 1);
        batch.put("schema_version", "1.0");
        batch.put("site_id", "testbed-site");
        batch.put("source_key", "testbed/phase-one");
        batch.put("source_type", "ARMS");
        batch.put("window_end", "2026-09-02T08:00:30Z");
        batch.put("window_start", "2026-09-02T08:00:00Z");
        byte[] batchBytes = JsonSupport.canonicalBytes(batch);

        ObjectNode descriptor = JsonSupport.MAPPER.createObjectNode();
        descriptor.put("outbox_schema_version", "1.0");
        descriptor.put("batch_schema_version", "1.0");
        descriptor.put("batch_id", batchId);
        descriptor.put("poll_id", "testbed-upload-poll-001");
        descriptor.put("batch_sequence", 0);
        descriptor.put("last_batch", true);
        descriptor.put("collector_id", collectorId);
        descriptor.put("source_type", "ARMS");
        descriptor.put("source_key", "testbed/phase-one");
        descriptor.put("window_start", "2026-09-02T08:00:00Z");
        descriptor.put("window_end", "2026-09-02T08:00:30Z");
        descriptor.put("record_count", 1);
        descriptor.put("content_sha256", batch.path("content_sha256").asText());
        descriptor.put("serialized_sha256", JsonSupport.sha256(batchBytes));
        descriptor.put("created_at", now);

        ObjectNode delivery = JsonSupport.MAPPER.createObjectNode();
        delivery.put("schema_version", "1.0");
        delivery.put("batch_id", batchId);
        delivery.put("state", "PENDING");
        delivery.put("attempt_count", 0);
        delivery.put("created_at", now);
        delivery.put("updated_at", now);
        delivery.putArray("child_batch_ids");
        delivery.putArray("committed_child_batch_ids");

        Path pendingRoot = dataRoot.resolve("outbox").resolve("pending");
        Path writingRoot = dataRoot.resolve("outbox").resolve("writing");
        Files.createDirectories(pendingRoot);
        Files.createDirectories(writingRoot);
        Files.createDirectories(dataRoot.resolve("outbox").resolve("retry"));
        Files.createDirectories(dataRoot.resolve("state"));
        String key = "batch-" + JsonSupport.sha256(batchId.getBytes(StandardCharsets.UTF_8));
        Path temporary = writingRoot.resolve(key + ".partial");
        Path target = pendingRoot.resolve(key);
        if (Files.exists(target) || Files.exists(temporary)) throw new IllegalStateException("fixture batch already exists");
        Files.createDirectory(temporary);
        Files.write(temporary.resolve("batch.json"), batchBytes);
        Files.write(temporary.resolve("outbox.json"), JsonSupport.write(descriptor));
        Files.write(temporary.resolve("delivery.json"), JsonSupport.write(delivery));
        Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
        System.out.println(batchId);
    }

    private static Map<String, String> options(String[] args) {
        Map<String, String> result = new LinkedHashMap<>();
        for (int index = 0; index < args.length; index += 2) {
            if (!args[index].startsWith("--") || index + 1 >= args.length) throw new IllegalArgumentException("expected --name value arguments");
            result.put(args[index].substring(2), args[index + 1]);
        }
        return result;
    }

    private static String required(Map<String, String> options, String name) {
        String value = options.get(name);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("--" + name + " is required");
        return value;
    }
}

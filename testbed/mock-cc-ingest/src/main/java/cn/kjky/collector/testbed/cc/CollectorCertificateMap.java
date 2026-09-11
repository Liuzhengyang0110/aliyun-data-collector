package cn.kjky.collector.testbed.cc;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

final class CollectorCertificateMap {
    private final Map<String, String> fingerprints;

    private CollectorCertificateMap(Map<String, String> fingerprints) {
        this.fingerprints = Map.copyOf(fingerprints);
    }

    static CollectorCertificateMap load(Path path) throws IOException {
        if (path == null) return new CollectorCertificateMap(Map.of());
        JsonNode root = JsonSupport.MAPPER.readTree(Files.readAllBytes(path));
        if (root == null || !root.isObject()) throw new IOException("collector certificate map must be a JSON object");
        Map<String, String> values = new LinkedHashMap<>();
        root.fields().forEachRemaining(entry -> {
            if (!entry.getValue().isTextual()) throw new IllegalArgumentException("certificate fingerprint must be text");
            values.put(entry.getKey(), normalize(entry.getValue().textValue()));
        });
        return new CollectorCertificateMap(values);
    }

    boolean isConfigured() {
        return !fingerprints.isEmpty();
    }

    boolean matches(String collectorId, String fingerprint) {
        String expected = fingerprints.get(collectorId);
        return expected != null && expected.equals(normalize(fingerprint));
    }

    private static String normalize(String value) {
        return value.replace(":", "").replace(" ", "").toLowerCase(Locale.ROOT);
    }
}

package cn.kjky.collector.testbed.cc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MockCcIngestServerTest {
    static Path root;

    static Path serverStore;
    static Path serverTrust;
    static Path clientStore;
    static Path clientTwoStore;
    static Path clientTrust;
    static Path expiredClient;
    static Path untrustedClient;
    static Path wrongHostnameServer;
    static Path untrustedServer;
    static Path otherServerTrust;
    static TestPki trustedPki;

    @BeforeAll
    static void createPki() throws Exception {
        root = Path.of("target", "test-work", java.util.UUID.randomUUID().toString()).toAbsolutePath().normalize();
        Files.createDirectories(root);
        trustedPki = TestPki.create(root.resolve("trusted"), "testbed-trusted-ca");
        serverStore = trustedPki.issueServer("server", "mock-cc-ingest",
                "dns:localhost,dns:mock-cc-ingest,ip:127.0.0.1");
        wrongHostnameServer = trustedPki.issueServer("wrong-host-server", "wrong-host", "dns:wrong.invalid");
        clientStore = trustedPki.issueClient("collector-one", "collector-one");
        clientTwoStore = trustedPki.issueClient("collector-two", "collector-two");
        expiredClient = trustedPki.issueExpiredClient("expired-client", "expired-client");
        serverTrust = trustedPki.trustStore("trusted-clients");
        clientTrust = trustedPki.trustStore("trusted-servers");

        TestPki untrusted = TestPki.create(root.resolve("untrusted"), "testbed-untrusted-ca");
        untrustedClient = untrusted.issueClient("untrusted-client", "untrusted-client");
        untrustedServer = untrusted.issueServer("untrusted-server", "localhost", "dns:localhost");
        otherServerTrust = untrusted.trustStore("untrusted-server-trust");
    }

    @Test
    void acceptsGzipCommitsAndReturnsAlreadyCommitted() throws Exception {
        Path state = root.resolve("commit-state");
        try (MockCcIngestServer server = start(state, serverStore, serverTrust, null, 1000, 2 * 1024 * 1024, 10 * 1024 * 1024)) {
            HttpClient client = client(clientStore, clientTrust);
            ObjectNode batch = batch("batch-commit", "collector-one", "hello");

            HttpResponse<byte[]> first = post(client, server, batch);
            HttpResponse<byte[]> second = post(client, server, batch);

            assertThat(first.statusCode()).isEqualTo(200);
            assertThat(json(first).path("status").asText()).isEqualTo("COMMITTED");
            assertThat(json(first).path("committed_at").asText()).isNotBlank();
            assertThat(json(second).path("status").asText()).isEqualTo("ALREADY_COMMITTED");
            JsonNode status = json(get(client, server, "/__admin/status"));
            assertThat(status.path("received_batches").asInt()).isEqualTo(1);
            assertThat(status.path("received_events").asInt()).isEqualTo(1);
            assertThat(status.path("duplicate_submissions").asInt()).isEqualTo(1);
        }
    }

    @Test
    void rejectsSchemaRecordCountAndBothHashMismatchesAtomically() throws Exception {
        Path state = root.resolve("validation-state");
        try (MockCcIngestServer server = start(state, serverStore, serverTrust, null, 1000, 2 * 1024 * 1024, 10 * 1024 * 1024)) {
            HttpClient client = client(clientStore, clientTrust);
            ObjectNode schema = batch("bad-schema", "collector-one", "a");
            schema.put("schema_version", "2.0");
            ObjectNode count = batch("bad-count", "collector-one", "b");
            count.put("record_count", 2);
            ObjectNode payloadHash = batch("bad-payload-hash", "collector-one", "c");
            ((ObjectNode) payloadHash.path("events").get(0)).put("payload_sha256", "0".repeat(64));
            payloadHash.put("content_sha256", JsonSupport.sha256Canonical(payloadHash.path("events")));
            ObjectNode contentHash = batch("bad-content-hash", "collector-one", "d");
            contentHash.put("content_sha256", "0".repeat(64));

            assertThat(post(client, server, schema).statusCode()).isEqualTo(400);
            assertThat(post(client, server, count).statusCode()).isEqualTo(400);
            assertThat(post(client, server, payloadHash).statusCode()).isEqualTo(400);
            assertThat(post(client, server, contentHash).statusCode()).isEqualTo(400);
            assertThat(json(get(client, server, "/__admin/received")).isEmpty()).isTrue();
        }
    }

    @Test
    void returnsConflictForSameBatchIdWithDifferentContentWithoutOverwrite() throws Exception {
        Path state = root.resolve("conflict-state");
        try (MockCcIngestServer server = start(state, serverStore, serverTrust, null, 1000, 2 * 1024 * 1024, 10 * 1024 * 1024)) {
            HttpClient client = client(clientStore, clientTrust);
            assertThat(post(client, server, batch("same-id", "collector-one", "first")).statusCode()).isEqualTo(200);
            HttpResponse<byte[]> conflict = post(client, server, batch("same-id", "collector-one", "different"));
            assertThat(conflict.statusCode()).isEqualTo(409);
            JsonNode stored = json(get(client, server, "/__admin/received?include_payload=true")).get(0);
            assertThat(stored.path("batch").path("events").get(0).path("payload").path("message").asText()).isEqualTo("first");
        }
    }

    @Test
    void enforcesCompressedAndUncompressedRequestLimitsWith413() throws Exception {
        Path state = root.resolve("size-state");
        try (MockCcIngestServer server = start(state, serverStore, serverTrust, null, 1000, 80, 300)) {
            HttpClient client = client(clientStore, clientTrust);
            HttpResponse<byte[]> compressedTooLarge = post(client, server, batch("large-compressed", "collector-one", "x".repeat(2000)));
            assertThat(compressedTooLarge.statusCode()).isEqualTo(413);
        }
        try (MockCcIngestServer server = start(root.resolve("inflate-state"), serverStore, serverTrust, null, 1000, 4096, 300)) {
            HttpClient client = client(clientStore, clientTrust);
            HttpResponse<byte[]> inflatedTooLarge = post(client, server, batch("large-inflated", "collector-one", "x".repeat(2000)));
            assertThat(inflatedTooLarge.statusCode()).isEqualTo(413);
        }
    }

    @Test
    void injects429And503ForRequestedCountsThenRecovers() throws Exception {
        try (MockCcIngestServer server = start(root.resolve("fault-state"), serverStore, serverTrust, null, 1000, 2 * 1024 * 1024, 10 * 1024 * 1024)) {
            HttpClient client = client(clientStore, clientTrust);
            configureScenario(client, server, "{\"scenario\":\"RATE_LIMITED\",\"times\":2,\"retry_after_seconds\":7}");
            ObjectNode batch = batch("rate-limited", "collector-one", "rate");
            HttpResponse<byte[]> first = post(client, server, batch);
            HttpResponse<byte[]> second = post(client, server, batch);
            HttpResponse<byte[]> recovered = post(client, server, batch);
            assertThat(first.statusCode()).isEqualTo(429);
            assertThat(first.headers().firstValue("Retry-After")).contains("7");
            assertThat(second.statusCode()).isEqualTo(429);
            assertThat(json(recovered).path("status").asText()).isEqualTo("COMMITTED");

            configureScenario(client, server, "{\"scenario\":\"SERVICE_UNAVAILABLE\",\"times\":1}");
            assertThat(post(client, server, batch("unavailable", "collector-one", "down")).statusCode()).isEqualTo(503);
            assertThat(post(client, server, batch("unavailable", "collector-one", "down")).statusCode()).isEqualTo(200);
        }
    }

    @Test
    void dropAfterCommitConvergesWithSameBatchIdAndAlreadyCommitted() throws Exception {
        try (MockCcIngestServer server = start(root.resolve("drop-state"), serverStore, serverTrust, null, 1000, 2 * 1024 * 1024, 10 * 1024 * 1024)) {
            HttpClient client = client(clientStore, clientTrust);
            configureScenario(client, server, "{\"scenario\":\"DROP_AFTER_COMMIT\",\"times\":1}");
            ObjectNode batch = batch("lost-receipt", "collector-one", "once");
            assertThatThrownBy(() -> post(client, server, batch)).isInstanceOf(IOException.class);
            HttpResponse<byte[]> retry = post(client, server, batch);
            assertThat(json(retry).path("status").asText()).isEqualTo("ALREADY_COMMITTED");
            assertThat(json(get(client, server, "/__admin/status")).path("dropped_after_commit").asInt()).isEqualTo(1);
        }
    }

    @Test
    void persistsIdempotencyAcrossServerRestart() throws Exception {
        Path state = root.resolve("restart-state");
        ObjectNode batch = batch("restart-batch", "collector-one", "persistent");
        String committedAt;
        try (MockCcIngestServer first = start(state, serverStore, serverTrust, null, 1000, 2 * 1024 * 1024, 10 * 1024 * 1024)) {
            committedAt = json(post(client(clientStore, clientTrust), first, batch)).path("committed_at").asText();
        }
        try (MockCcIngestServer second = start(state, serverStore, serverTrust, null, 1000, 2 * 1024 * 1024, 10 * 1024 * 1024)) {
            JsonNode receipt = json(post(client(clientStore, clientTrust), second, batch));
            assertThat(receipt.path("status").asText()).isEqualTo("ALREADY_COMMITTED");
            assertThat(receipt.path("committed_at").asText()).isEqualTo(committedAt);
        }
    }

    @Test
    void requiresTrustedValidClientCertificateAndTrustedMatchingServer() throws Exception {
        try (MockCcIngestServer server = start(root.resolve("mtls-state"), serverStore, serverTrust, null, 1000, 2 * 1024 * 1024, 10 * 1024 * 1024)) {
            ObjectNode batch = batch("mtls-batch", "collector-one", "secure");
            assertThat(post(client(clientStore, clientTrust), server, batch).statusCode()).isEqualTo(200);
            assertThatThrownBy(() -> post(client(null, clientTrust), server, batch)).isInstanceOf(IOException.class);
            assertThatThrownBy(() -> post(client(untrustedClient, clientTrust), server, batch)).isInstanceOf(IOException.class);
            assertThatThrownBy(() -> post(client(expiredClient, clientTrust), server, batch)).isInstanceOf(IOException.class);
            assertThatThrownBy(() -> post(client(clientStore, otherServerTrust), server, batch)).isInstanceOf(IOException.class);
        }
        try (MockCcIngestServer wrongHost = start(root.resolve("wrong-host-state"), wrongHostnameServer, serverTrust, null, 1000, 2 * 1024 * 1024, 10 * 1024 * 1024)) {
            assertThatThrownBy(() -> post(client(clientStore, clientTrust), wrongHost,
                    batch("wrong-host", "collector-one", "host"))).isInstanceOf(IOException.class);
        }
        try (MockCcIngestServer badServer = start(root.resolve("bad-server-state"), untrustedServer, serverTrust, null, 1000, 2 * 1024 * 1024, 10 * 1024 * 1024)) {
            assertThatThrownBy(() -> post(client(clientStore, clientTrust), badServer,
                    batch("bad-server", "collector-one", "trust"))).isInstanceOf(IOException.class);
        }
    }

    @Test
    void enforcesCollectorFingerprintMappingAndSupportsCertificateRotation() throws Exception {
        Path mapping = root.resolve("collector-map.json");
        String first = trustedPki.fingerprint(clientStore, "collector-one");
        Files.writeString(mapping, JsonSupport.MAPPER.writeValueAsString(Map.of("collector-one", first)), StandardCharsets.UTF_8);
        try (MockCcIngestServer server = start(root.resolve("mapping-state"), serverStore, serverTrust, mapping, 1000, 2 * 1024 * 1024, 10 * 1024 * 1024)) {
            assertThat(post(client(clientStore, clientTrust), server, batch("mapped", "collector-one", "one")).statusCode()).isEqualTo(200);
            assertThat(post(client(clientTwoStore, clientTrust), server, batch("mismatch", "collector-one", "two")).statusCode()).isEqualTo(403);
        }

        String rotated = trustedPki.fingerprint(clientTwoStore, "collector-two");
        Files.writeString(mapping, JsonSupport.MAPPER.writeValueAsString(Map.of("collector-one", rotated)), StandardCharsets.UTF_8);
        try (MockCcIngestServer server = start(root.resolve("rotation-state"), serverStore, serverTrust, mapping, 1000, 2 * 1024 * 1024, 10 * 1024 * 1024)) {
            assertThat(post(client(clientStore, clientTrust), server, batch("old-cert", "collector-one", "old")).statusCode()).isEqualTo(403);
            assertThat(post(client(clientTwoStore, clientTrust), server, batch("new-cert", "collector-one", "new")).statusCode()).isEqualTo(200);
        }
    }

    @Test
    void supportsReceiptCorruptionDelayDropBeforeAndExplicitResetSemantics() throws Exception {
        try (MockCcIngestServer server = start(root.resolve("remaining-faults-state"), serverStore, serverTrust, null, 1000, 2 * 1024 * 1024, 10 * 1024 * 1024)) {
            HttpClient client = client(clientStore, clientTrust);
            configureScenario(client, server, "{\"scenario\":\"MALFORMED_RECEIPT\",\"times\":1}");
            assertThat(new String(post(client, server, batch("malformed", "collector-one", "m")).body(), StandardCharsets.UTF_8)).isEqualTo("{not-json");
            configureScenario(client, server, "{\"scenario\":\"WRONG_BATCH_ID\",\"times\":1}");
            assertThat(json(post(client, server, batch("wrong-id", "collector-one", "w"))).path("batch_id").asText()).isEqualTo("wrong-wrong-id");
            configureScenario(client, server, "{\"scenario\":\"WRONG_CONTENT_HASH\",\"times\":1}");
            assertThat(json(post(client, server, batch("wrong-hash", "collector-one", "h"))).path("content_sha256").asText()).isEqualTo("0".repeat(64));
            configureScenario(client, server, "{\"scenario\":\"DELAY\",\"times\":1,\"delay_millis\":100}");
            long start = System.nanoTime();
            assertThat(post(client, server, batch("delayed", "collector-one", "d")).statusCode()).isEqualTo(200);
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(80));
            configureScenario(client, server, "{\"scenario\":\"DROP_BEFORE_COMMIT\",\"times\":1}");
            assertThatThrownBy(() -> post(client, server, batch("drop-before", "collector-one", "x"))).isInstanceOf(IOException.class);

            assertThat(postRaw(client, server, "/__admin/reset", "", "application/json").statusCode()).isEqualTo(400);
            assertThat(postRaw(client, server, "/__admin/reset?delete_committed=false", "", "application/json").statusCode()).isEqualTo(200);
            assertThat(json(get(client, server, "/__admin/received")).size()).isEqualTo(4);
            assertThat(postRaw(client, server, "/__admin/reset?delete_committed=true", "", "application/json").statusCode()).isEqualTo(200);
            assertThat(json(get(client, server, "/__admin/received")).isEmpty()).isTrue();
        }
    }

    private static MockCcIngestServer start(Path state, Path keys, Path trust, Path mapping,
                                            int records, long compressed, long request) throws Exception {
        ServerConfig config = new ServerConfig(java.net.InetAddress.getByName("127.0.0.1"), 0, state,
                keys, TestPki.PASSWORD, trust, TestPki.PASSWORD, mapping, records, compressed, request);
        MockCcIngestServer server = new MockCcIngestServer(config);
        server.start();
        return server;
    }

    private static HttpClient client(Path identity, Path trustStore) throws Exception {
        KeyStore trust = load(trustStore);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trust);
        javax.net.ssl.KeyManager[] managers = null;
        if (identity != null) {
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(load(identity), TestPki.PASSWORD.toCharArray());
            managers = kmf.getKeyManagers();
        }
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(managers, tmf.getTrustManagers(), null);
        return HttpClient.newBuilder().sslContext(context).connectTimeout(Duration.ofSeconds(2)).build();
    }

    private static KeyStore load(Path path) throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (var input = Files.newInputStream(path)) {
            store.load(input, TestPki.PASSWORD.toCharArray());
        }
        return store;
    }

    private static ObjectNode batch(String batchId, String collectorId, String message) {
        ObjectNode payload = JsonSupport.MAPPER.createObjectNode().put("message", message).put("sequence", 1);
        ObjectNode event = JsonSupport.MAPPER.createObjectNode();
        event.put("event_id", "event-" + batchId);
        event.put("record_type", "flow_record");
        event.put("event_time", "2026-09-02T08:00:03Z");
        event.put("service_id", "orders");
        event.put("instance_id", "pod-test-1");
        event.put("source_ref", "sls/test/logstore/" + batchId);
        event.put("payload_sha256", JsonSupport.sha256Canonical(payload));
        event.set("payload", payload);
        ArrayNode events = JsonSupport.MAPPER.createArrayNode().add(event);
        ObjectNode batch = JsonSupport.MAPPER.createObjectNode();
        batch.put("schema_version", "1.0");
        batch.put("batch_id", batchId);
        batch.put("poll_id", "poll-" + batchId);
        batch.put("batch_sequence", 0);
        batch.put("is_last_batch", true);
        batch.put("collector_id", collectorId);
        batch.put("site_id", "test-site");
        batch.put("collector_type", "ALIYUN");
        batch.put("source_type", "SLS");
        batch.put("source_key", "test-project/test-logstore");
        batch.put("window_start", "2026-09-02T08:00:00Z");
        batch.put("window_end", "2026-09-02T08:00:30Z");
        batch.put("record_count", 1);
        batch.put("content_sha256", JsonSupport.sha256Canonical(events));
        batch.set("events", events);
        return batch;
    }

    private static HttpResponse<byte[]> post(HttpClient client, MockCcIngestServer server, JsonNode batch)
            throws Exception {
        byte[] gzip = gzip(JsonSupport.canonicalBytes(batch));
        HttpRequest request = HttpRequest.newBuilder(uri(server, MockCcIngestServer.INGEST_PATH))
                .timeout(Duration.ofSeconds(3))
                .header("Content-Type", "application/json; charset=utf-8")
                .header("Content-Encoding", "gzip")
                .POST(HttpRequest.BodyPublishers.ofByteArray(gzip)).build();
        return client.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    private static HttpResponse<byte[]> get(HttpClient client, MockCcIngestServer server, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(uri(server, path)).timeout(Duration.ofSeconds(3)).GET().build();
        return client.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    private static void configureScenario(HttpClient client, MockCcIngestServer server, String body) throws Exception {
        HttpResponse<byte[]> response = postRaw(client, server, "/__admin/scenario", body, "application/json");
        assertThat(response.statusCode()).isEqualTo(200);
    }

    private static HttpResponse<byte[]> postRaw(HttpClient client, MockCcIngestServer server, String path,
                                                String body, String contentType) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(uri(server, path)).timeout(Duration.ofSeconds(3))
                .header("Content-Type", contentType)
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        return client.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    private static URI uri(MockCcIngestServer server, String path) {
        return URI.create("https://localhost:" + server.port() + path);
    }

    private static JsonNode json(HttpResponse<byte[]> response) throws IOException {
        return JsonSupport.MAPPER.readTree(response.body());
    }

    private static byte[] gzip(byte[] bytes) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(output)) {
            gzip.write(bytes);
        }
        return output.toByteArray();
    }
}

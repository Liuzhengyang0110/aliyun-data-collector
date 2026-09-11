package cn.kjky.collector.testbed.cc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsExchange;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

public final class MockCcIngestServer implements AutoCloseable {
    public static final String INGEST_PATH = "/collector/v1/events:batch";

    private final ServerConfig config;
    private final HttpsServer server;
    private final ExecutorService executor;
    private final BatchValidator validator;
    private final BatchStore store;
    private final CollectorCertificateMap certificateMap;
    private final ScenarioController scenarios = new ScenarioController();
    private final AtomicLong requestCount = new AtomicLong();
    private final AtomicLong duplicateCount = new AtomicLong();
    private final AtomicLong conflictCount = new AtomicLong();
    private final AtomicLong droppedBeforeCount = new AtomicLong();
    private final AtomicLong droppedAfterCount = new AtomicLong();

    public MockCcIngestServer(ServerConfig config) throws Exception {
        this(config, Clock.systemUTC());
    }

    MockCcIngestServer(ServerConfig config, Clock clock) throws Exception {
        this.config = config;
        this.validator = new BatchValidator(config.maxRecords());
        this.store = new BatchStore(config.stateDirectory(), clock);
        this.certificateMap = CollectorCertificateMap.load(config.collectorCertificateMap());
        this.server = HttpsServer.create(new InetSocketAddress(config.bindAddress(), config.port()), 0);
        SSLContext sslContext = sslContext(config);
        this.server.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
            @Override
            public void configure(com.sun.net.httpserver.HttpsParameters parameters) {
                SSLParameters ssl = getSSLContext().getDefaultSSLParameters();
                ssl.setNeedClientAuth(true);
                ssl.setProtocols(new String[]{"TLSv1.3", "TLSv1.2"});
                parameters.setSSLParameters(ssl);
            }
        });
        this.executor = Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors()), runnable -> {
            Thread thread = new Thread(runnable, "mock-cc-ingest-worker");
            thread.setDaemon(true);
            return thread;
        });
        this.server.setExecutor(executor);
        this.server.createContext(INGEST_PATH, this::handleIngest);
        this.server.createContext("/__admin/health", exchange -> handleAdmin(exchange, "health"));
        this.server.createContext("/__admin/status", exchange -> handleAdmin(exchange, "status"));
        this.server.createContext("/__admin/received", exchange -> handleAdmin(exchange, "received"));
        this.server.createContext("/__admin/reset", exchange -> handleAdmin(exchange, "reset"));
        this.server.createContext("/__admin/scenario", exchange -> handleAdmin(exchange, "scenario"));
    }

    public void start() {
        server.start();
        System.out.println("mock-cc-ingest listening on https://" + config.bindAddress().getHostAddress() + ":" + port());
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private void handleIngest(HttpExchange exchange) {
        try {
            if (!INGEST_PATH.equals(exchange.getRequestURI().getPath())) {
                sendError(exchange, 404, null, null, "NOT_FOUND", "unknown path");
                return;
            }
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "POST");
                sendError(exchange, 405, null, null, "METHOD_NOT_ALLOWED", "POST is required");
                return;
            }
            ClientCertificateIdentity identity = peerIdentity(exchange);
            logPeer(identity);
            requestCount.incrementAndGet();
            String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            String encoding = exchange.getRequestHeaders().getFirst("Content-Encoding");
            if (contentType == null || !"application/json".equalsIgnoreCase(contentType.split(";", 2)[0].trim())) {
                sendError(exchange, 400, null, null, "INVALID_CONTENT_TYPE", "Content-Type must be application/json");
                return;
            }
            if (encoding == null || !"gzip".equalsIgnoreCase(encoding.trim())) {
                sendError(exchange, 400, null, null, "INVALID_CONTENT_ENCODING", "Content-Encoding must be gzip");
                return;
            }
            long declaredLength = declaredContentLength(exchange.getRequestHeaders());
            if (declaredLength > config.maxCompressedBytes()) {
                exchange.getRequestBody().close();
                sendError(exchange, 413, null, null, "PAYLOAD_TOO_LARGE", "request exceeds configured size limit");
                return;
            }
            byte[] compressed = readLimited(exchange.getRequestBody(), config.maxCompressedBytes());
            byte[] json;
            try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
                json = readLimited(gzip, config.maxRequestBytes());
            } catch (TooLargeException e) {
                throw e;
            } catch (IOException e) {
                sendError(exchange, 400, null, null, "INVALID_GZIP", "request body is not valid gzip");
                return;
            }
            BatchValidator.ValidatedBatch batch;
            try {
                batch = validator.validate(json);
            } catch (BatchValidator.ContractException e) {
                sendError(exchange, 400, null, null, e.reasonCode(), e.getMessage());
                return;
            }
            if (certificateMap.isConfigured() && !certificateMap.matches(batch.collectorId(), identity.fingerprintSha256())) {
                sendError(exchange, 403, batch.batchId(), batch.contentSha256(), "COLLECTOR_CERTIFICATE_MISMATCH",
                        "collector_id is not bound to this client certificate");
                return;
            }

            ScenarioController.Active selected = scenarios.consume();
            applyScenario(exchange, batch, identity, selected);
        } catch (TooLargeException e) {
            safeError(exchange, 413, "PAYLOAD_TOO_LARGE", "request exceeds configured size limit");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            closeWithoutResponse(exchange);
        } catch (Exception e) {
            System.err.println("REQUEST_FAILED type=" + e.getClass().getSimpleName());
            safeError(exchange, 500, "INTERNAL_ERROR", "mock service failed to process request");
        } finally {
            exchange.close();
        }
    }

    private void applyScenario(HttpExchange exchange, BatchValidator.ValidatedBatch batch,
                               ClientCertificateIdentity identity, ScenarioController.Active selected)
            throws IOException, InterruptedException {
        switch (selected.scenario()) {
            case BAD_REQUEST -> sendError(exchange, 400, batch.batchId(), batch.contentSha256(), "INJECTED_BAD_REQUEST", "injected failure");
            case UNAUTHORIZED -> sendError(exchange, 401, batch.batchId(), batch.contentSha256(), "INJECTED_UNAUTHORIZED", "injected failure");
            case FORBIDDEN -> sendError(exchange, 403, batch.batchId(), batch.contentSha256(), "INJECTED_FORBIDDEN", "injected failure");
            case CONFLICT -> sendError(exchange, 409, batch.batchId(), batch.contentSha256(), "INJECTED_CONFLICT", "injected failure");
            case PAYLOAD_TOO_LARGE -> sendError(exchange, 413, batch.batchId(), batch.contentSha256(), "INJECTED_PAYLOAD_TOO_LARGE", "injected failure");
            case RATE_LIMITED -> {
                exchange.getResponseHeaders().set("Retry-After", Long.toString(selected.retryAfterSeconds()));
                sendError(exchange, 429, batch.batchId(), batch.contentSha256(), "INJECTED_RATE_LIMITED", "injected failure");
            }
            case SERVICE_UNAVAILABLE -> sendError(exchange, 503, batch.batchId(), batch.contentSha256(), "INJECTED_SERVICE_UNAVAILABLE", "injected failure");
            case DROP_BEFORE_COMMIT -> {
                droppedBeforeCount.incrementAndGet();
                closeWithoutResponse(exchange);
            }
            case DELAY -> {
                Thread.sleep(selected.delayMillis());
                commitAndReceipt(exchange, batch, identity, null);
            }
            case ALREADY_COMMITTED -> commitAndReceipt(exchange, batch, identity, "ALREADY_COMMITTED");
            case DROP_AFTER_COMMIT -> {
                BatchStore.CommitResult result = store.commit(batch, identity);
                if (result.type() == BatchStore.ResultType.CONFLICT) {
                    conflictCount.incrementAndGet();
                    sendError(exchange, 409, batch.batchId(), batch.contentSha256(), "BATCH_ID_CONFLICT", "batch_id already has different content");
                } else {
                    if (result.type() == BatchStore.ResultType.ALREADY_COMMITTED) duplicateCount.incrementAndGet();
                    droppedAfterCount.incrementAndGet();
                    closeWithoutResponse(exchange);
                }
            }
            case MALFORMED_RECEIPT -> {
                BatchStore.CommitResult result = store.commit(batch, identity);
                if (result.type() == BatchStore.ResultType.CONFLICT) {
                    sendError(exchange, 409, batch.batchId(), batch.contentSha256(), "BATCH_ID_CONFLICT", "batch_id already has different content");
                } else {
                    sendRaw(exchange, 200, "{not-json".getBytes(StandardCharsets.UTF_8), "application/json");
                }
            }
            case WRONG_BATCH_ID -> {
                BatchStore.CommitResult result = store.commit(batch, identity);
                if (result.type() == BatchStore.ResultType.CONFLICT) sendConflict(exchange, batch);
                else sendReceipt(exchange, batch, result, "wrong-" + batch.batchId(), batch.contentSha256(), null);
            }
            case WRONG_CONTENT_HASH -> {
                BatchStore.CommitResult result = store.commit(batch, identity);
                if (result.type() == BatchStore.ResultType.CONFLICT) sendConflict(exchange, batch);
                else sendReceipt(exchange, batch, result, batch.batchId(), "0".repeat(64), null);
            }
            case NORMAL -> commitAndReceipt(exchange, batch, identity, null);
        }
    }

    private void commitAndReceipt(HttpExchange exchange, BatchValidator.ValidatedBatch batch,
                                  ClientCertificateIdentity identity, String forcedStatus) throws IOException {
        BatchStore.CommitResult result = store.commit(batch, identity);
        if (result.type() == BatchStore.ResultType.CONFLICT) {
            conflictCount.incrementAndGet();
            sendConflict(exchange, batch);
            return;
        }
        if (result.type() == BatchStore.ResultType.ALREADY_COMMITTED) duplicateCount.incrementAndGet();
        sendReceipt(exchange, batch, result, batch.batchId(), batch.contentSha256(), forcedStatus);
    }

    private void sendConflict(HttpExchange exchange, BatchValidator.ValidatedBatch batch) throws IOException {
        sendError(exchange, 409, batch.batchId(), batch.contentSha256(), "BATCH_ID_CONFLICT",
                "batch_id already has different content");
    }

    private void sendReceipt(HttpExchange exchange, BatchValidator.ValidatedBatch batch, BatchStore.CommitResult result,
                             String responseBatchId, String responseHash, String forcedStatus) throws IOException {
        String status = forcedStatus != null ? forcedStatus
                : result.type() == BatchStore.ResultType.COMMITTED ? "COMMITTED" : "ALREADY_COMMITTED";
        ObjectNode receipt = JsonSupport.MAPPER.createObjectNode();
        receipt.put("schema_version", "1.0");
        receipt.put("batch_id", responseBatchId);
        receipt.put("content_sha256", responseHash);
        receipt.put("status", status);
        receipt.put("committed_at", result.committedAt().toString());
        sendJson(exchange, 200, receipt);
    }

    private void handleAdmin(HttpExchange exchange, String operation) {
        try {
            ClientCertificateIdentity identity = peerIdentity(exchange);
            logPeer(identity);
            switch (operation) {
                case "health" -> {
                    requireMethod(exchange, "GET");
                    sendJson(exchange, 200, Map.of("status", "UP", "schema_version", "1.0"));
                }
                case "status" -> {
                    requireMethod(exchange, "GET");
                    BatchStore.Counts counts = store.counts();
                    ScenarioController.Active active = scenarios.current();
                    ObjectNode body = JsonSupport.MAPPER.createObjectNode();
                    body.put("status", "UP");
                    body.put("scenario", active.scenario().name());
                    body.put("scenario_remaining_requests", active.remainingRequests());
                    body.put("request_count", requestCount.get());
                    body.put("received_batches", counts.batches());
                    body.put("received_events", counts.events());
                    body.put("duplicate_submissions", duplicateCount.get());
                    body.put("conflicts", conflictCount.get());
                    body.put("dropped_before_commit", droppedBeforeCount.get());
                    body.put("dropped_after_commit", droppedAfterCount.get());
                    sendJson(exchange, 200, body);
                }
                case "received" -> {
                    requireMethod(exchange, "GET");
                    boolean include = query(exchange.getRequestURI()).getOrDefault("include_payload", "false").equalsIgnoreCase("true");
                    sendJson(exchange, 200, store.list(include));
                }
                case "reset" -> {
                    requireMethod(exchange, "POST");
                    Map<String, String> query = query(exchange.getRequestURI());
                    if (!query.containsKey("delete_committed")) {
                        sendError(exchange, 400, null, null, "EXPLICIT_DELETE_PARAMETER_REQUIRED",
                                "delete_committed=true or false is required");
                        return;
                    }
                    boolean delete = Boolean.parseBoolean(query.get("delete_committed"));
                    scenarios.reset();
                    resetCounters();
                    if (delete) store.deleteCommitted();
                    sendJson(exchange, 200, Map.of("status", "RESET", "deleted_committed", delete));
                }
                case "scenario" -> {
                    requireMethod(exchange, "POST");
                    byte[] bytes = readLimited(exchange.getRequestBody(), 64 * 1024);
                    ScenarioController.Active active = scenarios.configure(JsonSupport.MAPPER.readTree(bytes));
                    sendJson(exchange, 200, Map.of(
                            "scenario", active.scenario().name(),
                            "remaining_requests", active.remainingRequests(),
                            "retry_after_seconds", active.retryAfterSeconds(),
                            "delay_millis", active.delayMillis()));
                }
                default -> sendError(exchange, 404, null, null, "NOT_FOUND", "unknown admin path");
            }
        } catch (MethodNotAllowedException e) {
            exchange.getResponseHeaders().set("Allow", e.expected);
            safeError(exchange, 405, "METHOD_NOT_ALLOWED", e.expected + " is required");
        } catch (IllegalArgumentException e) {
            safeError(exchange, 400, "INVALID_ADMIN_REQUEST", e.getMessage());
        } catch (Exception e) {
            System.err.println("ADMIN_REQUEST_FAILED type=" + e.getClass().getSimpleName());
            safeError(exchange, 500, "INTERNAL_ERROR", "mock service failed to process admin request");
        } finally {
            exchange.close();
        }
    }

    private static ClientCertificateIdentity peerIdentity(HttpExchange exchange) throws Exception {
        X509Certificate certificate = (X509Certificate) ((HttpsExchange) exchange)
                .getSSLSession().getPeerCertificates()[0];
        return ClientCertificateIdentity.from(certificate);
    }

    private static void logPeer(ClientCertificateIdentity identity) {
        System.out.println("CLIENT_CERT fingerprint_sha256=" + identity.fingerprintSha256()
                + " subject=" + identity.subjectSummary() + " not_after=" + identity.notAfter());
    }

    private static SSLContext sslContext(ServerConfig config) throws Exception {
        KeyStore keys = loadStore(config.keyStorePath(), config.keyStorePassword());
        KeyStore trust = loadStore(config.trustStorePath(), config.trustStorePassword());
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keys, config.keyStorePassword().toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trust);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        return context;
    }

    private static KeyStore loadStore(java.nio.file.Path path, String password) throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(path)) {
            store.load(input, password.toCharArray());
        }
        return store;
    }

    private static byte[] readLimited(InputStream input, long maximum) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream((int) Math.min(maximum, 8192));
        byte[] buffer = new byte[8192];
        long total = 0;
        int read;
        while ((read = input.read(buffer)) != -1) {
            total += read;
            if (total > maximum) throw new TooLargeException();
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }

    private static long declaredContentLength(Headers headers) {
        String value = headers.getFirst("Content-Length");
        if (value == null) return -1;
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ignored) {
            return -1;
        }
    }

    private static void sendError(HttpExchange exchange, int status, String batchId, String hash,
                                  String reason, String message) throws IOException {
        ObjectNode body = JsonSupport.MAPPER.createObjectNode();
        body.put("schema_version", "1.0");
        body.put("batch_id", batchId == null ? "" : batchId);
        body.put("content_sha256", hash == null ? "0".repeat(64) : hash);
        body.put("status", statusName(status));
        body.put("reason_code", reason);
        body.put("message", message);
        sendJson(exchange, status, body);
    }

    private static String statusName(int status) {
        return switch (status) {
            case 401 -> "UNAUTHORIZED";
            case 403 -> "FORBIDDEN";
            case 409 -> "CONFLICT";
            case 413 -> "PAYLOAD_TOO_LARGE";
            case 429 -> "RATE_LIMITED";
            case 500, 503 -> "SERVICE_UNAVAILABLE";
            default -> "SCHEMA_ERROR";
        };
    }

    private static void sendJson(HttpExchange exchange, int status, Object body) throws IOException {
        sendRaw(exchange, status, JsonSupport.write(body), "application/json; charset=utf-8");
    }

    private static void sendRaw(HttpExchange exchange, int status, byte[] body, String contentType) throws IOException {
        Headers headers = exchange.getResponseHeaders();
        headers.set("Content-Type", contentType);
        headers.set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
    }

    private static void closeWithoutResponse(HttpExchange exchange) {
        try {
            exchange.getRequestBody().close();
        } catch (IOException ignored) {
        }
        exchange.close();
    }

    private static Map<String, String> query(URI uri) {
        if (uri.getRawQuery() == null || uri.getRawQuery().isBlank()) return Map.of();
        java.util.LinkedHashMap<String, String> result = new java.util.LinkedHashMap<>();
        for (String part : uri.getRawQuery().split("&")) {
            String[] pieces = part.split("=", 2);
            result.put(decode(pieces[0]), pieces.length == 2 ? decode(pieces[1]) : "");
        }
        return result;
    }

    private static String decode(String value) {
        return java.net.URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private static void requireMethod(HttpExchange exchange, String expected) throws MethodNotAllowedException {
        if (!expected.equals(exchange.getRequestMethod())) throw new MethodNotAllowedException(expected);
    }

    private static void safeError(HttpExchange exchange, int status, String reason, String message) {
        try {
            sendError(exchange, status, null, null, reason, message);
        } catch (IOException ignored) {
            exchange.close();
        }
    }

    private void resetCounters() {
        requestCount.set(0);
        duplicateCount.set(0);
        conflictCount.set(0);
        droppedBeforeCount.set(0);
        droppedAfterCount.set(0);
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }

    private static final class TooLargeException extends IOException {
    }

    private static final class MethodNotAllowedException extends Exception {
        private final String expected;

        private MethodNotAllowedException(String expected) {
            this.expected = expected;
        }
    }
}

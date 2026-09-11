package cn.kjky.collector.testbed.arms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/** Local HTTP server compatible with the collector's current ARMS RPC and Dataset calls. */
public final class MockArmsServer implements AutoCloseable {
    private static final Set<String> RPC_ACTIONS = Set.of(
            "ListTraceApps", "SearchTracesByPage", "GetTrace", "QueryMetric", "QueryMetricByPage");
    private static final int MAX_BODY_BYTES = 1024 * 1024;

    private final MockArmsConfig config;
    private final ObjectMapper json = new ObjectMapper();
    private final ArmsScenarioController scenarios = new ArmsScenarioController();
    private final RequestJournal journal = new RequestJournal();
    private final AtomicLong sequence = new AtomicLong();
    private final HttpServer server;
    private final ExecutorService executor;

    public MockArmsServer(MockArmsConfig config) throws IOException {
        this.config = config;
        this.server = HttpServer.create(config.address(), 0);
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread thread = new Thread(r, "mock-arms-http");
            thread.setDaemon(true);
            return thread;
        });
        server.setExecutor(executor);
        server.createContext("/__admin/health", this::health);
        server.createContext("/__admin/requests", this::requests);
        server.createContext("/__admin/reset", this::reset);
        server.createContext("/__admin/scenario", this::scenario);
        server.createContext("/QueryDataset", this::dataset);
        server.createContext("/", this::rpc);
    }

    public void start() { server.start(); }
    public int port() { return server.getAddress().getPort(); }

    private void health(HttpExchange exchange) throws IOException {
        if (!method(exchange, "GET")) return;
        ObjectNode body = json.createObjectNode().put("status", "UP").put("service", "mock-arms");
        body.set("scenario", scenarioJson(scenarios.snapshot()));
        sendJson(exchange, 200, body, requestId());
    }

    private void requests(HttpExchange exchange) throws IOException {
        if (!method(exchange, "GET")) return;
        List<RequestJournal.Entry> entries = journal.snapshot();
        ObjectNode body = json.createObjectNode().put("request_count", entries.size());
        Map<String, Long> actionCounts = new LinkedHashMap<>();
        entries.forEach(entry -> actionCounts.merge(entry.action() == null ? "UNKNOWN" : entry.action(), 1L, Long::sum));
        body.set("action_counts", json.valueToTree(actionCounts));
        body.set("requests", json.valueToTree(entries));
        body.set("scenario", scenarioJson(scenarios.snapshot()));
        sendJson(exchange, 200, body, requestId());
    }

    private void reset(HttpExchange exchange) throws IOException {
        if (!method(exchange, "POST")) return;
        scenarios.reset();
        journal.clear();
        sendJson(exchange, 200, json.createObjectNode().put("status", "RESET"), requestId());
    }

    private void scenario(HttpExchange exchange) throws IOException {
        if (!method(exchange, "POST")) return;
        try {
            byte[] body = readLimited(exchange);
            ArmsScenarioController.Scenario configured = scenarios.configure(json.readTree(body));
            ObjectNode result = json.createObjectNode().put("status", "CONFIGURED");
            result.set("scenario", scenarioJson(configured));
            sendJson(exchange, 200, result, requestId());
        } catch (Exception e) {
            sendError(exchange, 400, "InvalidScenario", e.getMessage(), requestId());
        }
    }

    private void rpc(HttpExchange exchange) throws IOException {
        String id = requestId();
        if (!method(exchange, "POST")) return;
        Map<String, String> parameters;
        try { parameters = parameters(exchange); }
        catch (IllegalArgumentException e) { sendError(exchange, 400, "InvalidParameter", e.getMessage(), id); return; }
        String action = parameter(parameters, "Action");
        journal.add(exchange.getRequestMethod(), exchange.getRequestURI().getPath(), action, parameters);
        if (action == null || action.isBlank()) { sendError(exchange, 400, "MissingAction", "Action is required", id); return; }
        if (!RPC_ACTIONS.contains(action)) { sendError(exchange, 404, "UnknownAction", "Action is not supported", id); return; }
        if (parameter(parameters, "Version") == null || parameter(parameters, "RegionId") == null) {
            sendError(exchange, 400, "MissingCommonParameter", "Version and RegionId are required", id); return;
        }
        if (config.expectedAccessKeyId() != null
                && !config.expectedAccessKeyId().equals(parameter(parameters, "AccessKeyId"))) {
            sendError(exchange, 403, "InvalidAccessKeyId", "test AccessKeyId does not match", id); return;
        }
        Integer page = integerOrNull(parameter(parameters, "PageNumber"));
        String trace = first(parameters, "TraceID", "TraceId", "traceId");
        ArmsScenarioController.Scenario scenario = scenarios.consume(action, page, trace);
        if (applyFailure(exchange, scenario, id)) return;
        try {
            JsonNode body = switch (action) {
                case "ListTraceApps" -> listTraceApps(parameters, id, scenario.name());
                case "SearchTracesByPage" -> searchTraces(parameters, id, scenario.name());
                case "GetTrace" -> getTrace(parameters, id, scenario.name());
                case "QueryMetric" -> queryMetric(parameters, id, scenario.name(), false);
                case "QueryMetricByPage" -> queryMetric(parameters, id, scenario.name(), true);
                default -> throw new IllegalStateException(action);
            };
            sendJson(exchange, 200, body, id);
        } catch (IllegalArgumentException e) {
            sendError(exchange, 400, "InvalidParameter", e.getMessage(), id);
        }
    }

    private void dataset(HttpExchange exchange) throws IOException {
        String id = requestId();
        if (!method(exchange, "POST")) return;
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        if (contentType == null || !contentType.toLowerCase(Locale.ROOT).startsWith("application/x-www-form-urlencoded")) {
            sendError(exchange, 400, "InvalidContentType", "application/x-www-form-urlencoded is required", id); return;
        }
        Map<String, String> parameters;
        try { parameters = parameters(exchange); }
        catch (IllegalArgumentException e) { sendError(exchange, 400, "InvalidParameter", e.getMessage(), id); return; }
        journal.add(exchange.getRequestMethod(), exchange.getRequestURI().getPath(), "QueryDataset", parameters);
        if (!datasetAuthorized(exchange)) { sendError(exchange, 401, "Unauthorized", "test Basic authentication failed", id); return; }
        for (String required : List.of("datasetId", "minTime", "maxTime", "intervalInSec", "measures", "_userId")) {
            if (parameter(parameters, required) == null) { sendError(exchange, 400, "MissingParameter", required + " is required", id); return; }
        }
        ArmsScenarioController.Scenario scenario = scenarios.consume("QueryDataset", null, null);
        if (applyFailure(exchange, scenario, id)) return;
        try { sendJson(exchange, 200, queryDataset(parameters, id, scenario.name()), id); }
        catch (IllegalArgumentException e) { sendError(exchange, 400, "InvalidParameter", e.getMessage(), id); }
    }

    private boolean applyFailure(HttpExchange exchange, ArmsScenarioController.Scenario scenario, String id) throws IOException {
        if (scenario.delayMillis() > 0 || scenario.name() == ArmsScenarioController.Name.DELAY) {
            try { Thread.sleep(scenario.delayMillis()); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException("interrupted"); }
        }
        return switch (scenario.name()) {
            case RATE_LIMITED -> {
                exchange.getResponseHeaders().set("Retry-After", Long.toString(scenario.retryAfterSeconds()));
                sendError(exchange, 429, "Throttling", "synthetic rate limit", id); yield true;
            }
            case SERVER_ERROR -> { sendError(exchange, 500, "InternalError", "synthetic server error", id); yield true; }
            case MALFORMED_JSON -> { send(exchange, 200, "application/json", "{not-json", id); yield true; }
            case INVALID_SCHEMA -> { sendJson(exchange, 200, json.createObjectNode().put("unexpected", true), id); yield true; }
            default -> false;
        };
    }

    private JsonNode listTraceApps(Map<String, String> parameters, String id, ArmsScenarioController.Name scenario) {
        ObjectNode root = json.createObjectNode().put("RequestId", id);
        ArrayNode apps = root.putArray("TraceApps");
        if (scenario != ArmsScenarioController.Name.EMPTY) {
            apps.addObject().put("Pid", "testbed-pid").put("AppName", "testbed-app")
                    .put("RegionId", parameter(parameters, "RegionId"));
        }
        return root;
    }

    private JsonNode searchTraces(Map<String, String> p, String id, ArmsScenarioController.Name scenario) {
        long start = requiredLong(p, "StartTime");
        long end = requiredLong(p, "EndTime");
        if (end < start) throw new IllegalArgumentException("EndTime must not precede StartTime");
        int page = positiveInt(p, "PageNumber", 1);
        int size = positiveInt(p, "PageSize", 100);
        int total = scenario == ArmsScenarioController.Name.EMPTY ? 0
                : scenario == ArmsScenarioController.Name.PAGINATED ? size * 2 + 1 : 3;
        List<ObjectNode> records = traceRecords(start, end, total);
        List<ObjectNode> selected = page(records, page, size);
        if (scenario == ArmsScenarioController.Name.DUPLICATE_RECORDS && !selected.isEmpty()) selected.add(selected.get(0).deepCopy());
        ObjectNode pageBean = json.createObjectNode().put("PageNumber", page).put("PageSize", size).put("Total", total);
        ArrayNode output = pageBean.putArray("TraceInfos");
        selected.forEach(output::add);
        return json.createObjectNode().put("RequestId", id).set("PageBean", pageBean);
    }

    private List<ObjectNode> traceRecords(long start, long end, int total) {
        List<ObjectNode> result = new ArrayList<>();
        long width = Math.max(1, end - start + 1);
        for (int i = 0; i < total; i++) {
            long timestamp = start + Math.min(width - 1, (width * (i + 1)) / (total + 1));
            String traceId = "trace-" + shortHash(start + ":" + end + ":" + i);
            result.add(json.createObjectNode().put("TraceID", traceId).put("StartTime", timestamp)
                    .put("ServiceName", i % 2 == 0 ? "order-service" : "payment-service")
                    .put("service_id", i % 2 == 0 ? "order-service" : "payment-service")
                    .put("instance_id", "testbed-instance-" + (i % 2 + 1))
                    .put("OperationName", i % 2 == 0 ? "POST /orders" : "POST /pay")
                    .put("Duration", 10 + i).put("StatusCode", 200));
        }
        return result;
    }

    private JsonNode getTrace(Map<String, String> p, String id, ArmsScenarioController.Name scenario) {
        String traceId = first(p, "TraceID", "TraceId", "traceId");
        if (traceId == null) throw new IllegalArgumentException("TraceID is required");
        ObjectNode root = json.createObjectNode().put("RequestId", id).put("TraceId", traceId);
        ArrayNode spans = root.putObject("Data").putArray("Spans");
        if (scenario == ArmsScenarioController.Name.EMPTY || traceId.startsWith("missing-")) return root;
        int count = scenario == ArmsScenarioController.Name.DUPLICATE_RECORDS ? 3 : 2;
        for (int i = 0; i < count; i++) {
            int stableIndex = i == 2 ? 0 : i;
            spans.addObject().put("TraceID", traceId).put("SpanId", "span-" + shortHash(traceId + ":" + stableIndex))
                    .put("timestamp", 1_700_000_000_000L + Math.floorMod(traceId.hashCode(), 1_000_000) + stableIndex)
                    .put("serviceName", stableIndex == 0 ? "order-service" : "payment-service")
                    .put("service_id", stableIndex == 0 ? "order-service" : "payment-service")
                    .put("instanceId", "testbed-instance-" + (stableIndex + 1))
                    .put("instance_id", "testbed-instance-" + (stableIndex + 1))
                    .put("operationName", stableIndex == 0 ? "POST /orders" : "POST /pay")
                    .put("duration", 5 + stableIndex).put("status_code", 200);
        }
        return root;
    }

    private JsonNode queryMetric(Map<String, String> p, String id, ArmsScenarioController.Name scenario, boolean paged) {
        long start = requiredLong(p, "StartTime");
        long end = requiredLong(p, "EndTime");
        int interval = positiveInt(p, "IntervalInSec", 60);
        String metric = require(p, "Metric");
        List<ObjectNode> all = scenario == ArmsScenarioController.Name.EMPTY ? new ArrayList<>() : metricRecords(start, end, interval, metric);
        if (scenario == ArmsScenarioController.Name.DUPLICATE_RECORDS && !all.isEmpty()) all.add(all.get(0).deepCopy());
        ObjectNode root = json.createObjectNode().put("RequestId", id);
        if (!paged) {
            ArrayNode data = root.putArray("Data"); all.forEach(data::add);
        } else {
            int pageNumber = positiveInt(p, "PageNumber", 1);
            int pageSize = positiveInt(p, "PageSize", Math.max(1, positiveInt(p, "Limit", 100)));
            ObjectNode bean = root.putObject("PageBean").put("PageNumber", pageNumber).put("PageSize", pageSize).put("Total", all.size());
            ArrayNode records = bean.putArray("Records"); page(all, pageNumber, pageSize).forEach(records::add);
        }
        return root;
    }

    private List<ObjectNode> metricRecords(long start, long end, int intervalSeconds, String metric) {
        if (end < start) throw new IllegalArgumentException("EndTime must not precede StartTime");
        long step = Math.multiplyExact(intervalSeconds, 1000L);
        List<ObjectNode> result = new ArrayList<>();
        for (long time = start, index = 0; time <= end && result.size() < 1000; time += step, index++) {
            result.add(json.createObjectNode().put("timestamp", time).put("metric", metric)
                    .put("serviceName", "order-service").put("service_id", "order-service")
                    .put("instance_id", "testbed-instance-1").put("value", deterministicValue(metric, time, index))
                    .set("dimensions", json.createObjectNode().put("zone", "test-zone-a")));
            if (Long.MAX_VALUE - step < time) break;
        }
        return result;
    }

    private JsonNode queryDataset(Map<String, String> p, String id, ArmsScenarioController.Name scenario) {
        long start = requiredLong(p, "minTime");
        long end = requiredLong(p, "maxTime");
        int interval = positiveInt(p, "intervalInSec", 60);
        String datasetId = require(p, "datasetId");
        if (end < start) throw new IllegalArgumentException("maxTime must not precede minTime");
        int maximum = scenario == ArmsScenarioController.Name.PAGINATED ? 500 : 100;
        ArrayNode rows = json.createArrayNode();
        if (scenario != ArmsScenarioController.Name.EMPTY) {
            long step = Math.multiplyExact(interval, 1000L);
            for (long time = start, index = 0; time <= end && rows.size() < maximum; time += step, index++) {
                rows.addObject().put("timestamp", time).put("dataset_id", datasetId)
                        .put("service_id", "inventory-service").put("instance_id", "inventory-pod-1")
                        .put("value", deterministicValue(datasetId, time, index))
                        .set("dimensions", json.createObjectNode().put("cluster", "test-cluster-a"));
                if (Long.MAX_VALUE - step < time) break;
            }
            if (scenario == ArmsScenarioController.Name.DUPLICATE_RECORDS && !rows.isEmpty()) rows.add(rows.get(0).deepCopy());
        }
        return json.createObjectNode().put("requestId", id).set("rows", rows);
    }

    private boolean datasetAuthorized(HttpExchange exchange) {
        if (config.datasetUsername() == null && config.datasetPassword() == null) return true;
        String header = exchange.getRequestHeaders().getFirst("Authorization");
        if (header == null || !header.regionMatches(true, 0, "Basic ", 0, 6)) return false;
        String expected = config.datasetUsername() + ":" + (config.datasetPassword() == null ? "" : config.datasetPassword());
        try {
            byte[] actual = Base64.getDecoder().decode(header.substring(6));
            return MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8), actual);
        } catch (IllegalArgumentException e) { return false; }
    }

    private Map<String, String> parameters(HttpExchange exchange) throws IOException {
        Map<String, String> result = new LinkedHashMap<>();
        decodeInto(result, exchange.getRequestURI().getRawQuery());
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        if (contentType != null && contentType.toLowerCase(Locale.ROOT).startsWith("application/x-www-form-urlencoded")) {
            decodeInto(result, new String(readLimited(exchange), StandardCharsets.UTF_8));
        } else if (exchange.getRequestBody().read() != -1) {
            throw new IllegalArgumentException("only form request bodies are supported");
        }
        return result;
    }

    private byte[] readLimited(HttpExchange exchange) throws IOException {
        byte[] bytes = exchange.getRequestBody().readNBytes(MAX_BODY_BYTES + 1);
        if (bytes.length > MAX_BODY_BYTES) throw new IllegalArgumentException("request body is too large");
        return bytes;
    }

    private static void decodeInto(Map<String, String> target, String encoded) {
        if (encoded == null || encoded.isBlank()) return;
        for (String pair : encoded.split("&")) {
            if (pair.isBlank()) continue;
            int equals = pair.indexOf('=');
            String key = decode(equals < 0 ? pair : pair.substring(0, equals));
            String value = decode(equals < 0 ? "" : pair.substring(equals + 1));
            target.put(key, value);
        }
    }

    private static String decode(String value) { return URLDecoder.decode(value, StandardCharsets.UTF_8); }
    private String requestId() { return "mock-arms-" + String.format(Locale.ROOT, "%08d", sequence.incrementAndGet()); }

    private void sendError(HttpExchange exchange, int status, String code, String message, String id) throws IOException {
        sendJson(exchange, status, json.createObjectNode().put("RequestId", id).put("Code", code).put("Message", message), id);
    }

    private void sendJson(HttpExchange exchange, int status, JsonNode body, String id) throws IOException {
        send(exchange, status, "application/json; charset=UTF-8", json.writeValueAsString(body), id);
    }

    private void send(HttpExchange exchange, int status, String type, String body, String id) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        Headers headers = exchange.getResponseHeaders();
        headers.set("Content-Type", type);
        headers.set("x-acs-request-id", id);
        headers.set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private boolean method(HttpExchange exchange, String expected) throws IOException {
        if (expected.equalsIgnoreCase(exchange.getRequestMethod())) return true;
        exchange.getResponseHeaders().set("Allow", expected);
        sendError(exchange, 405, "MethodNotAllowed", expected + " is required", requestId());
        return false;
    }

    private ObjectNode scenarioJson(ArmsScenarioController.Scenario value) {
        ObjectNode node = json.createObjectNode().put("name", value.name().name())
                .put("remaining", value.name() == ArmsScenarioController.Name.NORMAL ? 0 : value.remaining())
                .put("delay_millis", value.delayMillis()).put("retry_after_seconds", value.retryAfterSeconds());
        if (value.targetAction() != null) node.put("target_action", value.targetAction());
        if (value.pageNumber() != null) node.put("page_number", value.pageNumber());
        if (value.traceId() != null) node.put("trace_id", value.traceId());
        return node;
    }

    private static String parameter(Map<String, String> values, String name) {
        return values.entrySet().stream().filter(e -> e.getKey().equalsIgnoreCase(name)).map(Map.Entry::getValue).findFirst().orElse(null);
    }
    private static String first(Map<String, String> values, String... names) {
        for (String name : names) { String value = parameter(values, name); if (value != null) return value; }
        return null;
    }
    private static String require(Map<String, String> values, String name) {
        String value = parameter(values, name); if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " is required"); return value;
    }
    private static long requiredLong(Map<String, String> values, String name) {
        try { return Long.parseLong(require(values, name)); }
        catch (NumberFormatException e) { throw new IllegalArgumentException(name + " must be an integer"); }
    }
    private static Integer integerOrNull(String value) {
        if (value == null || value.isBlank()) return null;
        try { return Integer.valueOf(value); } catch (NumberFormatException e) { return null; }
    }
    private static int positiveInt(Map<String, String> values, String name, int fallback) {
        String value = parameter(values, name);
        int parsed;
        try { parsed = value == null || value.isBlank() ? fallback : Integer.parseInt(value); }
        catch (NumberFormatException e) { throw new IllegalArgumentException(name + " must be an integer"); }
        if (parsed < 1) throw new IllegalArgumentException(name + " must be positive");
        return parsed;
    }
    private static <T> List<T> page(List<T> values, int page, int size) {
        long fromLong = (long) (page - 1) * size;
        if (fromLong >= values.size()) return new ArrayList<>();
        int from = (int) fromLong;
        return new ArrayList<>(values.subList(from, Math.min(values.size(), from + size)));
    }
    private static String shortHash(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            return java.util.HexFormat.of().formatHex(digest, 0, 8);
        } catch (Exception e) { throw new IllegalStateException(e); }
    }
    private static double deterministicValue(String identity, long time, long index) {
        return Math.floorMod((identity + ':' + time + ':' + index).hashCode(), 10000) / 100.0;
    }

    @Override public void close() { server.stop(0); executor.shutdownNow(); }
}

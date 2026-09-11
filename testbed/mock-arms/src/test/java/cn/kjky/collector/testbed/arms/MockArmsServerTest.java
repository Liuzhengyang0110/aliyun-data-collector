package cn.kjky.collector.testbed.arms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MockArmsServerTest {
    private final ObjectMapper json = new ObjectMapper();
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private MockArmsServer server;
    private String base;

    @BeforeEach void start() throws Exception {
        server = new MockArmsServer(new MockArmsConfig("127.0.0.1", 0, "testbed-access-key-id", "testbed-user", "testbed-password"));
        server.start();
        base = "http://127.0.0.1:" + server.port();
    }

    @AfterEach void stop() { server.close(); }

    @Test void routesEveryRpcActionAndReturnsRequestId() throws Exception {
        HttpResponse<String> apps = rpc("ListTraceApps", Map.of());
        HttpResponse<String> search = rpc("SearchTracesByPage", window(Map.of("PageNumber", "1", "PageSize", "10")));
        String traceId = json.readTree(search.body()).at("/PageBean/TraceInfos/0/TraceID").asText();
        HttpResponse<String> detail = rpc("GetTrace", Map.of("TraceID", traceId));
        HttpResponse<String> metric = rpc("QueryMetric", window(Map.of("Metric", "service_qps", "IntervalInSec", "10")));
        HttpResponse<String> metricPage = rpc("QueryMetricByPage", window(Map.of("Metric", "service_qps", "IntervalInSec", "10", "PageNumber", "1", "PageSize", "2")));

        assertThat(apps.statusCode()).isEqualTo(200);
        assertThat(apps.headers().firstValue("x-acs-request-id")).isPresent();
        assertThat(json.readTree(apps.body()).path("TraceApps")).hasSize(1);
        assertThat(json.readTree(search.body()).at("/PageBean/TraceInfos")).hasSize(3);
        assertThat(json.readTree(detail.body()).at("/Data/Spans")).hasSize(2);
        assertThat(json.readTree(metric.body()).path("Data").size()).isGreaterThan(1);
        assertThat(json.readTree(metricPage.body()).at("/PageBean/Records")).hasSize(2);
    }

    @Test void traceSearchPaginatesWithShortLastAndEmptyPage() throws Exception {
        scenario("PAGINATED", 4, "\"target_action\":\"SearchTracesByPage\"");
        JsonNode first = body(rpc("SearchTracesByPage", window(Map.of("PageNumber", "1", "PageSize", "2"))));
        JsonNode second = body(rpc("SearchTracesByPage", window(Map.of("PageNumber", "2", "PageSize", "2"))));
        JsonNode last = body(rpc("SearchTracesByPage", window(Map.of("PageNumber", "3", "PageSize", "2"))));
        JsonNode empty = body(rpc("SearchTracesByPage", window(Map.of("PageNumber", "4", "PageSize", "2"))));
        assertThat(first.at("/PageBean/TraceInfos")).hasSize(2);
        assertThat(second.at("/PageBean/TraceInfos")).hasSize(2);
        assertThat(last.at("/PageBean/TraceInfos")).hasSize(1);
        assertThat(empty.at("/PageBean/TraceInfos")).isEmpty();
        assertThat(first.at("/PageBean/TraceInfos/0/TraceID").asText())
                .isEqualTo(body(rpc("SearchTracesByPage", window(Map.of("PageNumber", "1", "PageSize", "2"))))
                        .at("/PageBean/TraceInfos/0/TraceID").asText());
    }

    @Test void supportsEmptyDuplicateAndSpecifiedPageError() throws Exception {
        scenario("EMPTY", 1, "\"target_action\":\"SearchTracesByPage\"");
        assertThat(body(rpc("SearchTracesByPage", window(Map.of("PageNumber", "1", "PageSize", "2"))))
                .at("/PageBean/TraceInfos")).isEmpty();

        scenario("DUPLICATE_RECORDS", 1, "\"target_action\":\"SearchTracesByPage\"");
        JsonNode duplicate = body(rpc("SearchTracesByPage", window(Map.of("PageNumber", "1", "PageSize", "2"))));
        assertThat(duplicate.at("/PageBean/TraceInfos/0/TraceID").asText())
                .isEqualTo(duplicate.at("/PageBean/TraceInfos/2/TraceID").asText());

        scenario("SERVER_ERROR", 1, "\"target_action\":\"SearchTracesByPage\",\"page_number\":2");
        assertThat(rpc("SearchTracesByPage", window(Map.of("PageNumber", "1", "PageSize", "2"))).statusCode()).isEqualTo(200);
        assertThat(rpc("SearchTracesByPage", window(Map.of("PageNumber", "2", "PageSize", "2"))).statusCode()).isEqualTo(500);
        assertThat(rpc("SearchTracesByPage", window(Map.of("PageNumber", "2", "PageSize", "2"))).statusCode()).isEqualTo(200);
    }

    @Test void traceDetailsSupportEmptyMissingErrorAndDelay() throws Exception {
        scenario("EMPTY", 1, "\"target_action\":\"GetTrace\",\"trace_id\":\"trace-empty\"");
        assertThat(body(rpc("GetTrace", Map.of("TraceID", "trace-empty"))).at("/Data/Spans")).isEmpty();
        assertThat(body(rpc("GetTrace", Map.of("TraceID", "missing-404"))).at("/Data/Spans")).isEmpty();
        scenario("SERVER_ERROR", 1, "\"target_action\":\"GetTrace\"");
        assertThat(rpc("GetTrace", Map.of("TraceID", "trace-error")).statusCode()).isEqualTo(500);
        scenario("DELAY", 1, "\"target_action\":\"GetTrace\",\"delay_millis\":120");
        long before = System.nanoTime();
        assertThat(rpc("GetTrace", Map.of("TraceID", "trace-delay")).statusCode()).isEqualTo(200);
        assertThat(Duration.ofNanos(System.nanoTime() - before).toMillis()).isGreaterThanOrEqualTo(100);
    }

    @Test void metricsAreDeterministicAndStayInsideTimeRange() throws Exception {
        Map<String, String> query = Map.of("StartTime", "1700000000000", "EndTime", "1700000030000",
                "Metric", "service_qps", "IntervalInSec", "10");
        HttpResponse<String> firstResponse = rpc("QueryMetric", query);
        HttpResponse<String> secondResponse = rpc("QueryMetric", query);
        JsonNode first = body(firstResponse);
        JsonNode second = body(secondResponse);
        assertThat(first.path("Data")).isEqualTo(second.path("Data"));
        first.path("Data").forEach(item -> assertThat(item.path("timestamp").asLong()).isBetween(1700000000000L, 1700000030000L));
        assertThat(first.at("/Data/0/service_id").asText()).isEqualTo("order-service");
        assertThat(first.at("/Data/0/instance_id").asText()).isEqualTo("testbed-instance-1");
    }

    @Test void metricByPageRoutesAndSupportsInvalidSchema() throws Exception {
        Map<String, String> query = Map.of("StartTime", "1700000000000", "EndTime", "1700000050000",
                "Metric", "latency", "IntervalInSec", "10", "PageNumber", "2", "PageSize", "2");
        JsonNode page = body(rpc("QueryMetricByPage", query));
        assertThat(page.at("/PageBean/Records")).hasSize(2);
        scenario("INVALID_SCHEMA", 1, "\"target_action\":\"QueryMetricByPage\"");
        assertThat(body(rpc("QueryMetricByPage", query)).path("unexpected").asBoolean()).isTrue();
    }

    @Test void datasetParsesFormChecksBasicAndIsDeterministic() throws Exception {
        Map<String, String> form = datasetForm();
        assertThat(dataset(form, null).statusCode()).isEqualTo(401);
        HttpResponse<String> first = dataset(form, basic("testbed-user", "testbed-password"));
        HttpResponse<String> second = dataset(form, basic("testbed-user", "testbed-password"));
        assertThat(first.statusCode()).isEqualTo(200);
        assertThat(body(first).path("rows")).isEqualTo(body(second).path("rows"));
        body(first).path("rows").forEach(row -> assertThat(row.path("timestamp").asLong()).isBetween(1700000000000L, 1700000030000L));
    }

    @Test void datasetSupportsEmptyDuplicatesLargeAndMalformed() throws Exception {
        String auth = basic("testbed-user", "testbed-password");
        scenario("EMPTY", 1, "\"target_action\":\"QueryDataset\"");
        assertThat(body(dataset(datasetForm(), auth)).path("rows")).isEmpty();
        scenario("DUPLICATE_RECORDS", 1, "\"target_action\":\"QueryDataset\"");
        JsonNode duplicate = body(dataset(datasetForm(), auth)).path("rows");
        assertThat(duplicate.get(0)).isEqualTo(duplicate.get(duplicate.size() - 1));
        scenario("PAGINATED", 1, "\"target_action\":\"QueryDataset\"");
        Map<String, String> large = new LinkedHashMap<>(datasetForm()); large.put("maxTime", "1700100000000");
        assertThat(body(dataset(large, auth)).path("rows").size()).isEqualTo(500);
        scenario("MALFORMED_JSON", 1, "\"target_action\":\"QueryDataset\"");
        assertThat(dataset(datasetForm(), auth).body()).isEqualTo("{not-json");
    }

    @Test void rateLimitServerErrorAndMalformedAutoRecover() throws Exception {
        scenario("RATE_LIMITED", 1, "\"target_action\":\"ListTraceApps\",\"retry_after_seconds\":7");
        HttpResponse<String> throttled = rpc("ListTraceApps", Map.of());
        assertThat(throttled.statusCode()).isEqualTo(429);
        assertThat(throttled.headers().firstValue("Retry-After")).contains("7");
        assertThat(rpc("ListTraceApps", Map.of()).statusCode()).isEqualTo(200);
        scenario("SERVER_ERROR", 1, "\"target_action\":\"ListTraceApps\"");
        assertThat(rpc("ListTraceApps", Map.of()).statusCode()).isEqualTo(500);
        scenario("MALFORMED_JSON", 1, "\"target_action\":\"ListTraceApps\"");
        assertThat(rpc("ListTraceApps", Map.of()).body()).isEqualTo("{not-json");
    }

    @Test void rejectsUnknownActionWrongAccessKeyAndNeverJournalsSecrets() throws Exception {
        assertThat(rpc("DeleteEverything", Map.of()).statusCode()).isEqualTo(404);
        HttpResponse<String> wrong = post("/", form(common("ListTraceApps", Map.of("AccessKeyId", "wrong", "Signature", "must-not-leak"))), null);
        assertThat(wrong.statusCode()).isEqualTo(403);
        JsonNode requests = body(get("/__admin/requests"));
        assertThat(requests.toString()).doesNotContain("must-not-leak", "AccessKeyId", "Signature");
        assertThat(requests.path("request_count").asInt()).isEqualTo(2);
    }

    private Map<String, String> window(Map<String, String> additions) {
        Map<String, String> values = new LinkedHashMap<>(Map.of("StartTime", "1700000000000", "EndTime", "1700000030000"));
        values.putAll(additions); return values;
    }
    private Map<String, String> datasetForm() {
        return new LinkedHashMap<>(Map.of("datasetId", "test-dataset", "minTime", "1700000000000", "maxTime", "1700000030000",
                "intervalInSec", "10", "measures", "value,count", "_userId", "testbed-user"));
    }
    private HttpResponse<String> rpc(String action, Map<String, String> values) throws Exception {
        return post("/", form(common(action, values)), null);
    }
    private Map<String, String> common(String action, Map<String, String> values) {
        Map<String, String> result = new LinkedHashMap<>(Map.of("Action", action, "Version", "2019-08-08", "RegionId", "test-region",
                "AccessKeyId", "testbed-access-key-id", "Signature", "synthetic-signature"));
        result.putAll(values); return result;
    }
    private HttpResponse<String> dataset(Map<String, String> values, String auth) throws Exception { return post("/QueryDataset", form(values), auth); }
    private HttpResponse<String> post(String path, String form, String auth) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(3))
                .header("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofString(form));
        if (auth != null) builder.header("Authorization", auth);
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }
    private HttpResponse<String> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(base + path)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }
    private void scenario(String name, int times, String extra) throws Exception {
        String body = "{\"scenario\":\"" + name + "\",\"times\":" + times + (extra.isBlank() ? "" : "," + extra) + "}";
        HttpResponse<String> response = client.send(HttpRequest.newBuilder(URI.create(base + "/__admin/scenario"))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
    }
    private JsonNode body(HttpResponse<String> response) throws Exception { return json.readTree(response.body()); }
    private String form(Map<String, String> values) {
        return values.entrySet().stream().map(e -> enc(e.getKey()) + "=" + enc(e.getValue())).collect(java.util.stream.Collectors.joining("&"));
    }
    private String enc(String value) { return URLEncoder.encode(value, StandardCharsets.UTF_8); }
    private String basic(String user, String password) {
        return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
    }
}

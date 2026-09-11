package cn.kjky.collector.testbed.arms;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** Container health probe with no shell or external binary dependency. */
public final class MockArmsHealthCheck {
    private MockArmsHealthCheck() {}
    public static void main(String[] args) throws Exception {
        String url = System.getenv().getOrDefault("MOCK_ARMS_HEALTH_URL", "http://127.0.0.1:9410/__admin/health");
        HttpResponse<Void> response = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()
                .send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(3)).GET().build(),
                        HttpResponse.BodyHandlers.discarding());
        if (response.statusCode() != 200) System.exit(1);
    }
}

package cn.kjky.collector.testbed.cc;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Map;

/** Container-friendly mTLS health probe. It never prints passwords or response payloads. */
public final class MockCcHealthCheck {
    private MockCcHealthCheck() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> env = System.getenv();
        Path keys = requiredPath(env, "MOCK_CC_HEALTH_KEYSTORE_PATH");
        Path trust = requiredPath(env, "MOCK_CC_HEALTH_TRUSTSTORE_PATH");
        String keyPassword = password(env, env.getOrDefault("MOCK_CC_HEALTH_KEYSTORE_PASSWORD_ENV", "MOCK_CC_KEYSTORE_PASSWORD"));
        String trustPassword = password(env, env.getOrDefault("MOCK_CC_HEALTH_TRUSTSTORE_PASSWORD_ENV", "MOCK_CC_TRUSTSTORE_PASSWORD"));
        String url = env.getOrDefault("MOCK_CC_HEALTH_URL", "https://localhost:9443/__admin/health");

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(load(keys, keyPassword), keyPassword.toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(load(trust, trustPassword));
        SSLContext ssl = SSLContext.getInstance("TLS");
        ssl.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        HttpClient client = HttpClient.newBuilder().sslContext(ssl).connectTimeout(Duration.ofSeconds(2)).build();
        HttpRequest request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(3)).GET().build();
        int status = client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
        if (status != 200) throw new IllegalStateException("health check returned HTTP " + status);
    }

    private static Path requiredPath(Map<String, String> env, String name) {
        String value = env.get(name);
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " is required");
        return Path.of(value);
    }

    private static String password(Map<String, String> env, String name) {
        String value = env.get(name);
        if (value == null || value.isEmpty()) throw new IllegalArgumentException("password environment variable is missing: " + name);
        return value;
    }

    private static KeyStore load(Path path, String password) throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(path)) {
            store.load(input, password.toCharArray());
        }
        return store;
    }
}

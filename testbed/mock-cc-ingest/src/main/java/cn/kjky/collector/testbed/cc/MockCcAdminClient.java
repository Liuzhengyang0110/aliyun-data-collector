package cn.kjky.collector.testbed.cc;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/** Small mTLS admin client used by PowerShell scripts without exposing secrets on the command line. */
public final class MockCcAdminClient {
    private MockCcAdminClient() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> options = options(args);
        Map<String, String> env = System.getenv();
        String url = required(options, "url");
        String method = options.getOrDefault("method", "GET").toUpperCase(java.util.Locale.ROOT);
        String body = options.getOrDefault("body", "");
        Path keys = requiredPath(env, "MOCK_CC_HEALTH_KEYSTORE_PATH");
        Path trust = requiredPath(env, "MOCK_CC_HEALTH_TRUSTSTORE_PATH");
        String keyPassword = password(env, env.getOrDefault("MOCK_CC_HEALTH_KEYSTORE_PASSWORD_ENV", "MOCK_CC_KEYSTORE_PASSWORD"));
        String trustPassword = password(env, env.getOrDefault("MOCK_CC_HEALTH_TRUSTSTORE_PASSWORD_ENV", "MOCK_CC_TRUSTSTORE_PASSWORD"));

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(load(keys, keyPassword), keyPassword.toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(load(trust, trustPassword));
        SSLContext ssl = SSLContext.getInstance("TLS");
        ssl.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        HttpClient client = HttpClient.newBuilder().sslContext(ssl).connectTimeout(Duration.ofSeconds(3)).build();
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5));
        if ("GET".equals(method)) request.GET();
        else if ("POST".equals(method)) request.header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        else throw new IllegalArgumentException("only GET and POST are supported");
        HttpResponse<String> response = client.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        System.out.print(response.body());
        if (response.statusCode() < 200 || response.statusCode() >= 300) System.exit(2);
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

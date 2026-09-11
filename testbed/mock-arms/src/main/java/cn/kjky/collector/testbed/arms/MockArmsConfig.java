package cn.kjky.collector.testbed.arms;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;

/** Runtime-only configuration for the local ARMS simulator. */
public record MockArmsConfig(String bindAddress, int port, String expectedAccessKeyId,
                             String datasetUsername, String datasetPassword) {
    public static MockArmsConfig defaults() {
        Map<String, String> env = System.getenv();
        return new MockArmsConfig(env.getOrDefault("MOCK_ARMS_BIND_ADDRESS", "127.0.0.1"),
                integer(env.get("MOCK_ARMS_PORT"), 9410), blankToNull(env.get("MOCK_ARMS_EXPECTED_ACCESS_KEY_ID")),
                blankToNull(env.get("MOCK_ARMS_DATASET_USERNAME")), blankToNull(env.get("MOCK_ARMS_DATASET_PASSWORD")));
    }

    public static MockArmsConfig fromArgs(String[] args) {
        MockArmsConfig defaults = defaults();
        Map<String, String> options = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (!args[i].startsWith("--") || i + 1 >= args.length) {
                throw new IllegalArgumentException("expected --name value arguments");
            }
            options.put(args[i].substring(2), args[i + 1]);
        }
        return new MockArmsConfig(options.getOrDefault("bind", defaults.bindAddress()),
                integer(options.get("port"), defaults.port()),
                blankToNull(options.getOrDefault("expected-access-key-id", defaults.expectedAccessKeyId())),
                blankToNull(options.getOrDefault("dataset-username", defaults.datasetUsername())),
                blankToNull(options.getOrDefault("dataset-password", defaults.datasetPassword())));
    }

    public InetSocketAddress address() { return new InetSocketAddress(bindAddress, port); }

    private static int integer(String value, int fallback) {
        int parsed = value == null || value.isBlank() ? fallback : Integer.parseInt(value);
        if (parsed < 0 || parsed > 65535) throw new IllegalArgumentException("invalid port: " + parsed);
        return parsed;
    }

    private static String blankToNull(String value) { return value == null || value.isBlank() ? null : value; }
}

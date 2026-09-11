package cn.kjky.collector.testbed.cc;

import java.net.InetAddress;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

public record ServerConfig(
        InetAddress bindAddress,
        int port,
        Path stateDirectory,
        Path keyStorePath,
        String keyStorePassword,
        Path trustStorePath,
        String trustStorePassword,
        Path collectorCertificateMap,
        int maxRecords,
        long maxCompressedBytes,
        long maxRequestBytes) {

    public static final int DEFAULT_PORT = 9443;
    public static final int DEFAULT_MAX_RECORDS = 1000;
    public static final long DEFAULT_MAX_COMPRESSED_BYTES = 2L * 1024 * 1024;
    public static final long DEFAULT_MAX_REQUEST_BYTES = 10L * 1024 * 1024;

    public ServerConfig {
        if (bindAddress == null) throw new IllegalArgumentException("bindAddress is required");
        if (port < 0 || port > 65535) throw new IllegalArgumentException("port must be in 0..65535");
        if (stateDirectory == null) throw new IllegalArgumentException("stateDirectory is required");
        if (keyStorePath == null || trustStorePath == null) throw new IllegalArgumentException("TLS stores are required");
        if (keyStorePassword == null || keyStorePassword.isEmpty()) throw new IllegalArgumentException("KeyStore password is required");
        if (trustStorePassword == null || trustStorePassword.isEmpty()) throw new IllegalArgumentException("TrustStore password is required");
        if (maxRecords < 1) throw new IllegalArgumentException("maxRecords must be positive");
        if (maxCompressedBytes < 1 || maxRequestBytes < 1) throw new IllegalArgumentException("request limits must be positive");
    }

    static ServerConfig from(String[] args, Map<String, String> environment) throws Exception {
        Map<String, String> options = parseOptions(args);
        String bind = value(options, environment, "bind", "MOCK_CC_BIND_ADDRESS", "127.0.0.1");
        int port = Integer.parseInt(value(options, environment, "port", "MOCK_CC_PORT", Integer.toString(DEFAULT_PORT)));
        Path state = Path.of(value(options, environment, "state-dir", "MOCK_CC_STATE_DIR", "./.state/mock-cc-ingest"));
        Path keys = requiredPath(options, environment, "key-store", "MOCK_CC_KEYSTORE_PATH");
        Path trust = requiredPath(options, environment, "trust-store", "MOCK_CC_TRUSTSTORE_PATH");
        String keyPassword = password(options, environment, "key-store-password-env", "MOCK_CC_KEYSTORE_PASSWORD_ENV",
                "MOCK_CC_KEYSTORE_PASSWORD");
        String trustPassword = password(options, environment, "trust-store-password-env", "MOCK_CC_TRUSTSTORE_PASSWORD_ENV",
                "MOCK_CC_TRUSTSTORE_PASSWORD");
        String mapping = value(options, environment, "collector-cert-map", "MOCK_CC_COLLECTOR_CERT_MAP", "");
        return new ServerConfig(InetAddress.getByName(bind), port, state, keys, keyPassword, trust, trustPassword,
                mapping.isBlank() ? null : Path.of(mapping),
                Integer.parseInt(value(options, environment, "max-records", "MOCK_CC_MAX_RECORDS", Integer.toString(DEFAULT_MAX_RECORDS))),
                Long.parseLong(value(options, environment, "max-compressed-bytes", "MOCK_CC_MAX_COMPRESSED_BYTES", Long.toString(DEFAULT_MAX_COMPRESSED_BYTES))),
                Long.parseLong(value(options, environment, "max-request-bytes", "MOCK_CC_MAX_REQUEST_BYTES", Long.toString(DEFAULT_MAX_REQUEST_BYTES))));
    }

    private static String password(Map<String, String> options, Map<String, String> env, String option,
                                   String envSelector, String defaultEnvName) {
        String variableName = value(options, env, option, envSelector, defaultEnvName);
        String secret = env.get(variableName);
        if (secret == null || secret.isEmpty()) {
            throw new IllegalArgumentException("Required password environment variable is not set: " + variableName);
        }
        return secret;
    }

    private static Path requiredPath(Map<String, String> options, Map<String, String> env,
                                     String option, String variable) {
        String value = value(options, env, option, variable, "");
        if (value.isBlank()) throw new IllegalArgumentException("Missing --" + option + " or " + variable);
        return Path.of(value);
    }

    private static String value(Map<String, String> options, Map<String, String> env,
                                String option, String variable, String fallback) {
        if (options.containsKey(option)) return options.get(option);
        String value = env.get(variable);
        return value == null ? fallback : value;
    }

    private static Map<String, String> parseOptions(String[] args) {
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            String current = args[i];
            if (!current.startsWith("--") || i + 1 >= args.length) {
                throw new IllegalArgumentException("Expected --name value arguments");
            }
            result.put(current.substring(2), args[++i]);
        }
        return result;
    }
}

package cn.kjky.collector.testbed.cc;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

public final class MockCcIngestApplication {
    private MockCcIngestApplication() {
    }

    public static void main(String[] args) throws Exception {
        if (Arrays.asList(args).contains("--help")) {
            printHelp();
            return;
        }
        ServerConfig config = ServerConfig.from(args, System.getenv());
        MockCcIngestServer server = new MockCcIngestServer(config);
        Runtime.getRuntime().addShutdownHook(new Thread(server::close, "mock-cc-ingest-shutdown"));
        server.start();
        new CountDownLatch(1).await();
    }

    private static void printHelp() {
        System.out.println("""
                mock-cc-ingest options (all values also support the documented MOCK_CC_* environment variables):
                  --bind <address>                   default 127.0.0.1
                  --port <port>                      default 9443
                  --state-dir <directory>            default ./.state/mock-cc-ingest
                  --key-store <server.p12>           required
                  --trust-store <trusted-clients.p12> required
                  --key-store-password-env <name>    default MOCK_CC_KEYSTORE_PASSWORD
                  --trust-store-password-env <name>  default MOCK_CC_TRUSTSTORE_PASSWORD
                  --collector-cert-map <file.json>   optional collector_id to SHA-256 fingerprint map
                  --max-records <count>              default 1000
                  --max-compressed-bytes <bytes>      default 2097152
                  --max-request-bytes <bytes>         default 10485760
                """);
    }
}

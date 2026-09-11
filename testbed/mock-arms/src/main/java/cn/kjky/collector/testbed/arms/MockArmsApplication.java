package cn.kjky.collector.testbed.arms;

import java.util.concurrent.CountDownLatch;

/** Standalone mock-arms entry point. */
public final class MockArmsApplication {
    private MockArmsApplication() {}

    public static void main(String[] args) throws Exception {
        MockArmsConfig config = MockArmsConfig.fromArgs(args);
        MockArmsServer server = new MockArmsServer(config);
        Runtime.getRuntime().addShutdownHook(new Thread(server::close, "mock-arms-shutdown"));
        server.start();
        System.out.printf("mock-arms listening on http://%s:%d%n", config.bindAddress(), server.port());
        new CountDownLatch(1).await();
    }
}

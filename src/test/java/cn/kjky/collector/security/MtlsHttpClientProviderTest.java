package cn.kjky.collector.security;

import cn.kjky.collector.config.CollectorConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MtlsHttpClientProviderTest {
    private static Path root;
    private static TestPki trustedPki;
    private static TestPki otherPki;
    private static Path server;
    private static Path wrongHostnameServer;
    private static Path clientOne;
    private static Path clientTwo;
    private static Path wrongClient;
    private static Path expiredClient;
    private static Path trustedServerStore;
    private static Path otherTrustStore;

    @BeforeAll
    static void createCertificates() throws Exception {
        root = Path.of("target", "test-mtls", UUID.randomUUID().toString());
        trustedPki = TestPki.create(root.resolve("trusted"), "collector-test-ca");
        otherPki = TestPki.create(root.resolve("other"), "other-test-ca");
        server = trustedPki.issueServer("server", "localhost", "dns:localhost,ip:127.0.0.1");
        wrongHostnameServer = trustedPki.issueServer("wrong-host-server", "wrong.invalid", "dns:wrong.invalid");
        clientOne = trustedPki.issueClient("client-one", "collector-one");
        clientTwo = trustedPki.issueClient("client-two", "collector-two");
        wrongClient = otherPki.issueClient("wrong-client", "untrusted-collector");
        expiredClient = trustedPki.issueExpiredClient("expired-client", "expired-collector");
        trustedServerStore = trustedPki.trustStore("trust-server");
        otherTrustStore = otherPki.trustStore("trust-other");
    }

    @AfterAll
    static void leaveGeneratedMaterialsOnlyUnderTarget() {
        // target 已由 Git 忽略；不把任何测试私钥复制到源码、资源或文档目录。
    }

    @Test
    void loadsKeyStoreFromSecretVolumeAndPerformsMutualTls() throws Exception {
        List<String> logs = new ArrayList<>();
        try (TestPki.RunningServer endpoint = TestPki.startServer(server, trustedServerStore);
             MtlsHttpClientProvider provider = provider(keyStoreConfig(clientOne, trustedServerStore), logs)) {
            HttpResponse<String> response = get(provider, endpoint.url());

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(endpoint.peerFingerprint()).isEqualTo(provider.certificateInfo().fingerprintSha256());
            assertThat(provider.certificateInfo().subjectSummary()).isEqualTo("CN=collector-one");
            assertSafeCertificateLogs(logs);
        }
    }

    @Test
    void loadsPkcs8PemCertificateAndPrivateKey() throws Exception {
        TestPki.PemIdentity pem = trustedPki.exportPem(clientOne, "client-one", "client-one");
        CollectorConfig.SecurityConfig config = baseConfig(trustedServerStore);
        config.clientAuthMode = "PEM";
        config.clientCertificatePath = pem.certificate().toString();
        config.privateKeyPath = pem.privateKey().toString();
        config.keyStorePath = null;
        config.keyStorePasswordEnv = null;

        try (TestPki.RunningServer endpoint = TestPki.startServer(server, trustedServerStore);
             MtlsHttpClientProvider provider = provider(config, new ArrayList<>())) {
            assertThat(get(provider, endpoint.url()).statusCode()).isEqualTo(200);
            assertThat(endpoint.peerFingerprint()).isEqualTo(provider.certificateInfo().fingerprintSha256());
        }
    }

    @Test
    void rejectsMissingClientCertificateWithoutLeakingPath() {
        CollectorConfig.SecurityConfig config = keyStoreConfig(root.resolve("missing-client.p12"), trustedServerStore);

        assertThatThrownBy(() -> provider(config, new ArrayList<>()))
                .isInstanceOf(MtlsClientException.class)
                .hasMessageContaining("不存在或不可读")
                .hasMessageNotContaining(root.toString());
    }

    @Test
    void rejectsExpiredClientCertificateAtStartup() {
        CollectorConfig.SecurityConfig config = keyStoreConfig(expiredClient, trustedServerStore);

        assertThatThrownBy(() -> provider(config, new ArrayList<>()))
                .isInstanceOf(MtlsClientException.class)
                .hasMessageContaining("已过期或尚未生效");
    }

    @Test
    void serverRejectsClientCertificateSignedByWrongAuthority() throws Exception {
        try (TestPki.RunningServer endpoint = TestPki.startServer(server, trustedServerStore);
             MtlsHttpClientProvider provider = provider(keyStoreConfig(wrongClient, trustedServerStore), new ArrayList<>())) {
            assertThatThrownBy(() -> get(provider, endpoint.url()))
                    .isInstanceOf(java.io.IOException.class);
        }
    }

    @Test
    void rejectsServerCertificateOutsideConfiguredTrustStore() throws Exception {
        try (TestPki.RunningServer endpoint = TestPki.startServer(server, trustedServerStore);
             MtlsHttpClientProvider provider = provider(keyStoreConfig(clientOne, otherTrustStore), new ArrayList<>())) {
            assertThatThrownBy(() -> get(provider, endpoint.url()))
                    .isInstanceOf(java.io.IOException.class);
        }
    }

    @Test
    void rejectsTrustedServerWhoseHostnameDoesNotMatch() throws Exception {
        try (TestPki.RunningServer endpoint = TestPki.startServer(wrongHostnameServer, trustedServerStore);
             MtlsHttpClientProvider provider = provider(keyStoreConfig(clientOne, trustedServerStore), new ArrayList<>())) {
            assertThatThrownBy(() -> get(provider, endpoint.url()))
                    .isInstanceOf(java.io.IOException.class);
        }
    }

    @Test
    void reloadsRotatedSecretAtomicallyAndKeepsOutboxUntouched() throws Exception {
        Path activeClient = root.resolve("active-client.p12");
        Files.copy(clientOne, activeClient);
        Path pendingBatch = root.resolve("data/outbox/pending/batch-001.json");
        Files.createDirectories(pendingBatch.getParent());
        Files.writeString(pendingBatch, "pending", StandardCharsets.UTF_8);
        List<String> logs = new ArrayList<>();

        try (TestPki.RunningServer endpoint = TestPki.startServer(server, trustedServerStore);
             MtlsHttpClientProvider provider = provider(keyStoreConfig(activeClient, trustedServerStore), logs)) {
            assertThat(get(provider, endpoint.url()).statusCode()).isEqualTo(200);
            String firstFingerprint = endpoint.peerFingerprint();

            TestPki.atomicReplace(clientTwo, activeClient);
            assertThat(get(provider, endpoint.url()).statusCode()).isEqualTo(200);
            String secondFingerprint = endpoint.peerFingerprint();

            assertThat(secondFingerprint).isNotEqualTo(firstFingerprint)
                    .isEqualTo(provider.certificateInfo().fingerprintSha256());
            assertThat(provider.certificateInfo().subjectSummary()).isEqualTo("CN=collector-two");
            assertThat(Files.readString(pendingBatch)).isEqualTo("pending");

            Files.writeString(activeClient, "broken-secret-update", StandardCharsets.UTF_8);
            assertThat(get(provider, endpoint.url()).statusCode()).isEqualTo(200);
            assertThat(endpoint.peerFingerprint()).isEqualTo(secondFingerprint);
            assertThat(Files.readString(pendingBatch)).isEqualTo("pending");
            assertThat(logs).anyMatch(line -> line.startsWith("MTLS_CERTIFICATE_RELOAD_FAILED_USING_PREVIOUS"));
            assertSafeCertificateLogs(logs);
        }
    }

    @Test
    void warnsWhenClientCertificateApproachesExpiryUsingOnlySafeMetadata() {
        CollectorConfig.SecurityConfig config = keyStoreConfig(clientOne, trustedServerStore);
        config.certificateExpiryWarningDays = 400;
        List<String> logs = new ArrayList<>();

        try (MtlsHttpClientProvider ignored = provider(config, logs)) {
            assertThat(logs).anyMatch(line -> line.startsWith("MTLS_CLIENT_CERTIFICATE_EXPIRING"));
            assertSafeCertificateLogs(logs);
        }
    }

    private static MtlsHttpClientProvider provider(CollectorConfig.SecurityConfig config, List<String> logs) {
        return new MtlsHttpClientProvider(config, 5,
                ignored -> TestPki.PASSWORD.toCharArray(), Clock.systemUTC(), Duration.ZERO,
                logs::add, logs::add);
    }

    private static CollectorConfig.SecurityConfig keyStoreConfig(Path client, Path trustStore) {
        CollectorConfig.SecurityConfig config = baseConfig(trustStore);
        config.clientAuthMode = "KEYSTORE";
        config.keyStorePath = client.toString();
        config.keyStoreType = "PKCS12";
        config.keyStorePasswordEnv = "TEST_KEYSTORE_PASSWORD";
        return config;
    }

    private static CollectorConfig.SecurityConfig baseConfig(Path trustStore) {
        CollectorConfig.SecurityConfig config = new CollectorConfig.SecurityConfig();
        config.mtlsEnabled = true;
        config.trustStorePath = trustStore.toString();
        config.trustStoreType = "PKCS12";
        config.trustStorePasswordEnv = "TEST_TRUSTSTORE_PASSWORD";
        config.certificateExpiryWarningDays = 30;
        config.certificateReloadIntervalSeconds = 1;
        return config;
    }

    private static HttpResponse<String> get(MtlsHttpClientProvider provider, String url) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url)).GET().build();
        return provider.client().send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static void assertSafeCertificateLogs(List<String> logs) {
        assertThat(logs).allSatisfy(line -> {
            assertThat(line).contains("fingerprint_sha256=", "subject=CN=", "not_after=");
            assertThat(line).doesNotContain(TestPki.PASSWORD, "PRIVATE KEY", "BEGIN CERTIFICATE", root.toString());
        });
    }
}

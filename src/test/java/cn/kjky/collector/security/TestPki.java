package cn.kjky.collector.security;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsExchange;
import com.sun.net.httpserver.HttpsServer;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.jce.X509Principal;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.x509.X509V3CertificateGenerator;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Date;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicLong;

/** 在 target 临时目录中生成 mTLS 测试材料，不把任何测试私钥提交到 Git。 */
@SuppressWarnings("deprecation")
final class TestPki {
    static final String PASSWORD = "test-changeit";
    private static final AtomicLong SERIAL = new AtomicLong(System.currentTimeMillis());

    static {
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    private final Path root;
    private final KeyPair caKeys;
    private final X509Certificate caCertificate;

    private TestPki(Path root, KeyPair caKeys, X509Certificate caCertificate) {
        this.root = root;
        this.caKeys = caKeys;
        this.caCertificate = caCertificate;
    }

    static TestPki create(Path root, String commonName) throws Exception {
        Files.createDirectories(root);
        KeyPair keys = keyPair();
        X509Certificate certificate = certificate("CN=" + commonName, keys, null, keys.getPrivate(),
                Instant.now().minus(1, ChronoUnit.DAYS), Instant.now().plus(3650, ChronoUnit.DAYS),
                true, null, null);
        return new TestPki(root, keys, certificate);
    }

    Path issueClient(String name, String commonName) throws Exception {
        return issue(name, commonName, false, true,
                Instant.now().minus(1, ChronoUnit.DAYS), Instant.now().plus(365, ChronoUnit.DAYS), null);
    }

    Path issueExpiredClient(String name, String commonName) throws Exception {
        return issue(name, commonName, false, true,
                Instant.parse("2020-01-01T00:00:00Z"), Instant.parse("2020-01-02T00:00:00Z"), null);
    }

    Path issueServer(String name, String commonName, String san) throws Exception {
        return issue(name, commonName, true, false,
                Instant.now().minus(1, ChronoUnit.DAYS), Instant.now().plus(365, ChronoUnit.DAYS), san);
    }

    private Path issue(String name, String commonName, boolean server, boolean client,
                       Instant notBefore, Instant notAfter, String san) throws Exception {
        KeyPair keys = keyPair();
        X509Certificate certificate = certificate("CN=" + commonName, keys, caCertificate, caKeys.getPrivate(),
                notBefore, notAfter, false, server ? Boolean.TRUE : client ? Boolean.FALSE : null, san);
        Path storePath = root.resolve(name + ".p12");
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, PASSWORD.toCharArray());
        store.setKeyEntry(name, keys.getPrivate(), PASSWORD.toCharArray(),
                new java.security.cert.Certificate[]{certificate, caCertificate});
        try (var output = Files.newOutputStream(storePath)) {
            store.store(output, PASSWORD.toCharArray());
        }
        return storePath;
    }

    Path trustStore(String name) throws Exception {
        Path storePath = root.resolve(name + ".p12");
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, PASSWORD.toCharArray());
        store.setCertificateEntry("ca", caCertificate);
        try (var output = Files.newOutputStream(storePath)) {
            store.store(output, PASSWORD.toCharArray());
        }
        return storePath;
    }

    PemIdentity exportPem(Path keyStore, String alias, String name) throws Exception {
        KeyStore store = loadStore(keyStore);
        X509Certificate[] chain = java.util.Arrays.stream(store.getCertificateChain(alias))
                .map(X509Certificate.class::cast).toArray(X509Certificate[]::new);
        PrivateKey key = (PrivateKey) store.getKey(alias, PASSWORD.toCharArray());
        Path certificate = root.resolve(name + "-chain.pem");
        Path privateKey = root.resolve(name + "-key.pem");
        StringBuilder pemChain = new StringBuilder();
        for (X509Certificate item : chain) pemChain.append(pem("CERTIFICATE", item.getEncoded()));
        Files.writeString(certificate, pemChain, StandardCharsets.US_ASCII);
        Files.writeString(privateKey, pem("PRIVATE KEY", key.getEncoded()), StandardCharsets.US_ASCII);
        return new PemIdentity(certificate, privateKey);
    }

    static RunningServer startServer(Path serverKeyStore, Path trustedClients) throws Exception {
        KeyStore keys = loadStore(serverKeyStore);
        KeyStore trust = loadStore(trustedClients);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keys, PASSWORD.toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trust);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

        HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(context) {
            @Override
            public void configure(com.sun.net.httpserver.HttpsParameters parameters) {
                SSLParameters ssl = getSSLContext().getDefaultSSLParameters();
                ssl.setNeedClientAuth(true);
                parameters.setSSLParameters(ssl);
            }
        });
        java.util.concurrent.atomic.AtomicReference<String> peerFingerprint = new java.util.concurrent.atomic.AtomicReference<>();
        server.createContext("/health", exchange -> {
            try {
                X509Certificate peer = (X509Certificate) ((HttpsExchange) exchange)
                        .getSSLSession().getPeerCertificates()[0];
                peerFingerprint.set(fingerprint(peer));
                byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            } finally {
                exchange.close();
            }
        });
        server.start();
        return new RunningServer(server, peerFingerprint);
    }

    static void atomicReplace(Path source, Path target) throws IOException {
        Path temporary = target.resolveSibling(target.getFileName() + ".next");
        Files.copy(source, temporary, StandardCopyOption.REPLACE_EXISTING);
        try {
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (java.nio.file.AtomicMoveNotSupportedException e) {
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static KeyPair keyPair() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048, new SecureRandom());
        return generator.generateKeyPair();
    }

    private static X509Certificate certificate(String subject, KeyPair subjectKeys, X509Certificate issuer,
                                               PrivateKey issuerKey, Instant notBefore, Instant notAfter,
                                               boolean ca, Boolean serverCertificate, String san) throws Exception {
        X509V3CertificateGenerator generator = new X509V3CertificateGenerator();
        X509Principal subjectName = new X509Principal(subject);
        generator.setSerialNumber(BigInteger.valueOf(SERIAL.incrementAndGet()));
        generator.setIssuerDN(issuer == null
                ? subjectName : new X509Principal(issuer.getSubjectX500Principal().getEncoded()));
        generator.setNotBefore(Date.from(notBefore));
        generator.setNotAfter(Date.from(notAfter));
        generator.setSubjectDN(subjectName);
        generator.setPublicKey(subjectKeys.getPublic());
        generator.setSignatureAlgorithm("SHA256WithRSAEncryption");
        generator.addExtension(Extension.basicConstraints, true, new BasicConstraints(ca));
        if (serverCertificate != null) {
            generator.addExtension(Extension.extendedKeyUsage, false,
                    new ExtendedKeyUsage(serverCertificate ? KeyPurposeId.id_kp_serverAuth : KeyPurposeId.id_kp_clientAuth));
        }
        if (san != null) generator.addExtension(Extension.subjectAlternativeName, false, subjectAlternativeNames(san));
        X509Certificate certificate = generator.generate(issuerKey, BouncyCastleProvider.PROVIDER_NAME);
        certificate.verify(issuer == null ? subjectKeys.getPublic() : issuer.getPublicKey());
        return certificate;
    }

    private static GeneralNames subjectAlternativeNames(String specification) {
        GeneralName[] names = java.util.Arrays.stream(specification.split(","))
                .map(String::trim)
                .map(value -> {
                    String[] parts = value.split(":", 2);
                    int type = "ip".equalsIgnoreCase(parts[0]) ? GeneralName.iPAddress : GeneralName.dNSName;
                    return new GeneralName(type, parts[1]);
                }).toArray(GeneralName[]::new);
        return new GeneralNames(names);
    }

    private static KeyStore loadStore(Path path) throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (var input = Files.newInputStream(path)) {
            store.load(input, PASSWORD.toCharArray());
        }
        return store;
    }

    private static String pem(String label, byte[] bytes) {
        String encoded = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(bytes);
        return "-----BEGIN " + label + "-----\n" + encoded + "\n-----END " + label + "-----\n";
    }

    private static String fingerprint(X509Certificate certificate) throws IOException {
        try {
            return HexFormat.ofDelimiter(":").withUpperCase().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded()));
        } catch (java.security.GeneralSecurityException e) {
            throw new IOException("测试证书指纹计算失败", e);
        }
    }

    record PemIdentity(Path certificate, Path privateKey) {
    }

    static final class RunningServer implements AutoCloseable {
        private final HttpsServer server;
        private final java.util.concurrent.atomic.AtomicReference<String> peerFingerprint;

        RunningServer(HttpsServer server, java.util.concurrent.atomic.AtomicReference<String> peerFingerprint) {
            this.server = server;
            this.peerFingerprint = peerFingerprint;
        }

        String url() {
            return "https://localhost:" + server.getAddress().getPort() + "/health";
        }

        String peerFingerprint() {
            return peerFingerprint.get();
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}

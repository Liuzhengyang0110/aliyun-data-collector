package cn.kjky.collector.testbed.cc;

import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.jce.X509Principal;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.x509.X509V3CertificateGenerator;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.Date;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("deprecation")
final class TestPki {
    static final String PASSWORD = "testbed-changeit";
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
        return issue(name, commonName, false, true, Instant.now().minus(1, ChronoUnit.DAYS),
                Instant.now().plus(365, ChronoUnit.DAYS), null);
    }

    Path issueExpiredClient(String name, String commonName) throws Exception {
        return issue(name, commonName, false, true, Instant.parse("2020-01-01T00:00:00Z"),
                Instant.parse("2020-01-02T00:00:00Z"), null);
    }

    Path issueServer(String name, String commonName, String san) throws Exception {
        return issue(name, commonName, true, false, Instant.now().minus(1, ChronoUnit.DAYS),
                Instant.now().plus(365, ChronoUnit.DAYS), san);
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

    String fingerprint(Path keyStore, String alias) throws Exception {
        KeyStore store = loadStore(keyStore);
        X509Certificate certificate = (X509Certificate) store.getCertificate(alias);
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded()));
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
        generator.setIssuerDN(issuer == null ? subjectName : new X509Principal(issuer.getSubjectX500Principal().getEncoded()));
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
}

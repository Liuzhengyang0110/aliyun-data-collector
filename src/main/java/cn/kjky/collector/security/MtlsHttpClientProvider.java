package cn.kjky.collector.security;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.config.SecretResolver;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import javax.naming.ldap.LdapName;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.Enumeration;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * 从只读 Secret Volume 加载客户端身份和服务端 TrustStore，并创建强制主机名校验的 HTTPS 客户端。
 * Secret 文件更新时先完整验证候选材料，再原子替换客户端；失败不会触碰 Outbox，也不会替换仍有效的旧客户端。
 */
public final class MtlsHttpClientProvider implements AutoCloseable {
    private static final long MAX_SECRET_FILE_BYTES = 16L * 1024 * 1024;
    private static final String CLIENT_AUTH_OID = "1.3.6.1.5.5.7.3.2";

    private final CollectorConfig.SecurityConfig config;
    private final int connectTimeoutSeconds;
    private final PasswordProvider passwords;
    private final Clock clock;
    private final Duration reloadInterval;
    private final Consumer<String> infoLog;
    private final Consumer<String> warningLog;
    private final ScheduledExecutorService watcher;
    private final AtomicReference<ClientState> current = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Instant nextCheckAt = Instant.EPOCH;
    private volatile String warnedFingerprint;

    /** 使用环境变量密码和系统时钟创建生产 mTLS 客户端提供器。 */
    public MtlsHttpClientProvider(CollectorConfig.SecurityConfig config, int connectTimeoutSeconds) {
        this(config, connectTimeoutSeconds, new SecretResolver()::requiredChars, Clock.systemUTC(),
                Duration.ofSeconds(requireConfig(config).certificateReloadIntervalSeconds),
                System.out::println, System.err::println, true);
    }

    /** 允许测试注入密码、时钟、检查周期和日志接收器。 */
    MtlsHttpClientProvider(CollectorConfig.SecurityConfig config, int connectTimeoutSeconds,
                           PasswordProvider passwords, Clock clock, Duration reloadInterval,
                           Consumer<String> infoLog, Consumer<String> warningLog) {
        this(config, connectTimeoutSeconds, passwords, clock, reloadInterval, infoLog, warningLog, false);
    }

    private MtlsHttpClientProvider(CollectorConfig.SecurityConfig config, int connectTimeoutSeconds,
                                   PasswordProvider passwords, Clock clock, Duration reloadInterval,
                                   Consumer<String> infoLog, Consumer<String> warningLog,
                                   boolean startWatcher) {
        this.config = requireConfig(config);
        if (!config.mtlsEnabled) throw new MtlsClientException("security.mtlsEnabled=false，不能创建 mTLS 客户端");
        if (connectTimeoutSeconds < 1) throw new MtlsClientException("mTLS 客户端连接超时必须大于 0");
        this.connectTimeoutSeconds = connectTimeoutSeconds;
        this.passwords = Objects.requireNonNull(passwords, "密码读取器不能为空");
        this.clock = Objects.requireNonNull(clock, "时钟不能为空");
        this.reloadInterval = Objects.requireNonNull(reloadInterval, "重载检查周期不能为空");
        if (reloadInterval.isNegative()) throw new MtlsClientException("重载检查周期不能为负数");
        this.infoLog = Objects.requireNonNull(infoLog, "info 日志接收器不能为空");
        this.warningLog = Objects.requireNonNull(warningLog, "warning 日志接收器不能为空");

        ClientState initial = loadCandidate();
        current.set(initial);
        nextCheckAt = clock.instant().plus(reloadInterval);
        logCertificate("MTLS_CLIENT_CERTIFICATE_LOADED", initial.certificate(), infoLog);
        warnIfExpiring(initial.certificate(), clock.instant());
        if (startWatcher) {
            if (reloadInterval.isZero()) throw new MtlsClientException("证书后台检查周期必须大于 0");
            this.watcher = Executors.newSingleThreadScheduledExecutor(task -> {
                Thread thread = new Thread(task, "mtls-certificate-watcher");
                thread.setDaemon(true);
                return thread;
            });
            long intervalMillis = Math.max(1L, reloadInterval.toMillis());
            watcher.scheduleWithFixedDelay(this::scheduledCheck, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        } else {
            this.watcher = null;
        }
    }

    /** 返回当前有效客户端；到达检查周期时检测 Kubernetes Secret 的原子投影更新。 */
    public HttpClient client() throws IOException {
        if (closed.get()) throw new IOException("mTLS 客户端已关闭");
        Instant now = clock.instant();
        ClientState state = current.get();
        ensureCurrentlyValid(state.certificate(), now);
        if (!now.isBefore(nextCheckAt)) state = reloadIfChanged(now);
        ensureCurrentlyValid(state.certificate(), now);
        warnIfExpiring(state.certificate(), now);
        return state.client();
    }

    /** 返回当前客户端证书的安全摘要，供健康状态和测试使用。 */
    public ClientCertificateInfo certificateInfo() {
        return current.get().certificate();
    }

    private void scheduledCheck() {
        try {
            client();
        } catch (IOException | RuntimeException ignored) {
            // client()/reloadIfChanged 已经仅用安全证书摘要记录告警；后台线程不得输出异常正文或路径。
        }
    }

    private synchronized ClientState reloadIfChanged(Instant now) throws IOException {
        if (now.isBefore(nextCheckAt)) return current.get();
        nextCheckAt = now.plus(reloadInterval);
        ClientState previous = current.get();
        try {
            String observed = materialFingerprint();
            if (observed.equals(previous.materialFingerprint())) return previous;
            ClientState candidate = loadCandidate();
            current.set(candidate);
            warnedFingerprint = null;
            logCertificate("MTLS_CLIENT_CERTIFICATE_RELOADED", candidate.certificate(), infoLog);
            return candidate;
        } catch (RuntimeException e) {
            logCertificate("MTLS_CERTIFICATE_RELOAD_FAILED_USING_PREVIOUS", previous.certificate(), warningLog);
            ensureCurrentlyValid(previous.certificate(), now);
            return previous;
        }
    }

    private ClientState loadCandidate() {
        char[] keyPassword = null;
        char[] trustPassword = null;
        try {
            String fingerprintBeforeLoad = materialFingerprint();
            Identity identity;
            if ("PEM".equalsIgnoreCase(config.clientAuthMode)) {
                identity = loadPemIdentity(requiredFile(config.clientCertificatePath), requiredFile(config.privateKeyPath));
            } else if ("KEYSTORE".equalsIgnoreCase(config.clientAuthMode)) {
                keyPassword = requiredPassword(config.keyStorePasswordEnv);
                identity = loadKeyStoreIdentity(requiredFile(config.keyStorePath), config.keyStoreType, keyPassword);
            } else {
                throw new MtlsClientException("不支持的客户端身份模式");
            }
            validateIdentity(identity, clock.instant());

            trustPassword = requiredPassword(config.trustStorePasswordEnv);
            KeyStore trustStore = loadStore(requiredFile(config.trustStorePath), config.trustStoreType, trustPassword,
                    "TrustStore");
            ensureTrustAnchors(trustStore);

            KeyStore identityStore = identity.store();
            char[] effectiveKeyPassword = identity.password();
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(identityStore, effectiveKeyPassword);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);

            SSLContext ssl = SSLContext.getInstance("TLS");
            ssl.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            SSLParameters parameters = new SSLParameters();
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            parameters.setProtocols(new String[]{"TLSv1.3", "TLSv1.2"});
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(connectTimeoutSeconds))
                    .sslContext(ssl)
                    .sslParameters(parameters)
                    .build();
            ClientCertificateInfo info = certificateInfo(identity.leaf());
            String fingerprintAfterLoad = materialFingerprint();
            if (!fingerprintBeforeLoad.equals(fingerprintAfterLoad)) {
                throw new MtlsClientException("mTLS Secret Volume 在加载过程中发生变化，将在下一次检查时重试");
            }
            return new ClientState(client, info, fingerprintAfterLoad);
        } catch (MtlsClientException e) {
            throw e;
        } catch (Exception e) {
            throw new MtlsClientException("mTLS Secret Volume 内容无效或无法加载", e);
        } finally {
            clear(keyPassword);
            clear(trustPassword);
        }
    }

    private Identity loadKeyStoreIdentity(Path path, String type, char[] password) throws Exception {
        KeyStore store = loadStore(path, type, password, "KeyStore");
        Enumeration<String> aliases = store.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (!store.isKeyEntry(alias)) continue;
            Key key = store.getKey(alias, password);
            Certificate[] chain = store.getCertificateChain(alias);
            if (key instanceof PrivateKey privateKey && chain != null && chain.length > 0
                    && chain[0] instanceof X509Certificate leaf) {
                return new Identity(store, password, privateKey, leaf);
            }
        }
        throw new MtlsClientException("客户端 KeyStore 中没有可用的私钥证书条目");
    }

    private Identity loadPemIdentity(Path certificatePath, Path privateKeyPath) throws Exception {
        byte[] certificateBytes = readSecret(certificatePath);
        byte[] privateKeyBytes = readSecret(privateKeyPath);
        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            List<Certificate> certificates = new ArrayList<>(factory.generateCertificates(
                    new ByteArrayInputStream(certificateBytes)));
            if (certificates.isEmpty() || !(certificates.get(0) instanceof X509Certificate leaf)) {
                throw new MtlsClientException("PEM 客户端证书文件不包含 X.509 证书");
            }
            PrivateKey privateKey = parsePkcs8PrivateKey(privateKeyBytes, leaf.getPublicKey().getAlgorithm());
            char[] inMemoryPassword = new char[0];
            KeyStore store = KeyStore.getInstance("PKCS12");
            store.load(null, inMemoryPassword);
            store.setKeyEntry("client", privateKey, inMemoryPassword, certificates.toArray(Certificate[]::new));
            return new Identity(store, inMemoryPassword, privateKey, leaf);
        } finally {
            Arrays.fill(certificateBytes, (byte) 0);
            Arrays.fill(privateKeyBytes, (byte) 0);
        }
    }

    private PrivateKey parsePkcs8PrivateKey(byte[] pemBytes, String preferredAlgorithm) throws Exception {
        String pem = new String(pemBytes, StandardCharsets.US_ASCII);
        if (pem.contains("BEGIN RSA PRIVATE KEY") || pem.contains("BEGIN EC PRIVATE KEY")) {
            throw new MtlsClientException("PEM 私钥必须使用未加密 PKCS#8 PRIVATE KEY 格式");
        }
        String base64 = pem.replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replaceAll("\\s", "");
        if (base64.isBlank()) throw new MtlsClientException("PEM 私钥文件不包含 PKCS#8 PRIVATE KEY");
        byte[] encoded;
        try {
            encoded = Base64.getDecoder().decode(base64);
        } catch (IllegalArgumentException e) {
            throw new MtlsClientException("PEM 私钥 Base64 无效");
        }
        try {
            List<String> algorithms = new ArrayList<>();
            algorithms.add(preferredAlgorithm);
            for (String algorithm : List.of("RSA", "EC", "Ed25519", "DSA")) {
                if (!algorithms.contains(algorithm)) algorithms.add(algorithm);
            }
            for (String algorithm : algorithms) {
                try {
                    return KeyFactory.getInstance(algorithm).generatePrivate(new PKCS8EncodedKeySpec(encoded));
                } catch (GeneralSecurityException ignored) {
                    // 尝试下一种标准算法；不会输出私钥内容。
                }
            }
            throw new MtlsClientException("PEM 私钥算法不受支持或内容无效");
        } finally {
            Arrays.fill(encoded, (byte) 0);
        }
    }

    private KeyStore loadStore(Path path, String type, char[] password, String label) throws Exception {
        if (type == null || !("PKCS12".equalsIgnoreCase(type) || "JKS".equalsIgnoreCase(type))) {
            throw new MtlsClientException(label + " 类型只允许 PKCS12 或 JKS");
        }
        byte[] bytes = readSecret(path);
        try {
            KeyStore store = KeyStore.getInstance(type.toUpperCase(Locale.ROOT));
            store.load(new ByteArrayInputStream(bytes), password);
            return store;
        } finally {
            Arrays.fill(bytes, (byte) 0);
        }
    }

    private void ensureTrustAnchors(KeyStore store) throws Exception {
        Enumeration<String> aliases = store.aliases();
        while (aliases.hasMoreElements()) {
            Certificate certificate = store.getCertificate(aliases.nextElement());
            if (certificate instanceof X509Certificate) return;
        }
        throw new MtlsClientException("TrustStore 中没有可用的 X.509 信任证书");
    }

    private void validateIdentity(Identity identity, Instant now) throws Exception {
        try {
            identity.leaf().checkValidity(Date.from(now));
        } catch (Exception e) {
            throw new MtlsClientException("客户端证书已过期或尚未生效");
        }
        List<String> usages = identity.leaf().getExtendedKeyUsage();
        if (usages != null && !usages.contains(CLIENT_AUTH_OID)) {
            throw new MtlsClientException("客户端证书不允许用于 TLS clientAuth");
        }
        String signatureAlgorithm = signatureAlgorithm(identity.privateKey().getAlgorithm());
        Signature signature = Signature.getInstance(signatureAlgorithm);
        byte[] challenge = "aliyun-data-collector-mtls-key-check".getBytes(StandardCharsets.US_ASCII);
        signature.initSign(identity.privateKey());
        signature.update(challenge);
        byte[] signed = signature.sign();
        signature.initVerify(identity.leaf().getPublicKey());
        signature.update(challenge);
        if (!signature.verify(signed)) throw new MtlsClientException("客户端私钥与证书不匹配");
        Arrays.fill(signed, (byte) 0);
    }

    private String signatureAlgorithm(String keyAlgorithm) {
        return switch (keyAlgorithm.toUpperCase(Locale.ROOT)) {
            case "RSA", "RSASSA-PSS" -> "SHA256withRSA";
            case "EC" -> "SHA256withECDSA";
            case "ED25519" -> "Ed25519";
            case "DSA" -> "SHA256withDSA";
            default -> throw new MtlsClientException("不支持的客户端私钥算法");
        };
    }

    private ClientCertificateInfo certificateInfo(X509Certificate certificate) throws Exception {
        String fingerprint = HexFormat.ofDelimiter(":").withUpperCase()
                .formatHex(MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded()));
        return new ClientCertificateInfo(fingerprint, subjectSummary(certificate),
                certificate.getNotBefore().toInstant(), certificate.getNotAfter().toInstant());
    }

    private String subjectSummary(X509Certificate certificate) {
        try {
            LdapName name = new LdapName(certificate.getSubjectX500Principal().getName());
            return name.getRdns().stream()
                    .filter(rdn -> "CN".equalsIgnoreCase(rdn.getType()))
                    .map(rdn -> "CN=" + safeText(String.valueOf(rdn.getValue())))
                    .findFirst().orElse("CN=<absent>");
        } catch (Exception e) {
            return "CN=<unavailable>";
        }
    }

    private void warnIfExpiring(ClientCertificateInfo certificate, Instant now) {
        Instant warningAt = certificate.notAfter().minus(Duration.ofDays(config.certificateExpiryWarningDays));
        if (now.isBefore(warningAt) || certificate.fingerprintSha256().equals(warnedFingerprint)) return;
        warnedFingerprint = certificate.fingerprintSha256();
        logCertificate("MTLS_CLIENT_CERTIFICATE_EXPIRING", certificate, warningLog);
    }

    private void ensureCurrentlyValid(ClientCertificateInfo certificate, Instant now) throws IOException {
        if (now.isBefore(certificate.notBefore()) || !now.isBefore(certificate.notAfter())) {
            logCertificate("MTLS_CLIENT_CERTIFICATE_INVALID", certificate, warningLog);
            throw new IOException("客户端证书已过期或尚未生效，mTLS 上传已失败关闭");
        }
    }

    private void logCertificate(String code, ClientCertificateInfo certificate, Consumer<String> target) {
        target.accept(code + " fingerprint_sha256=" + certificate.fingerprintSha256()
                + " subject=" + certificate.subjectSummary()
                + " not_after=" + certificate.notAfter());
    }

    private String materialFingerprint() {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            if ("PEM".equalsIgnoreCase(config.clientAuthMode)) {
                updateDigest(digest, requiredFile(config.clientCertificatePath));
                updateDigest(digest, requiredFile(config.privateKeyPath));
            } else {
                updateDigest(digest, requiredFile(config.keyStorePath));
            }
            updateDigest(digest, requiredFile(config.trustStorePath));
            return HexFormat.of().formatHex(digest.digest());
        } catch (MtlsClientException e) {
            throw e;
        } catch (Exception e) {
            throw new MtlsClientException("mTLS Secret Volume 更新检查失败", e);
        }
    }

    private void updateDigest(MessageDigest digest, Path path) throws IOException {
        byte[] bytes = readSecret(path);
        try {
            digest.update(bytes);
        } finally {
            Arrays.fill(bytes, (byte) 0);
        }
    }

    private byte[] readSecret(Path path) throws IOException {
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new MtlsClientException("必需的 mTLS Secret Volume 文件不存在或不可读");
        }
        long size = Files.size(path);
        if (size <= 0 || size > MAX_SECRET_FILE_BYTES) {
            throw new MtlsClientException("mTLS Secret Volume 文件为空或超过安全大小限制");
        }
        return Files.readAllBytes(path);
    }

    private Path requiredFile(String value) {
        if (value == null || value.isBlank()) throw new MtlsClientException("mTLS Secret Volume 文件路径未配置");
        return Path.of(value).toAbsolutePath().normalize();
    }

    private char[] requiredPassword(String envName) {
        if (envName == null || envName.isBlank()) throw new MtlsClientException("mTLS 密码环境变量名未配置");
        char[] password = passwords.password(envName);
        if (password == null || password.length == 0) throw new MtlsClientException("mTLS 密码环境变量未设置");
        return password;
    }

    private String safeText(String value) {
        String sanitized = value.replace('\r', ' ').replace('\n', ' ').replaceAll("[\\p{Cntrl}]", "?");
        return sanitized.length() <= 120 ? sanitized : sanitized.substring(0, 120);
    }

    private static CollectorConfig.SecurityConfig requireConfig(CollectorConfig.SecurityConfig config) {
        return Objects.requireNonNull(config, "security 配置不能为空");
    }

    private void clear(char[] value) {
        if (value != null) Arrays.fill(value, '\0');
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        if (watcher != null) watcher.shutdownNow();
    }

    @FunctionalInterface
    interface PasswordProvider {
        char[] password(String environmentVariableName);
    }

    private record Identity(KeyStore store, char[] password, PrivateKey privateKey, X509Certificate leaf) {
    }

    private record ClientState(HttpClient client, ClientCertificateInfo certificate, String materialFingerprint) {
    }
}

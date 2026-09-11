package cn.kjky.collector.config;

import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import cn.kjky.collector.output.OutputSession;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigExpansionTest {
    private final ConfigLoader loader = new ConfigLoader();
    private final ConfigValidator validator = new ConfigValidator();

    @Test
    void loadsAndValidatesCompleteP002Configuration() throws Exception {
        CollectorConfig config = loadValid();

        assertThat(config.collector.collectorId).isEqualTo("aliyun-collector-test01");
        assertThat(config.scheduler.compensationIntervalSeconds).isEqualTo(600);
        assertThat(config.upload.maxCompressedBytes).isEqualTo(2L * 1024 * 1024);
        assertThat(config.security.clientAuthMode).isEqualTo("KEYSTORE");
        assertThat(config.security.certificateExpiryWarningDays).isEqualTo(30);
        assertThat(config.security.certificateReloadIntervalSeconds).isEqualTo(30);
        assertThat(config.storage.outboxDir).isEqualTo("outbox");
        assertThat(config.storage.diskResumeWatermarkPercent).isEqualTo(75);
        assertThat(config.storage.diskCheckIntervalSeconds).isEqualTo(10);
        assertThat(config.control.taskPollIntervalSeconds).isEqualTo(10);
        assertThat(config.effectiveDataRoot()).isEqualTo("./target/config-test-data");
        assertThat(validator.validate(config, false)).isEmpty();
    }

    @Test
    void supportsLegacyOutputRootAndRejectsConflictingRoots() throws Exception {
        CollectorConfig config = loadValid();
        config.storage.dataRoot = null;
        config.outputRoot = "./target/legacy-data";
        assertThat(config.effectiveDataRoot()).isEqualTo("./target/legacy-data");
        assertThat(validator.validate(config, false)).isEmpty();

        config.storage.dataRoot = "./target/new-data";
        assertThat(validator.validate(config, false))
                .contains("outputRoot 与 storage.dataRoot 不能同时配置为不同目录");
    }

    @Test
    void rejectsInvalidIdentityUrlsCapacityPathsAndDiskThresholds() throws Exception {
        CollectorConfig config = loadValid();
        config.collector.collectorId = "bad/id";
        config.collector.collectorType = "EBPF";
        config.upload.ingestUrl = "not-a-url";
        config.upload.maxRecords = 0;
        config.upload.maxCompressedBytes = 4096;
        config.upload.maxRequestBytes = 2048;
        config.upload.maxInFlightBatches = 0;
        config.upload.retryInitialDelayMillis = 0;
        config.upload.retryMaxDelayMillis = 90_000_000;
        config.upload.retryJitterRatio = 1.1;
        config.upload.retryWarningAttempts = 0;
        config.storage.outboxDir = "../outside";
        config.storage.stateDir = "../state-outside";
        config.storage.diskWarningWatermarkPercent = 95;
        config.storage.diskCriticalWatermarkPercent = 90;
        config.storage.diskResumeWatermarkPercent = 99;
        config.storage.diskCheckIntervalSeconds = 0;
        config.control.taskClaimUrl = "ftp://control/jobs";

        assertThat(validator.validate(config, false))
                .contains("collector.collectorId 只能包含字母、数字、点、下划线和连字符，且必须以字母或数字开头")
                .contains("collector.collectorType 只能是 ALIYUN")
                .contains("upload.ingestUrl 必须是包含主机名的 HTTP 或 HTTPS 绝对 URL")
                .contains("upload.maxRecords 必须在 1..10000")
                .contains("upload.maxCompressedBytes 不能大于 maxRequestBytes")
                .contains("upload.maxInFlightBatches 必须在 1..32")
                .contains("upload.retryInitialDelayMillis 必须在 1..3600000")
                .contains("upload.retryMaxDelayMillis 必须不小于 retryInitialDelayMillis 且不大于 86400000")
                .contains("upload.retryJitterRatio 必须在 0..1")
                .contains("upload.retryWarningAttempts 必须在 1..1000000")
                .contains("storage.outboxDir 必须是 dataRoot 内的相对路径，且不能包含越界跳转")
                .contains("storage.stateDir 必须是 dataRoot 内的相对路径，且不能包含越界跳转")
                .contains("storage 磁盘水位必须满足 diskResumeWatermarkPercent < diskWarningWatermarkPercent < diskCriticalWatermarkPercent")
                .contains("storage.diskCheckIntervalSeconds 必须在 1..3600")
                .contains("control.taskClaimUrl 必须是包含主机名的 HTTP 或 HTTPS 绝对 URL");
    }

    @Test
    void rejectsUnexpectedIngestPath() throws Exception {
        CollectorConfig config = loadValid();
        config.upload.ingestUrl = "https://cc-ingest.example.invalid/api/events";

        assertThat(validator.validate(config, false))
                .contains("upload.ingestUrl 必须使用固定路径 /collector/v1/events:batch，且不能包含查询参数或片段");
    }

    @Test
    void rejectsInvalidCapacityResumeThresholdAndCheckInterval() throws Exception {
        CollectorConfig config = loadValid();
        config.storage.diskResumeWatermarkPercent = 0;
        config.storage.diskCheckIntervalSeconds = 3601;

        assertThat(validator.validate(config, false))
                .contains("storage.diskResumeWatermarkPercent 必须在 1..99")
                .contains("storage.diskCheckIntervalSeconds 必须在 1..3600");
    }

    @Test
    void rejectsOverlappingPersistentDirectories() throws Exception {
        CollectorConfig config = loadValid();
        config.storage.outboxDir = "persistent";
        config.storage.stateDir = "persistent/state";
        config.storage.rawPersistence.directory = "persistent/debug-raw";

        assertThat(validator.validate(config, false))
                .contains("storage.outboxDir 和 stateDir 不能相同或相互嵌套")
                .contains("storage.rawPersistence.directory 不能与 outboxDir 或 stateDir 相同或相互嵌套");
    }

    @Test
    void validatesPemAndRejectsMixedClientIdentityModes() throws Exception {
        CollectorConfig config = loadValid();
        config.security.clientAuthMode = "PEM";
        config.security.clientCertificatePath = "./secrets/client.crt";
        config.security.privateKeyPath = "./secrets/client.key";
        config.security.keyStorePath = null;
        config.security.keyStorePasswordEnv = null;
        assertThat(validator.validate(config, false)).isEmpty();

        config.security.keyStorePath = "./secrets/client.p12";
        assertThat(validator.validate(config, false))
                .contains("security.clientAuthMode=PEM 时不能配置 KeyStore");
    }

    @Test
    void productionUploadRequiresMtlsAndValidCertificateCheckIntervals() throws Exception {
        CollectorConfig config = loadValid();
        config.runtime.deploymentMode = "PRODUCTION";
        config.storage.rawPersistence.enabled = false;
        config.security.mtlsEnabled = false;
        config.security.certificateExpiryWarningDays = 0;
        config.security.certificateReloadIntervalSeconds = 86401;

        assertThat(validator.validate(config, false))
                .contains("PRODUCTION 模式启用上传时必须启用 security.mtlsEnabled")
                .contains("security.certificateExpiryWarningDays 必须在 1..3650")
                .contains("security.certificateReloadIntervalSeconds 必须在 1..86400");
    }

    @Test
    void reportsMissingTopLevelSectionsInsteadOfThrowingNullPointer() throws Exception {
        CollectorConfig config = loadValid();
        config.collector = null;
        config.runtime = null;
        config.scheduler = null;
        config.upload = null;
        config.security = null;
        config.storage = null;
        config.control = null;
        config.arms = null;
        config.sls = null;

        assertThat(validator.validate(config, false))
                .contains("collector 不能为空", "runtime 不能为空", "scheduler 不能为空")
                .contains("upload 不能为空", "security 不能为空", "storage 不能为空")
                .contains("control 不能为空", "arms 不能为空", "sls 不能为空");
    }

    @Test
    void rejectsUnknownYamlProperties() throws Exception {
        Path yaml = writeYaml("projectCode: test\nunknownP002Field: true\n");
        assertThatThrownBy(() -> loader.load(yaml))
                .isInstanceOf(UnrecognizedPropertyException.class);
    }

    @Test
    void loadsBothExampleSchemasWithoutUnknownProperties() throws Exception {
        CollectorConfig rpc = loader.load(Path.of("config/site.example.yaml"));
        CollectorConfig dataset = loader.load(Path.of("config/arms-dataset.example.yaml"));

        assertThat(rpc.storage.dataRoot).isEqualTo("./data-package");
        assertThat(rpc.runtime.deploymentMode).isEqualTo("DEVELOPMENT");
        assertThat(rpc.storage.rawPersistence.enabled).isTrue();
        assertThat(rpc.upload.maxRecords).isEqualTo(1000);
        assertThat(dataset.arms.mode).isEqualTo("DATASET");
        assertThat(dataset.scheduler.compensationLookbackSeconds).isEqualTo(1800);
    }

    @Test
    void productionRequiresRawDisabledAndFailsClosedUntilOutboxExists() throws Exception {
        CollectorConfig config = loadValid();
        config.runtime.deploymentMode = "PRODUCTION";
        assertThat(validator.validate(config, false))
                .contains("PRODUCTION 模式必须关闭 storage.rawPersistence.enabled");

        config.storage.rawPersistence.enabled = false;
        Path dataRoot = Path.of("target/test-production", UUID.randomUUID().toString());
        config.storage.dataRoot = dataRoot.toString();
        assertThat(validator.validate(config, false)).isEmpty();
        assertThatThrownBy(() -> new OutputSession(config, Instant.EPOCH, Instant.EPOCH.plusSeconds(60)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("尚未完成 Outbox");
        assertThat(Files.exists(dataRoot.resolve("raw"))).isFalse();
    }

    @Test
    void developmentCanUseConfiguredRawDirectory() throws Exception {
        CollectorConfig config = loadValid();
        Path dataRoot = Path.of("target/test-development", UUID.randomUUID().toString());
        config.storage.dataRoot = dataRoot.toString();
        config.storage.rawPersistence.directory = "debug-raw";

        new OutputSession(config, Instant.EPOCH, Instant.EPOCH.plusSeconds(60));

        assertThat(Files.isDirectory(dataRoot.resolve("debug-raw/arms"))).isTrue();
        assertThat(Files.isDirectory(dataRoot.resolve("debug-raw/sls"))).isTrue();
    }

    @Test
    void loadsAndValidatesNormalizationPolicies() throws Exception {
        CollectorConfig config = loadValid();
        var policy = config.sls.projects.get(0).logstores.get(0).normalization;
        assertThat(policy.payloadFields).containsExactly("message", "trace_id", "service_id");
        assertThat(policy.identityFields).containsExactly("request_id");

        policy.payloadFields = new ArrayList<>(java.util.List.of("message", "MESSAGE", " "));
        assertThat(validator.validate(config, false))
                .contains("sls.project01.logstore01.normalization.payloadFields 包含重复字段: MESSAGE")
                .contains("sls.project01.logstore01.normalization.payloadFields 不能包含空字段名");

        policy.payloadFields = null;
        policy.identityFields = null;
        assertThat(validator.validate(config, false))
                .contains("sls.project01.logstore01.normalization.payloadFields 不能为空")
                .contains("sls.project01.logstore01.normalization.identityFields 不能为空");
    }

    private CollectorConfig loadValid() throws Exception {
        return loader.load(Path.of("src/test/resources/config/p0-02-valid.yaml"));
    }

    private Path writeYaml(String content) throws Exception {
        Path directory = Path.of("target/test-config", UUID.randomUUID().toString());
        Files.createDirectories(directory);
        Path yaml = directory.resolve("site.yaml");
        Files.writeString(yaml, content);
        return yaml;
    }
}

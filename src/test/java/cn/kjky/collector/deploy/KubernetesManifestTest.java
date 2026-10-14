package cn.kjky.collector.deploy;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.config.ConfigValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 防止生产部署模板退化为公网镜像、明文 Secret 或不安全的 Kubernetes 配置。 */
class KubernetesManifestTest {
    private static final Path ROOT = Path.of("").toAbsolutePath().normalize();
    private static final Path DEPLOY = ROOT.resolve("deploy");
    private final ObjectMapper yaml = new ObjectMapper(new YAMLFactory());

    @Test
    void dockerfileRequiresExplicitInternalJava17ImageAndRunsAsNonRoot() throws IOException {
        String dockerfile = Files.readString(ROOT.resolve("Dockerfile")).replace("\r\n", "\n");

        assertTrue(dockerfile.contains("ARG RUNTIME_IMAGE\nFROM ${RUNTIME_IMAGE}"),
                "基础镜像必须由无默认值构建参数传入");
        assertFalse(dockerfile.contains("ARG RUNTIME_IMAGE="), "不得提供可能回退公网的默认镜像");
        assertFalse(dockerfile.matches("(?is).*(docker\\.io|eclipse-temurin|openjdk:|amazoncorretto|maven:).*"),
                "不得默认引用公网基础镜像");
        assertTrue(dockerfile.contains("USER 10001:10001"));
        assertTrue(dockerfile.contains("ENTRYPOINT [\"java\", \"-jar\""), "必须使用 exec 形式入口");
        assertTrue(dockerfile.contains("-XX:MaxRAMPercentage=75.0"));
        assertFalse(dockerfile.matches("(?is).*(curl|wget|apt-get|yum install|apk add).*"),
                "运行镜像不得下载依赖或假定存在诊断工具");

        String dockerignore = Files.readString(ROOT.resolve(".dockerignore"));
        assertTrue(dockerignore.startsWith("**"));
        assertTrue(dockerignore.contains("!target/aliyun-data-collector-0.1.0-SNAPSHOT.jar"));
    }

    @Test
    void deploymentIsSingleReplicaRecreateAndHardened() throws IOException {
        JsonNode deployment = one("deployment.yaml");
        JsonNode spec = deployment.path("spec");
        JsonNode podSpec = spec.path("template").path("spec");
        JsonNode container = podSpec.path("containers").get(0);

        assertEquals("Deployment", deployment.path("kind").asText());
        assertEquals(1, spec.path("replicas").asInt());
        assertEquals("Recreate", spec.path("strategy").path("type").asText());
        assertEquals("aliyun-data-collector", podSpec.path("serviceAccountName").asText());
        assertFalse(podSpec.path("automountServiceAccountToken").asBoolean(true));
        assertEquals(90, podSpec.path("terminationGracePeriodSeconds").asInt());
        assertTrue(podSpec.path("securityContext").path("runAsNonRoot").asBoolean());
        assertEquals("RuntimeDefault", podSpec.path("securityContext").path("seccompProfile").path("type").asText());

        JsonNode security = container.path("securityContext");
        assertTrue(security.path("runAsNonRoot").asBoolean());
        assertFalse(security.path("allowPrivilegeEscalation").asBoolean(true));
        assertTrue(security.path("readOnlyRootFilesystem").asBoolean());
        assertEquals("ALL", security.path("capabilities").path("drop").get(0).asText());
        assertEquals("RuntimeDefault", security.path("seccompProfile").path("type").asText());
        assertFalse(security.has("privileged"));
        assertFalse(podSpec.has("hostNetwork"));
        assertFalse(podSpec.has("hostPID"));

        String image = container.path("image").asText();
        assertTrue(image.startsWith("registry.internal.invalid/"));
        assertTrue(image.matches(".+@sha256:[0-9a-f]{64}"), "应用镜像必须使用固定 digest 形式");
        assertEquals("daemon", container.path("args").get(0).asText());
        assertEquals("/etc/aliyun-data-collector/site.yaml", container.path("args").get(2).asText());
        assertTrue(container.path("resources").path("requests").has("ephemeral-storage"));
        assertTrue(container.path("resources").path("limits").has("ephemeral-storage"));
        assertFalse(container.has("ports"), "采集端不应暴露入站端口");
        assertFalse(container.has("livenessProbe"), "可靠健康检查未实现前不得伪造 liveness");
        assertFalse(container.has("readinessProbe"), "可靠健康检查未实现前不得伪造 readiness");

        JsonNode configMount = named(container.path("volumeMounts"), "config");
        JsonNode tlsMount = named(container.path("volumeMounts"), "tls");
        JsonNode dataMount = named(container.path("volumeMounts"), "data");
        assertTrue(configMount.path("readOnly").asBoolean());
        assertTrue(tlsMount.path("readOnly").asBoolean());
        assertEquals("/var/lib/aliyun-data-collector", dataMount.path("mountPath").asText());

        JsonNode dataVolume = named(podSpec.path("volumes"), "data");
        JsonNode tlsVolume = named(podSpec.path("volumes"), "tls");
        assertEquals("aliyun-data-collector-data",
                dataVolume.path("persistentVolumeClaim").path("claimName").asText());
        assertEquals("aliyun-data-collector-tls", tlsVolume.path("secret").path("secretName").asText());
        assertFalse(containsVolumeType(podSpec.path("volumes"), "hostPath"));
    }

    @Test
    void productionConfigUsesOnlyOutboxAndStatePersistenceBoundaries() throws IOException {
        JsonNode configMap = one("configmap.yaml");
        String siteYaml = configMap.path("data").path("site.yaml").asText();
        CollectorConfig config = yaml.readValue(siteYaml, CollectorConfig.class);

        assertEquals("PRODUCTION", config.runtime.deploymentMode);
        assertFalse(config.storage.rawPersistence.enabled);
        assertTrue(config.upload.enabled);
        assertTrue(config.control.enabled);
        assertEquals("/var/lib/aliyun-data-collector", config.storage.dataRoot);
        assertEquals("outbox", config.storage.outboxDir);
        assertEquals("state", config.storage.stateDir);
        assertEquals(10L * 1024 * 1024 * 1024, config.storage.outboxQuota.defaultBytes);
        assertEquals(1024L * 1024 * 1024, config.storage.outboxQuota.minBytes);
        assertEquals(20L * 1024 * 1024 * 1024, config.storage.outboxQuota.maxBytes);
        assertEquals(60, config.scheduler.shutdownGraceSeconds);
        assertTrue(new ConfigValidator().validate(config, false).isEmpty(),
                () -> "生产模板结构校验失败: " + new ConfigValidator().validate(config, false));

        String lower = siteYaml.toLowerCase();
        assertFalse(lower.contains("accesskeyid: "), "ConfigMap 不能保存 AccessKey 值");
        assertFalse(lower.contains("accesskeysecret: "), "ConfigMap 不能保存 AccessKey Secret 值");
        assertFalse(lower.contains("privatekey:"), "ConfigMap 不能保存客户端私钥");
        assertFalse(lower.contains("token:"), "ConfigMap 不能保存 Token");

        JsonNode deployment = one("deployment.yaml");
        assertTrue(deployment.path("spec").path("template").path("spec")
                .path("terminationGracePeriodSeconds").asInt() > config.scheduler.shutdownGraceSeconds);
    }

    @Test
    void secretExampleContainsOnlyPlaceholdersAndIsNotKustomized() throws IOException {
        List<JsonNode> secrets = all("secret.example.yaml");
        assertEquals(2, secrets.size());
        for (JsonNode secret : secrets) {
            assertEquals("Secret", secret.path("kind").asText());
            assertFalse(secret.has("data"), "示例不得包含真实 Base64 data");
            JsonNode stringData = secret.path("stringData");
            assertTrue(stringData.isObject());
            stringData.fields().forEachRemaining(entry ->
                    assertTrue(entry.getValue().asText().startsWith("REPLACE_"),
                            () -> entry.getKey() + " 必须是明确占位符"));
        }

        JsonNode kustomization = one("kustomization.yaml");
        Set<String> resources = new HashSet<>();
        kustomization.path("resources").forEach(node -> resources.add(node.asText()));
        assertFalse(resources.contains("secret.example.yaml"), "占位 Secret 不得被默认应用");
    }

    @Test
    void manifestsExposeNoServiceOrIngressAndNetworkPolicyIsDefaultDenyEgressOnly() throws IOException {
        Set<String> kinds = new HashSet<>();
        for (String file : List.of("namespace.yaml", "serviceaccount.yaml", "configmap.yaml",
                "pvc.yaml", "deployment.yaml", "network-policy.yaml")) {
            for (JsonNode document : all(file)) kinds.add(document.path("kind").asText());
        }
        assertFalse(kinds.contains("Service"));
        assertFalse(kinds.contains("Ingress"));

        List<JsonNode> policies = all("network-policy.yaml");
        assertEquals(2, policies.size());
        JsonNode deny = policies.stream()
                .filter(node -> "default-deny-all".equals(node.path("metadata").path("name").asText()))
                .findFirst().orElseThrow();
        assertEquals(0, deny.path("spec").path("ingress").size());
        assertEquals(0, deny.path("spec").path("egress").size());

        JsonNode allow = policies.stream()
                .filter(node -> "aliyun-data-collector-egress".equals(node.path("metadata").path("name").asText()))
                .findFirst().orElseThrow();
        assertFalse(allow.path("spec").has("ingress"), "采集 Pod 不得有入站放行");
        assertEquals(4, allow.path("spec").path("egress").size(),
                "只允许 DNS、cc-ingest、cc-control 和 ARMS/SLS 四类出站");
        String policyText = Files.readString(DEPLOY.resolve("network-policy.yaml"));
        assertTrue(policyText.contains("192.0.2.0/24"));
        assertTrue(policyText.contains("198.51.100.0/24"));
        assertFalse(policyText.contains("0.0.0.0/0"));
    }

    @Test
    void pvcServiceAccountAndKustomizationAreConsistent() throws IOException {
        JsonNode pvc = one("pvc.yaml");
        assertEquals("PersistentVolumeClaim", pvc.path("kind").asText());
        assertEquals("ReadWriteOnce", pvc.path("spec").path("accessModes").get(0).asText());

        JsonNode serviceAccount = one("serviceaccount.yaml");
        assertEquals("ServiceAccount", serviceAccount.path("kind").asText());
        assertFalse(serviceAccount.path("automountServiceAccountToken").asBoolean(true));

        JsonNode kustomization = one("kustomization.yaml");
        Set<String> actual = new HashSet<>();
        kustomization.path("resources").forEach(node -> actual.add(node.asText()));
        assertEquals(Set.of("namespace.yaml", "serviceaccount.yaml", "configmap.yaml", "pvc.yaml",
                "deployment.yaml", "network-policy.yaml"), actual);
    }

    private JsonNode one(String file) throws IOException {
        List<JsonNode> documents = all(file);
        assertEquals(1, documents.size(), file + " 应只有一个 YAML 文档");
        return documents.get(0);
    }

    private List<JsonNode> all(String file) throws IOException {
        List<JsonNode> documents = new ArrayList<>();
        try (MappingIterator<JsonNode> iterator = yaml.readerFor(JsonNode.class)
                .readValues(DEPLOY.resolve(file).toFile())) {
            while (iterator.hasNextValue()) {
                JsonNode value = iterator.nextValue();
                if (value != null && !value.isNull()) documents.add(value);
            }
        }
        return documents;
    }

    private JsonNode named(JsonNode array, String name) {
        assertNotNull(array);
        for (JsonNode node : array) {
            if (name.equals(node.path("name").asText())) return node;
        }
        throw new AssertionError("未找到 name=" + name);
    }

    private boolean containsVolumeType(JsonNode volumes, String field) {
        for (JsonNode volume : volumes) if (volume.has(field)) return true;
        return false;
    }
}

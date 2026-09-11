package cn.kjky.collector.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** 校验哈希规范和提交给中控开发使用的 OpenAPI 文件。 */
class IngestContractTest {

    @Test
    void canonicalHashDoesNotDependOnObjectKeyInsertionOrder() {
        ObjectMapper json = new ObjectMapper();
        JsonNode first = json.createObjectNode().put("service", "order").put("count", 3);
        JsonNode second = json.createObjectNode().put("count", 3).put("service", "order");

        assertThat(IngestJson.sha256Canonical(first)).isEqualTo(IngestJson.sha256Canonical(second));
    }

    @Test
    void openApiContainsFrozenBatchPathAndFields() throws Exception {
        ObjectMapper yaml = new ObjectMapper(new YAMLFactory());
        JsonNode root = yaml.readTree(Path.of("docs/openapi/cc-ingest-v1.openapi.yaml").toFile());

        assertThat(root.path("openapi").asText()).isEqualTo("3.1.0");
        assertThat(root.path("paths").path("/collector/v1/events:batch").path("post").isObject()).isTrue();
        JsonNode required = root.path("components").path("schemas").path("BatchEnvelope").path("required");
        assertThat(required).anySatisfy(field -> assertThat(field.asText()).isEqualTo("is_last_batch"));
        assertThat(required).anySatisfy(field -> assertThat(field.asText()).isEqualTo("content_sha256"));
    }
}

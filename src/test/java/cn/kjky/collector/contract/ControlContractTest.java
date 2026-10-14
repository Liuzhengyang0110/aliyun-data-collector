package cn.kjky.collector.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** 校验提交给 cc-control 开发使用的控制、任务状态和节点状态 OpenAPI 契约。 */
class ControlContractTest {
    private static final String OPENAPI = "/contracts/cc-control-v1.openapi.yaml";

    @Test
    void openApiParsesAndFreezesOutboundMtlsPaths() throws Exception {
        JsonNode root = load();

        assertThat(root.path("openapi").asText()).isEqualTo("3.1.0");
        assertThat(root.path("info").path("version").asText()).isEqualTo("1.0");
        assertThat(root.path("servers").path(0).path("url").asText()).endsWith(".invalid");
        assertThat(root.path("components").path("securitySchemes").path("mutualTLS").path("type").asText())
                .isEqualTo("mutualTLS");

        for (String path : Set.of(
                "/collector/v1/jobs:claim",
                "/collector/v1/jobs/{job_id}/status",
                "/collector/v1/status")) {
            JsonNode post = root.path("paths").path(path).path("post");
            assertThat(post.isObject()).as(path).isTrue();
            assertThat(post.path("security").path(0).has("mutualTLS")).as(path).isTrue();
        }
    }

    @Test
    void jobEnvelopeFreezesCommandsRequiredFieldsAndStrictParameters() throws Exception {
        JsonNode root = load();

        assertThat(textValues(schema(root, "CommandType").path("enum"))).containsExactlyInAnyOrder(
                "PAUSE_COLLECTION", "RESUME_COLLECTION", "SET_OUTBOX_QUOTA");
        assertThat(textValues(schema(root, "JobExecutionStatus").path("enum"))).containsExactlyInAnyOrder(
                "RECEIVED", "RUNNING", "SUCCEEDED", "FAILED", "REJECTED");
        assertThat(required(schema(root, "JobEnvelopeBase"))).contains(
                "schema_version", "job_id", "collector_id", "site_id", "config_version",
                "created_at", "expires_at");
        assertThat(required(schema(root, "JobEnvelopeBase"))).hasSize(7);

        assertJobVariant(root, "PauseCollectionJob", "PAUSE_COLLECTION", "PauseCollectionParameters");
        assertJobVariant(root, "ResumeCollectionJob", "RESUME_COLLECTION", "ResumeCollectionParameters");
        assertJobVariant(root, "SetOutboxQuotaJob", "SET_OUTBOX_QUOTA", "SetOutboxQuotaParameters");

        JsonNode quota = schema(root, "SetOutboxQuotaParameters");
        assertThat(quota.path("additionalProperties").asBoolean()).isFalse();
        assertThat(required(quota)).containsExactly("requested_quota_bytes");
        assertThat(fieldNames(quota.path("properties"))).containsExactly("requested_quota_bytes");
        assertThat(quota.path("properties").path("requested_quota_bytes").path("minimum").asLong()).isEqualTo(1L);

        assertThat(textValues(schema(root, "RejectReason").path("enum"))).contains(
                "JOB_ID_CONFLICT", "EXPIRED_JOB", "STALE_CONFIG_VERSION", "UNKNOWN_COMMAND",
                "INVALID_PARAMETERS", "IDENTITY_MISMATCH");

        JsonNode jobStatus = schema(root, "JobStatusUpdate");
        assertThat(required(jobStatus)).containsExactlyInAnyOrder(
                "schema_version", "job_id", "collector_id", "site_id", "command_type",
                "config_version", "status", "occurred_at");
        assertThat(jobStatus.path("properties").path("command_type").path("$ref").asText())
                .isEqualTo("#/components/schemas/ReportedCommandType");
        assertThat(jobStatus.path("allOf").toString()).contains(
                "SET_OUTBOX_QUOTA", "requested_quota_bytes", "effective_quota_bytes",
                "local_min_bytes", "local_max_bytes", "REJECTED", "reject_reason");
    }

    @Test
    void commandAndResultExamplesMatchTheirSchemas() throws Exception {
        JsonNode root = load();

        assertJobExample(root, "PauseCollectionJob", "PauseCollectionJob", "PAUSE_COLLECTION");
        assertJobExample(root, "ResumeCollectionJob", "ResumeCollectionJob", "RESUME_COLLECTION");
        assertJobExample(root, "SetOutboxQuotaJob", "SetOutboxQuotaJob", "SET_OUTBOX_QUOTA");

        JsonNode applied = example(root, "QuotaAppliedStatus");
        assertTopLevelSchema(applied, schema(root, "JobStatusUpdate"));
        assertEnumValue(root, "JobExecutionStatus", applied.path("status").asText());
        assertThat(applied.path("status").asText()).isEqualTo("SUCCEEDED");
        assertThat(applied.path("requested_quota_bytes").asLong())
                .isEqualTo(applied.path("effective_quota_bytes").asLong());

        JsonNode rejected = example(root, "StaleVersionRejectedStatus");
        assertTopLevelSchema(rejected, schema(root, "JobStatusUpdate"));
        assertEnumValue(root, "JobExecutionStatus", rejected.path("status").asText());
        assertEnumValue(root, "RejectReason", rejected.path("reject_reason").asText());
        assertThat(rejected.path("status").asText()).isEqualTo("REJECTED");
        assertThat(rejected.path("reject_reason").asText()).isEqualTo("STALE_CONFIG_VERSION");
    }

    @Test
    void statusSchemaAndExampleContainIdentityWatermarksBacklogAndBothCapacityViews() throws Exception {
        JsonNode root = load();
        JsonNode statusSchema = schema(root, "CollectorStatusSnapshot");
        JsonNode status = example(root, "RunningCollectorStatus");

        assertThat(required(statusSchema)).containsExactlyInAnyOrder(
                "schema_version", "collector_id", "site_id", "collector_type", "collector_version",
                "deployment_mode", "started_at", "sent_at", "config_version", "overall_status",
                "status_reasons", "sources", "upload", "capacity");
        assertTopLevelSchema(status, statusSchema);
        assertThat(textValues(schema(root, "OverallStatus").path("enum")))
                .containsExactlyInAnyOrder("RUNNING", "PAUSED", "DEGRADED");

        JsonNode source = status.path("sources").path(0);
        assertTopLevelSchema(source, schema(root, "SourceStatus"));
        assertEnumValue(root, "SourceType", source.path("source_type").asText());
        assertEnumValue(root, "ComponentState", source.path("state").asText());
        assertTopLevelSchema(source.path("watermarks").path(0), schema(root, "CollectionWatermarkStatus"));

        JsonNode upload = status.path("upload");
        assertTopLevelSchema(upload, schema(root, "UploadStatus"));
        assertEnumValue(root, "ComponentState", upload.path("state").asText());
        for (String backlog : Set.of("pending", "retry", "blocked")) {
            assertTopLevelSchema(upload.path(backlog), schema(root, "BacklogStatus"));
        }
        assertTopLevelSchema(upload.path("watermarks").path(0), schema(root, "UploadWatermarkStatus"));

        JsonNode capacity = status.path("capacity");
        assertTopLevelSchema(capacity, schema(root, "CapacityStatus"));
        assertEnumValue(root, "CapacityState", capacity.path("state").asText());
        assertThat(fieldNames(capacity)).contains(
                "pvc_total_bytes", "pvc_usable_bytes", "pvc_used_bytes", "pvc_used_percent",
                "outbox_used_bytes", "requested_quota_bytes", "effective_quota_bytes",
                "local_min_bytes", "local_max_bytes", "reserved_free_bytes", "quota_used_percent");
        assertThat(Instant.parse(status.path("sent_at").asText())).isNotNull();
    }

    @Test
    void examplesContainNoCredentialsOrRealEndpoint() throws Exception {
        JsonNode root = load();
        String examples = root.path("components").path("examples").toString().toLowerCase();

        assertThat(examples).doesNotContain(
                "access_key", "accesskey", "private_key", "password", "secret", "token", "http://", "https://");
        assertThat(root.path("servers").path(0).path("url").asText()).isEqualTo("https://cc-control.example.invalid");
    }

    private JsonNode load() throws Exception {
        try (InputStream input = getClass().getResourceAsStream(OPENAPI)) {
            assertThat(input).as("control OpenAPI test resource").isNotNull();
            return new ObjectMapper(new YAMLFactory()).readTree(input);
        }
    }

    private JsonNode schema(JsonNode root, String name) {
        JsonNode value = root.path("components").path("schemas").path(name);
        assertThat(value.isObject()).as("schema %s", name).isTrue();
        return value;
    }

    private JsonNode example(JsonNode root, String name) {
        JsonNode value = root.path("components").path("examples").path(name).path("value");
        assertThat(value.isObject()).as("example %s", name).isTrue();
        return value;
    }

    private void assertJobVariant(JsonNode root, String schemaName, String command, String parametersSchema) {
        JsonNode concreteSchema = schema(root, schemaName);
        JsonNode variant = concreteSchema.path("allOf").path(1);
        assertThat(concreteSchema.path("unevaluatedProperties").asBoolean()).isFalse();
        assertThat(required(variant)).containsExactlyInAnyOrder("command_type", "parameters");
        assertThat(variant.path("properties").path("command_type").path("const").asText()).isEqualTo(command);
        assertThat(variant.path("properties").path("parameters").path("$ref").asText())
                .isEqualTo("#/components/schemas/" + parametersSchema);
    }

    private void assertJobExample(JsonNode root, String exampleName, String schemaName, String command) {
        JsonNode value = example(root, exampleName);
        JsonNode base = schema(root, "JobEnvelopeBase");
        JsonNode variant = schema(root, schemaName).path("allOf").path(1);
        assertThat(fieldNames(value)).isSubsetOf(union(fieldNames(base.path("properties")), fieldNames(variant.path("properties"))));
        assertThat(fieldNames(value)).containsAll(required(base)).containsAll(required(variant));
        assertThat(value.path("command_type").asText()).isEqualTo(command);
        JsonNode parameterSchema = resolveRef(root, variant.path("properties").path("parameters").path("$ref").asText());
        assertTopLevelSchema(value.path("parameters"), parameterSchema);
        assertThat(Instant.parse(value.path("created_at").asText()))
                .isBefore(Instant.parse(value.path("expires_at").asText()));
    }

    private void assertEnumValue(JsonNode root, String schemaName, String value) {
        assertThat(textValues(schema(root, schemaName).path("enum"))).contains(value);
    }

    private JsonNode resolveRef(JsonNode root, String ref) {
        assertThat(ref).startsWith("#/components/schemas/");
        return schema(root, ref.substring("#/components/schemas/".length()));
    }

    private void assertTopLevelSchema(JsonNode value, JsonNode valueSchema) {
        assertThat(value.isObject()).isTrue();
        assertThat(fieldNames(value)).containsAll(required(valueSchema));
        assertThat(fieldNames(value)).isSubsetOf(fieldNames(valueSchema.path("properties")));
    }

    private Set<String> required(JsonNode valueSchema) {
        return textValues(valueSchema.path("required"));
    }

    private Set<String> fieldNames(JsonNode object) {
        Set<String> names = new LinkedHashSet<>();
        object.fieldNames().forEachRemaining(names::add);
        return names;
    }

    private Set<String> textValues(JsonNode array) {
        Set<String> values = new LinkedHashSet<>();
        array.forEach(value -> values.add(value.asText()));
        return values;
    }

    private Set<String> union(Set<String> left, Set<String> right) {
        Set<String> values = new LinkedHashSet<>(left);
        values.addAll(right);
        return values;
    }
}

package cn.kjky.collector.contract;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/** cc-ingest v1 契约使用的 JSON 编码和内容哈希工具。 */
public final class IngestJson {
    public static final String SCHEMA_VERSION = "1.0";

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private IngestJson() {}

    /** 将批次编码成无缩进、字段顺序稳定的 UTF-8 JSON。 */
    public static byte[] writeBatch(BatchEnvelope batch) throws IOException {
        return canonicalBytes(batch);
    }

    /**
     * 使用与 cc-ingest 请求一致的 gzip 编码压缩字节。
     * 组批和 HTTP 客户端必须共用该方法，避免本地大小判断与实际请求不一致。
     */
    public static byte[] gzip(byte[] input) throws IOException {
        if (input == null) throw new IllegalArgumentException("待压缩字节不能为空");
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(output)) {
            gzip.write(input);
        }
        return output.toByteArray();
    }

    /** 解析 cc-ingest 返回的 v1 回执；未知字段由模型忽略。 */
    public static IngestResponse readResponse(byte[] body) throws IOException {
        return MAPPER.readValue(body, IngestResponse.class);
    }

    /** 编码标准回执，供中控模拟器和契约测试使用。 */
    public static byte[] writeResponse(IngestResponse response) throws IOException {
        return canonicalBytes(response);
    }

    /** 供接口测试和后续 Outbox 读取同一份契约模型。 */
    public static BatchEnvelope readBatch(byte[] body) throws IOException {
        return MAPPER.readValue(body, BatchEnvelope.class);
    }

    /** 计算事件数组的稳定 SHA-256，数组顺序保留、对象键按字典序排列。 */
    public static String eventsSha256(List<StandardEvent> events) {
        return sha256Canonical(events);
    }

    /** 计算任意 JSON 兼容值的稳定 SHA-256。 */
    public static String sha256Canonical(Object value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonicalBytes(value)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("当前 JRE 不支持 SHA-256", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("无法序列化待哈希内容", e);
        }
    }

    private static byte[] canonicalBytes(Object value) throws IOException {
        JsonNode tree = MAPPER.valueToTree(value);
        return MAPPER.writeValueAsBytes(canonicalize(tree));
    }

    private static JsonNode canonicalize(JsonNode node) {
        if (node == null || node.isNull() || node.isValueNode()) return node;
        if (node.isArray()) {
            ArrayNode result = MAPPER.createArrayNode();
            node.forEach(child -> result.add(canonicalize(child)));
            return result;
        }
        if (node.isObject()) {
            ObjectNode result = MAPPER.createObjectNode();
            List<String> names = new ArrayList<>();
            node.fieldNames().forEachRemaining(names::add);
            names.sort(Comparator.naturalOrder());
            names.forEach(name -> result.set(name, canonicalize(node.get(name))));
            return result;
        }
        return node.deepCopy();
    }
}

package cn.kjky.collector.pagination;

import cn.kjky.collector.config.CollectorConfig.PageMapping;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.node.MissingNode;
import java.util.List;

/** Strict response adapter. Explicit configured pointers are required to exist. */
public final class JsonPage {
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    private JsonPage() {}
    public static JsonNode parse(String body) {
        try {
            JsonNode result = JSON.readTree(body);
            if (result == null) throw PageGuard.failure("INVALID_JSON");
            return result;
        } catch (Exception e) { throw PageGuard.failure("INVALID_JSON"); }
    }
    public static JsonNode records(JsonNode root, boolean trace) {
        if (root.isArray()) return root;
        List<String> paths = trace ? List.of("/PageBean/TraceInfos", "/Data/TraceInfos", "/TraceInfos", "/data/traceInfos", "/records")
                : List.of("/Data", "/data", "/Records", "/records", "/Result", "/result", "/Rows", "/rows", "/PageBean/Records");
        for (String path : paths) if (root.at(path).isArray()) return root.at(path);
        throw PageGuard.failure("INVALID_RECORDS");
    }
    public static PageGuard.Metadata metadata(JsonNode root, PageMapping mapping) {
        if (mapping == null) throw PageGuard.failure("INVALID_MAPPING");
        return new PageGuard.Metadata(
                number(field(root, mapping.totalPointer, "TotalCount", "totalCount", "Total", "total")),
                number(field(root, mapping.positionPointer, "PageNumber", "pageNumber", "Page", "page")),
                bool(field(root, mapping.hasMorePointer, "HasMore", "hasMore", "has_more")),
                bool(field(root, mapping.truncatedPointer, "Truncated", "truncated", "IsTruncated", "isTruncated")),
                cursor(field(root, mapping.nextCursorPointer, "NextToken", "nextToken", "NextCursor", "nextCursor")));
    }
    /** A server-side page-size clamp must not masquerade as a short terminal page. */
    public static void verifyPageSize(JsonNode root, int requested) {
        Long returned = number(field(root, null, "PageSize", "pageSize", "page_size"));
        if (returned != null && returned != requested) throw PageGuard.failure("PAGE_SIZE_MISMATCH");
    }
    private static JsonNode field(JsonNode root, String pointer, String... aliases) {
        JsonNode found = MissingNode.getInstance();
        if (pointer != null) {
            found = root.at(pointer);
            if (found.isMissingNode()) throw PageGuard.failure("MISSING_METADATA");
        }
        // Conservative recognition only in envelopes, never in individual business records.
        for (String prefix : List.of("", "/PageBean", "/Data", "/data", "/pagination")) {
            if (!root.at(prefix).isObject()) continue;
            for (String name : aliases) {
                JsonNode candidate = root.at(prefix + "/" + name);
                if (candidate.isMissingNode()) continue;
                if (!found.isMissingNode() && !found.equals(candidate)) throw PageGuard.failure("CONFLICTING_METADATA");
                found = candidate;
            }
        }
        return found;
    }
    private static Long number(JsonNode n) {
        if (n.isMissingNode()) return null;
        if (!n.isIntegralNumber() || !n.canConvertToLong() || n.longValue() < 0) throw PageGuard.failure("INVALID_METADATA");
        return n.longValue();
    }
    private static Boolean bool(JsonNode n) {
        if (n.isMissingNode()) return null;
        if (!n.isBoolean()) throw PageGuard.failure("INVALID_METADATA");
        return n.booleanValue();
    }
    private static String cursor(JsonNode n) {
        if (n.isMissingNode()) return null;
        if (n.isNull()) return "";
        if (!n.isTextual() || n.textValue().length() > 8192) throw PageGuard.failure("INVALID_METADATA");
        return n.textValue();
    }
}

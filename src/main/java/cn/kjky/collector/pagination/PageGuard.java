package cn.kjky.collector.pagination;

import cn.kjky.collector.config.CollectorConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/** Per-window bounded progress/completeness guard. No response values enter exceptions. */
public final class PageGuard {
    private static final ObjectMapper JSON = new ObjectMapper();
    public record Metadata(Long total, Long position, Boolean hasMore, Boolean truncated, String nextCursor) {
        public static Metadata empty() { return new Metadata(null, null, null, null, null); }
    }
    private final int maxPages;
    private final long maxRecords;
    private final Set<String> pages = new HashSet<>();
    private final Set<String> cursors = new HashSet<>();
    private Long total;
    private long lastPosition = -1;
    private int pageCount;
    private long recordCount;
    private boolean ended;

    public PageGuard(CollectorConfig.PaginationLimits limits) {
        if (limits == null || limits.maxPages < 1 || limits.maxRecords < 1) throw failure("INVALID_LIMITS");
        maxPages = limits.maxPages;
        maxRecords = limits.maxRecords;
    }

    /** pageSize==0 disables the short-page fallback (unknown Dataset contract). */
    public boolean accept(JsonNode records, long requestedPosition, int pageSize, Metadata m) {
        if (ended || ++pageCount > maxPages) throw failure("MAX_PAGES");
        if (records == null || !records.isArray()) throw failure("INVALID_RECORDS");
        int count = records.size();
        if (pageSize > 0 && count > pageSize) throw failure("OVERSIZED_PAGE");
        if (requestedPosition <= lastPosition || (m.position != null && m.position != requestedPosition))
            throw failure("POSITION_NOT_ADVANCING");
        lastPosition = requestedPosition;
        // Reordered identical pages are still non-progress. Keep only bounded digests, never raw records.
        List<String> recordHashes = new ArrayList<>();
        records.forEach(record -> recordHashes.add(hash(canonical(record).toString())));
        Collections.sort(recordHashes);
        if (count > 0 && !pages.add(hash(String.join("", recordHashes)))) throw failure("REPEATED_PAGE");
        recordCount += count;
        if (recordCount > maxRecords) throw failure("MAX_RECORDS");
        if (m.total != null) {
            if (m.total < 0 || (total != null && !total.equals(m.total))) throw failure("TOTAL_CHANGED");
            total = m.total;
        }
        if (total != null && recordCount > total) throw failure("TOTAL_MISMATCH");
        Boolean more = m.hasMore;
        if (m.truncated != null) more = agree(more, m.truncated);
        if (m.nextCursor != null) {
            boolean cursorMore = !m.nextCursor.isEmpty();
            more = agree(more, cursorMore);
            if (cursorMore && !cursors.add(hash(m.nextCursor))) throw failure("CURSOR_NOT_ADVANCING");
        }
        if (total != null) more = agree(more, recordCount < total);
        if (more == null) {
            if (pageSize == 0) throw failure("COMPLETENESS_UNKNOWN");
            more = count == pageSize;
        }
        if (more && count == 0) throw failure("EMPTY_PAGE_WITH_MORE");
        if (more && pageCount >= maxPages) throw failure("MAX_PAGES");
        if (more && recordCount >= maxRecords) throw failure("MAX_RECORDS");
        ended = !more;
        return ended;
    }

    private Boolean agree(Boolean current, boolean value) {
        if (current != null && current != value) throw failure("CONFLICTING_METADATA");
        return value;
    }
    private static JsonNode canonical(JsonNode node) {
        if (node.isObject()) {
            var result = JSON.createObjectNode();
            TreeSet<String> keys = new TreeSet<>();
            node.fieldNames().forEachRemaining(keys::add);
            keys.forEach(key -> result.set(key, canonical(node.get(key))));
            return result;
        }
        if (node.isArray()) {
            var result = JSON.createArrayNode();
            node.forEach(child -> result.add(canonical(child)));
            return result;
        }
        return node;
    }
    public static String hash(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) { throw failure("HASH_FAILED"); }
    }
    public static IllegalStateException failure(String code) {
        return new IllegalStateException("PAGINATION_" + code);
    }
}

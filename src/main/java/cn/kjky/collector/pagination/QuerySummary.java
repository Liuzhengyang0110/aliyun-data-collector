package cn.kjky.collector.pagination;

import cn.kjky.collector.core.TimeWindow;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Exactly one bounded, body-free summary per executed task window, including failures. */
public final class QuerySummary implements AutoCloseable {
    private final Map<String, Object> fields = new LinkedHashMap<>();
    private final Set<String> requestIds = new LinkedHashSet<>();
    private int pages;
    private long records;
    private boolean complete;
    public QuerySummary(String sourceKey, TimeWindow window) {
        fields.put("source_key", sourceKey);
        fields.put("window_start", window.start().toString());
        fields.put("window_end", window.end().toString());
    }
    public void page(int count, String requestId) {
        pages++;
        records += count;
        request(requestId);
    }
    public void request(String requestId) {
        if (requestId != null && requestIds.size() < 16) requestIds.add(PageGuard.hash(requestId));
    }
    public void complete() { complete = true; }
    @Override public void close() {
        fields.put("page_count", pages);
        fields.put("record_count", records);
        fields.put("request_id_sha256_sample", requestIds);
        fields.put("complete", complete);
        try { System.out.println("QUERY_SUMMARY " + new ObjectMapper().writeValueAsString(fields)); }
        catch (Exception ignored) { System.out.println("QUERY_SUMMARY SERIALIZATION_FAILED"); }
    }
}

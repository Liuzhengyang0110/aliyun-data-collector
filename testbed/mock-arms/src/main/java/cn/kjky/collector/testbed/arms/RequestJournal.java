package cn.kjky.collector.testbed.arms;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Bounded request journal that never stores signing/authentication material. */
final class RequestJournal {
    private static final int MAX_ENTRIES = 1000;
    private static final List<String> SECRET_NAMES = List.of("signature", "securitytoken", "accesskeysecret", "authorization");
    private final List<Entry> entries = new ArrayList<>();

    synchronized void add(String method, String path, String action, Map<String, String> parameters) {
        Map<String, String> safe = new LinkedHashMap<>();
        parameters.forEach((key, value) -> {
            String lower = key.toLowerCase(java.util.Locale.ROOT);
            if (SECRET_NAMES.stream().noneMatch(lower::contains) && !"accesskeyid".equals(lower)) safe.put(key, value);
        });
        if (entries.size() == MAX_ENTRIES) entries.remove(0);
        entries.add(new Entry(Instant.now().toString(), method, path, action, Map.copyOf(safe)));
    }

    synchronized List<Entry> snapshot() { return List.copyOf(entries); }
    synchronized void clear() { entries.clear(); }

    record Entry(String received_at, String method, String path, String action, Map<String, String> parameters) {}
}

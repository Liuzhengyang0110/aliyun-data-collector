package cn.kjky.collector.normalize;

import cn.kjky.collector.contract.StandardEvent;

import java.util.List;

/** 一次响应或一条日志的标准事件和非致命告警。 */
public record NormalizationResult(
        List<StandardEvent> events,
        List<NormalizationWarning> warnings) {

    public NormalizationResult {
        events = events == null ? List.of() : List.copyOf(events);
        warnings = warnings == null ? List.of() : List.copyOf(warnings);
    }
}

package cn.kjky.collector.normalize;

import java.util.List;

/** 不阻断转换的结构化告警；只保存字段路径，不保存原始字段值。 */
public record NormalizationWarning(
        String code,
        String message,
        String sourceRef,
        List<String> fieldPaths) {

    public NormalizationWarning {
        if (code == null || code.isBlank()) throw new IllegalArgumentException("告警 code 不能为空");
        if (message == null || message.isBlank()) throw new IllegalArgumentException("告警 message 不能为空");
        fieldPaths = fieldPaths == null ? List.of() : List.copyOf(fieldPaths);
    }
}

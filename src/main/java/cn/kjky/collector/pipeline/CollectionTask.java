package cn.kjky.collector.pipeline;

import java.time.Instant;
import java.util.Objects;

/** 单个数据源采集任务在一个查询窗口中的稳定上下文。 */
public record CollectionTask(
        String taskKey,
        String sourceType,
        String recordType,
        Instant windowStart,
        Instant windowEnd) {

    public CollectionTask {
        require(taskKey, "taskKey");
        require(sourceType, "sourceType");
        require(recordType, "recordType");
        Objects.requireNonNull(windowStart, "windowStart 不能为空");
        Objects.requireNonNull(windowEnd, "windowEnd 不能为空");
        if (!windowStart.isBefore(windowEnd)) throw new IllegalArgumentException("windowStart 必须早于 windowEnd");
    }

    /** ARMS Trace 搜索及 Trace 详情共用同一个任务水位。 */
    public static String armsTraceKey(String taskName) {
        return "ARMS/TRACE/" + segment(taskName, "ARMS Trace 任务名");
    }

    /** ARMS 指标任务使用独立水位。 */
    public static String armsMetricKey(String taskName) {
        return "ARMS/METRIC/" + segment(taskName, "ARMS Metric 任务名");
    }

    /** ARMS Dataset 任务使用独立水位。 */
    public static String armsDatasetKey(String taskName) {
        return "ARMS/DATASET/" + segment(taskName, "ARMS Dataset 任务名");
    }

    /** 每一个 SLS Project/Logstore 组合使用独立水位。 */
    public static String slsLogKey(String project, String logstore) {
        return "SLS/LOG/" + segment(project, "SLS Project") + "/" + segment(logstore, "SLS Logstore");
    }

    private static String segment(String value, String name) {
        require(value, name);
        return value.trim();
    }

    private static void require(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " 不能为空");
    }
}

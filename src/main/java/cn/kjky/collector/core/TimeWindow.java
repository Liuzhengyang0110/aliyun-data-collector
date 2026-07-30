package cn.kjky.collector.core;

import java.time.Instant;

/**
 * 一个逻辑上的左闭右开采集窗口 {@code [start, end)}。
 * 使用普通不可变类而不是 record，以兼容 Java 8。
 */
public final class TimeWindow {
    /** 窗口开始时刻（包含）。 */
    private final Instant start;
    /** 窗口结束时刻（不包含）。 */
    private final Instant end;

    /**
     * 创建并校验时间窗口。
     *
     * @param start 窗口开始时刻
     * @param end 窗口结束时刻
     */
    public TimeWindow(Instant start, Instant end) {
        if (start == null || end == null || !start.isBefore(end)) throw new IllegalArgumentException("时间窗必须满足 start < end");
        this.start = start;
        this.end = end;
    }

    /** @return 窗口开始时刻 */
    public Instant start() { return start; }

    /** @return 窗口结束时刻 */
    public Instant end() { return end; }
}

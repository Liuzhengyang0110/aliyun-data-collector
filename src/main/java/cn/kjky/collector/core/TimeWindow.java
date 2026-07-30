package cn.kjky.collector.core;

import java.time.Instant;

/**
 * 一个逻辑上的左闭右开采集窗口 {@code [start, end)}。
 *
 * @param start 窗口开始时刻（包含）
 * @param end 窗口结束时刻（不包含）
 */
public record TimeWindow(Instant start, Instant end) {
    /** 校验窗口两端都存在，并且开始严格早于结束。 */
    public TimeWindow {
        if (start == null || end == null || !start.isBefore(end)) throw new IllegalArgumentException("时间窗必须满足 start < end");
    }
}

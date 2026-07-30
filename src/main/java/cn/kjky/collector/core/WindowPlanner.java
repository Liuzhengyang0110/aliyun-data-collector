package cn.kjky.collector.core;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** 把较大的采集时间范围拆成连续、互不重叠的小窗口。 */
public final class WindowPlanner {
    /**
     * 将 {@code [start, end)} 拆分成最长为 seconds 秒的时间窗口。
     *
     * @param start 整体采集开始时刻（包含）
     * @param end 整体采集结束时刻（不包含）
     * @param seconds 每个窗口的最大秒数
     * @return 按时间升序排列的窗口列表
     * @throws IllegalArgumentException seconds 不大于 0，或时间范围不合法
     */
    public List<TimeWindow> split(Instant start, Instant end, int seconds) {
        if (seconds <= 0) throw new IllegalArgumentException("窗口秒数必须大于 0");
        // result 保存最终所有窗口；cursor 表示下一个窗口的起点。
        List<TimeWindow> result = new ArrayList<>();
        Instant cursor = start;
        // size 是配置秒数对应的 Duration，避免循环中重复创建。
        Duration size = Duration.ofSeconds(seconds);
        while (cursor.isBefore(end)) {
            // next 先按标准窗口推进，最后一个窗口不能超过整体结束时间。
            Instant next = cursor.plus(size);
            if (next.isAfter(end)) next = end;
            result.add(new TimeWindow(cursor, next));
            cursor = next;
        }
        return result;
    }
}

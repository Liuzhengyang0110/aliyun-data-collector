package cn.kjky.collector.output;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.core.TimeWindow;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** 按现有统一命名规范生成 raw 数据文件名。 */
public final class FileNaming {
    /** 将 Instant 格式化为固定 UTC 文件名时间，例如 20260727T010000Z。 */
    private static final DateTimeFormatter UTC = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

    /**
     * 生成一个完整文件名，不包含目录。
     *
     * @param c 提供 projectCode、envCode、batchId 的根配置
     * @param collectorId 采集器标识，当前为 arms 或 sls
     * @param recordType 现有命名规范中的记录类型
     * @param w 文件覆盖的逻辑时间窗口
     * @param part 同一采集器、记录类型和窗口内的分片号
     * @param extension 扩展名，不包含点
     * @return 符合统一规范的文件名
     */
    public String name(CollectorConfig c, String collectorId, String recordType, TimeWindow w, int part, String extension) {
        return String.join("_", token(c.projectCode), token(c.envCode), token(c.batchId), token(collectorId),
                token(recordType), UTC.format(w.start()), UTC.format(w.end()), "p" + String.format("%04d", part))
                + "." + token(extension);
    }

    /**
     * 校验一个文件名字段，防止路径分隔符和非法字符进入文件名。
     *
     * @param value 待校验字段
     * @return 原值，便于在 String.join 中直接使用
     * @throws IllegalArgumentException 字段为空或含非法字符
     */
    private String token(String value) {
        if (value == null || !value.matches("[A-Za-z0-9][A-Za-z0-9._-]*")) throw new IllegalArgumentException("非法文件名字段: " + value);
        return value;
    }
}

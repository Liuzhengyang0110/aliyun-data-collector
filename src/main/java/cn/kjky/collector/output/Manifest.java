package cn.kjky.collector.output;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 一个批次的正式输出清单。
 * JSON 字段保持 public，便于 Jackson 直接序列化和恢复。
 */
public class Manifest {
    /** 清单结构版本。 */
    public String schemaVersion;
    /** 项目编号。 */
    public String projectCode;
    /** 环境编号。 */
    public String envCode;
    /** 批次编号。 */
    public String batchId;
    /** 测试案例编号。 */
    public String caseId;
    /** 配置时区。 */
    public String timezone;
    /** 本清单覆盖的最早采集时刻。 */
    public Instant collectionStart;
    /** 本清单覆盖的最晚采集时刻。 */
    public Instant collectionEnd;
    /** 清单最近更新时间。 */
    public Instant generatedAt;
    /** 生成清单的采集程序版本。 */
    public String collectorVersion;
    /** 已成功落盘的文件记录。 */
    public List<FileEntry> files = new ArrayList<>();
    /** 采集过程中记录的错误。 */
    public List<ErrorEntry> errors = new ArrayList<>();

    /** 一个 raw 文件的完整性和来源信息。 */
    public static class FileEntry {
        /** 相对于 outputRoot 的路径，统一使用正斜杠。 */
        public String relativePath;
        /** 采集器标识：arms 或 sls。 */
        public String collectorId;
        /** 命名规范中的记录类型。 */
        public String recordType;
        /** 文件逻辑窗口开始。 */
        public Instant windowStart;
        /** 文件逻辑窗口结束。 */
        public Instant windowEnd;
        /** 同窗口分片号。 */
        public int part;
        /** 文件字节数。 */
        public long bytes;
        /** 可识别的记录数，无法估算时为 -1。 */
        public long records;
        /** 文件 SHA-256。 */
        public String sha256;
        /** 对应 API Request ID。 */
        public String requestId;
    }

    /** 一次采集失败摘要。 */
    public static class ErrorEntry {
        /** 记录错误的时刻。 */
        public Instant time;
        /** 失败操作或窗口。 */
        public String operation;
        /** 已脱敏的异常摘要。 */
        public String message;
    }
}

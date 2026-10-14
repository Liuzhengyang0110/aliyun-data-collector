package cn.kjky.collector.outbox;

import java.io.IOException;

/**
 * Outbox 持久化路径使用的容量计数与原子预留接口。
 * 新采集批次受硬配额约束；上传所需的状态元数据和 413 子批不能被采集门禁阻断。
 */
public interface OutboxSpaceGuard {
    Reservation reserveNewBatch(long peakBytes) throws IOException;

    Reservation reserveMaintenance(long peakBytes) throws IOException;

    void adjustUsage(long deltaBytes) throws IOException;

    void reconcileNow() throws IOException;

    boolean collectionAllowed();

    interface Reservation extends AutoCloseable {
        /** 完成写入，并以最终持久化字节差量替换临时空间预留。 */
        void commit(long permanentDeltaBytes) throws IOException;

        @Override
        void close();
    }
}

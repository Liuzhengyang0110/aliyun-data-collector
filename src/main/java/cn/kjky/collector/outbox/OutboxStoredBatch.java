package cn.kjky.collector.outbox;

import cn.kjky.collector.contract.BatchEnvelope;

import java.nio.file.Path;

/** 从 PVC 验证并加载出的完整 Outbox 批次。 */
public record OutboxStoredBatch(
        OutboxBatch descriptor,
        BatchEnvelope batch,
        OutboxDelivery delivery,
        Path directory,
        byte[] serializedBatch) {

    public OutboxStoredBatch {
        serializedBatch = serializedBatch.clone();
    }

    @Override
    public byte[] serializedBatch() {
        return serializedBatch.clone();
    }
}

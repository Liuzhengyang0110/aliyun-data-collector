package cn.kjky.collector.outbox;

import java.util.concurrent.atomic.AtomicBoolean;

/** 单进程上传工作线程对一个 Outbox 批次的互斥认领。 */
public final class OutboxClaim implements AutoCloseable {
    private final OutboxStore owner;
    private final OutboxStoredBatch stored;
    private final AtomicBoolean active = new AtomicBoolean(true);

    OutboxClaim(OutboxStore owner, OutboxStoredBatch stored) {
        this.owner = owner;
        this.stored = stored;
    }

    public OutboxStoredBatch stored() {
        return stored;
    }

    public boolean active() {
        return active.get();
    }

    void requireOwner(OutboxStore expected) {
        if (owner != expected) throw new IllegalArgumentException("OutboxClaim 不属于当前 OutboxStore");
        if (!active.get()) throw new IllegalStateException("OutboxClaim 已经完成或释放");
    }

    boolean finish() {
        return active.compareAndSet(true, false);
    }

    boolean ownedBy(OutboxStore expected) {
        return owner == expected;
    }

    @Override
    public void close() {
        owner.release(this);
    }
}

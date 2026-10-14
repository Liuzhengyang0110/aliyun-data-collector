package cn.kjky.collector.control;

import cn.kjky.collector.runtime.CollectionSource;

import java.util.Objects;
import java.util.function.BooleanSupplier;

/** 组合人工暂停、Outbox 逻辑配额、PVC 物理容量与本地配置有效性。 */
public final class CollectionGate {
    private final ManualPauseStateStore pauseStore;
    private final BooleanSupplier logicalQuotaAllowed;
    private final BooleanSupplier physicalCapacityAllowed;
    private final BooleanSupplier localConfigValid;

    public CollectionGate(ManualPauseStateStore pauseStore,
                          BooleanSupplier logicalQuotaAllowed,
                          BooleanSupplier physicalCapacityAllowed,
                          BooleanSupplier localConfigValid) {
        this.pauseStore = Objects.requireNonNull(pauseStore, "pauseStore 不能为空");
        this.logicalQuotaAllowed = Objects.requireNonNull(logicalQuotaAllowed, "logicalQuotaAllowed 不能为空");
        this.physicalCapacityAllowed = Objects.requireNonNull(physicalCapacityAllowed, "physicalCapacityAllowed 不能为空");
        this.localConfigValid = Objects.requireNonNull(localConfigValid, "localConfigValid 不能为空");
    }

    public boolean collectionAllowed(CollectionSource source) {
        return !pauseStore.paused(Objects.requireNonNull(source, "source 不能为空"))
                && logicalQuotaAllowed.getAsBoolean()
                && physicalCapacityAllowed.getAsBoolean()
                && localConfigValid.getAsBoolean();
    }
}

package cn.kjky.collector.runtime;

/** 生产 Outbox/PVC 容量保护状态。 */
public enum CapacityState {
    /** 容量低于告警水位，正常采集和上传。 */
    NORMAL,
    /** 容量达到告警水位，记录告警但继续采集和上传。 */
    WARNING,
    /** 容量达到临界水位，暂停新采集窗口但保持上传。 */
    CRITICAL,
    /** 从临界状态下降到恢复水位后恢复采集，并观察是否稳定。 */
    RECOVERING
}

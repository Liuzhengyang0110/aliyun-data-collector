package cn.kjky.collector.runtime;

/** 将本地策略拒绝原因稳定映射到已冻结的 cc-control v1 枚举。 */
public final class QuotaPolicyRejectedException extends Exception {
    public enum Reason {
        QUOTA_BELOW_LOCAL_MIN,
        QUOTA_ABOVE_LOCAL_MAX,
        PVC_SAFETY_LIMIT,
        STALE_CONFIG_VERSION,
        INVALID_PARAMETERS
    }

    private final Reason reason;

    public QuotaPolicyRejectedException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}

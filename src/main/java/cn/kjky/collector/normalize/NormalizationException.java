package cn.kjky.collector.normalize;

/** 原始响应结构无法安全解释时抛出，调用方不得把该页当作成功或推进水位。 */
public final class NormalizationException extends RuntimeException {
    private final String code;

    public NormalizationException(String code, String message) {
        super(message);
        this.code = code;
    }

    public NormalizationException(String code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public String code() {
        return code;
    }
}

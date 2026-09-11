package cn.kjky.collector.security;

/** mTLS Secret Volume 内容缺失、无效或不安全时的失败关闭异常。 */
public final class MtlsClientException extends IllegalStateException {
    public MtlsClientException(String message) {
        super(message);
    }

    public MtlsClientException(String message, Throwable cause) {
        super(message, cause);
    }
}

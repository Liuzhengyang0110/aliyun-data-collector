package cn.kjky.collector.core;

import com.aliyun.openservices.log.exception.LogException;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.exceptions.ErrorType;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.http.HttpTimeoutException;
import java.util.Locale;

/** 对 ARMS RPC、ARMS Dataset 和 SLS 查询异常分类；独立于上传侧策略。 */
public final class CloudQueryErrorClassifier {
    /** 只包含可安全写入日志的信息。 */
    public record Classification(QueryErrorCategory category, String safeReason, String retryAfter) {
        public boolean retryable() {
            return category == QueryErrorCategory.RETRYABLE || category == QueryErrorCategory.RATE_LIMITED;
        }
    }

    public Classification classify(Throwable failure) {
        Throwable root = unwrap(failure);
        if (root instanceof CloudQueryException e) {
            return new Classification(e.category(), e.safeCode(), e.retryAfter());
        }
        if (root instanceof LogException e) return classifySls(e);
        if (root instanceof ClientException e) return classifyRpc(e);
        if (root instanceof JsonProcessingException) return nonRetryable("INVALID_RESPONSE_SCHEMA");
        if (root instanceof IllegalArgumentException) return nonRetryable("INVALID_ARGUMENT");
        if (root instanceof SecurityException) return authentication("SECURITY_POLICY");
        if (root instanceof HttpTimeoutException || root instanceof SocketTimeoutException
                || root instanceof ConnectException || root instanceof NoRouteToHostException
                || root instanceof SocketException || root instanceof InterruptedIOException) {
            return retryable("NETWORK_OR_TIMEOUT");
        }
        if (root instanceof java.io.IOException) return retryable("NETWORK_IO");
        return nonRetryable("NON_RETRYABLE_EXCEPTION");
    }

    private Classification classifySls(LogException e) {
        int status = e.GetHttpCode();
        String code = normalized(e.GetErrorCode());
        if (status == 401 || status == 403 || authenticationCode(code)) return authentication(safeCode(code, status));
        if (status == 429 || rateLimitCode(code)) return rateLimited(safeCode(code, status));
        if (status == 408 || retryableStatus(status) || retryableCode(code) || status == 0) {
            return retryable(safeCode(code, status));
        }
        return nonRetryable(safeCode(code, status));
    }

    private Classification classifyRpc(ClientException e) {
        String code = normalized(e.getErrCode());
        if (authenticationCode(code)) return authentication(safeCode(code, 0));
        if (rateLimitCode(code) || e.getErrorType() == ErrorType.Throttling) return rateLimited(safeCode(code, 429));
        if (retryableCode(code) || e.getErrorType() == ErrorType.Server) return retryable(safeCode(code, 0));
        if (e.getCause() != null) {
            Classification caused = classify(e.getCause());
            if (caused.retryable()) return caused;
        }
        return nonRetryable(safeCode(code, 0));
    }

    private boolean retryableStatus(int status) {
        return status == 500 || status == 502 || status == 503 || status == 504;
    }

    private boolean authenticationCode(String code) {
        return containsAny(code, "unauthor", "forbidden", "accessdenied", "permission", "signature",
                "invalidaccesskey", "invalidsecuritytoken", "securitytoken", "authentication", "authfailed");
    }

    private boolean rateLimitCode(String code) {
        return containsAny(code, "throttl", "ratelimit", "toomanyrequest", "flowcontrol", "quotaexceed");
    }

    private boolean retryableCode(String code) {
        return containsAny(code, "timeout", "timedout", "network", "connection", "socket", "reset",
                "serviceunavailable", "temporarilyunavailable", "internalerror", "servererror",
                "serverunreachable", "servicebusy");
    }

    private boolean containsAny(String value, String... fragments) {
        for (String fragment : fragments) if (value.contains(fragment)) return true;
        return false;
    }

    private String normalized(String value) {
        return value == null ? "" : value.replaceAll("[^A-Za-z0-9_.-]", "").toLowerCase(Locale.ROOT);
    }

    private String safeCode(String code, int status) {
        if (!code.isBlank()) return code.length() <= 80 ? code : code.substring(0, 80);
        return status > 0 ? "HTTP_" + status : "SDK_ERROR";
    }

    private Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        for (int i = 0; i < 8 && current.getCause() != null && current.getCause() != current; i++) {
            if (current instanceof CloudQueryException || current instanceof LogException
                    || current instanceof ClientException || current instanceof JsonProcessingException
                    || current instanceof java.io.IOException || current instanceof IllegalArgumentException
                    || current instanceof SecurityException) break;
            current = current.getCause();
        }
        return current;
    }

    private Classification retryable(String reason) {
        return new Classification(QueryErrorCategory.RETRYABLE, reason, null);
    }

    private Classification nonRetryable(String reason) {
        return new Classification(QueryErrorCategory.NON_RETRYABLE, reason, null);
    }

    private Classification authentication(String reason) {
        return new Classification(QueryErrorCategory.AUTHENTICATION, reason, null);
    }

    private Classification rateLimited(String reason) {
        return new Classification(QueryErrorCategory.RATE_LIMITED, reason, null);
    }
}

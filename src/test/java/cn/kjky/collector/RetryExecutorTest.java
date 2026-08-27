package cn.kjky.collector;

import cn.kjky.collector.core.RetryExecutor;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.exceptions.ServerException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryExecutorTest {
    @Test
    void retriesAliyunServerExceptionEvenWhenCodeHasNoServerKeyword() throws Exception {
        AtomicInteger attempts = new AtomicInteger();

        String result = new RetryExecutor(2, 0).execute("mock ARMS", () -> {
            if (attempts.incrementAndGet() == 1) {
                throw new ServerException("InternalError", "temporary server error");
            }
            return "ok";
        });

        assertThat(result).isEqualTo("ok");
        assertThat(attempts).hasValue(2);
    }

    @Test
    void doesNotRetryPermanentClientError() {
        AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(() -> new RetryExecutor(2, 0).execute("mock ARMS", () -> {
            attempts.incrementAndGet();
            throw new ClientException("InvalidAccessKeyId.NotFound", "invalid access key");
        })).isInstanceOf(ClientException.class);

        assertThat(attempts).hasValue(1);
    }
}

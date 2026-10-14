package cn.kjky.collector.status;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.runtime.DaemonLoop;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 独立状态循环：周期心跳、变化合并上报和失败有界退避均只保留最新快照。
 * 它不写业务 Outbox，也不调用或等待采集、上传和控制循环。
 */
public final class StatusReporter implements DaemonLoop {
    private static final long CHANGE_DETECTION_MILLIS = 200;

    @FunctionalInterface
    public interface StatusTransport {
        void report(CollectorStatusSnapshot snapshot) throws IOException, InterruptedException;
    }

    private final RuntimeStatusRegistry registry;
    private final StatusTransport transport;
    private final AutoCloseable closeableTransport;
    private final Clock clock;
    private final Random random;
    private final long heartbeatMillis;
    private final long minChangeIntervalMillis;
    private final long retryInitialMillis;
    private final long retryMaxMillis;
    private final double retryJitterRatio;
    private final int shutdownGraceSeconds;
    private final Consumer<String> info;
    private final Consumer<String> error;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final AtomicLong externalChanges = new AtomicLong();
    private final Object wakeup = new Object();
    private final CountDownLatch terminated = new CountDownLatch(1);
    private final Thread thread;

    public StatusReporter(CollectorConfig config, RuntimeStatusRegistry registry, StatusClient client) {
        this(config, registry, client, client, Clock.systemUTC(), new Random(),
                System.out::println, System.err::println);
    }

    StatusReporter(CollectorConfig config, RuntimeStatusRegistry registry, StatusTransport transport,
                   AutoCloseable closeableTransport, Clock clock, Random random,
                   Consumer<String> info, Consumer<String> error) {
        this.registry = Objects.requireNonNull(registry, "registry 不能为空");
        this.transport = Objects.requireNonNull(transport, "transport 不能为空");
        this.closeableTransport = closeableTransport;
        this.clock = Objects.requireNonNull(clock, "clock 不能为空");
        this.random = Objects.requireNonNull(random, "random 不能为空");
        CollectorConfig.ControlConfig control = Objects.requireNonNull(config.control, "control 配置不能为空");
        this.heartbeatMillis = control.heartbeatIntervalSeconds * 1000L;
        this.minChangeIntervalMillis = control.statusChangeMinIntervalMillis;
        this.retryInitialMillis = control.retryInitialDelayMillis;
        this.retryMaxMillis = control.retryMaxDelayMillis;
        this.retryJitterRatio = control.retryJitterRatio;
        this.shutdownGraceSeconds = config.scheduler.shutdownGraceSeconds;
        this.info = Objects.requireNonNull(info, "info 不能为空");
        this.error = Objects.requireNonNull(error, "error 不能为空");
        this.thread = new Thread(this::run, "collector-status-reporter");
        this.thread.setDaemon(false);
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) return;
        thread.start();
        info.accept("采集端节点状态主动上报已启动，心跳间隔=" + heartbeatMillis / 1000 + "秒");
    }

    /** 供组件或测试显式提示变化；循环仍会用轻量令牌兜底检测。 */
    public void signalChange() {
        externalChanges.incrementAndGet();
        synchronized (wakeup) { wakeup.notifyAll(); }
    }

    private void run() {
        long nextHeartbeat = 0;
        long nextRetry = Long.MAX_VALUE;
        long lastAttempt = Long.MIN_VALUE / 2;
        int failures = 0;
        long observedExternal = externalChanges.get();
        RuntimeStatusRegistry.ChangeToken observedToken = registry.changeToken();
        boolean dirty = true;
        try {
            while (accepting.get()) {
                long now = System.nanoTime() / 1_000_000L;
                RuntimeStatusRegistry.ChangeToken token = registry.changeToken();
                long external = externalChanges.get();
                if (!token.equals(observedToken) || external != observedExternal) {
                    observedToken = token;
                    observedExternal = external;
                    dirty = true;
                }

                boolean retryDue = nextRetry != Long.MAX_VALUE && now >= nextRetry;
                boolean heartbeatDue = nextRetry == Long.MAX_VALUE && now >= nextHeartbeat;
                boolean changeDue = nextRetry == Long.MAX_VALUE && dirty
                        && now - lastAttempt >= minChangeIntervalMillis;
                if (retryDue || heartbeatDue || changeDue) {
                    RuntimeStatusRegistry.ChangeToken sendingToken = registry.changeToken();
                    long sendingExternal = externalChanges.get();
                    lastAttempt = now;
                    try {
                        transport.report(registry.snapshot(clock.instant()));
                        failures = 0;
                        nextRetry = Long.MAX_VALUE;
                        nextHeartbeat = now + heartbeatMillis;
                        dirty = !registry.changeToken().equals(sendingToken)
                                || externalChanges.get() != sendingExternal;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        failures = Math.min(30, failures + 1);
                        long retryAfter = e instanceof StatusClient.StatusException status
                                && status.retryAfterMillis() != null ? status.retryAfterMillis()
                                : retryDelay(failures);
                        nextRetry = now + Math.min(retryMaxMillis, Math.max(1, retryAfter));
                        dirty = true;
                        error.accept("节点状态上报失败，将退避后发送最新快照；采集、上传和控制继续运行: "
                                + safe(e));
                    }
                    continue;
                }

                long deadline = Math.min(nextHeartbeat, nextRetry);
                if (dirty && nextRetry == Long.MAX_VALUE) {
                    deadline = Math.min(deadline, lastAttempt + minChangeIntervalMillis);
                }
                long wait = Math.min(CHANGE_DETECTION_MILLIS, Math.max(1, deadline - now));
                synchronized (wakeup) {
                    if (accepting.get()) wakeup.wait(wait);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            terminated.countDown();
        }
    }

    private long retryDelay(int failures) {
        int shift = Math.min(20, failures - 1);
        long base;
        try { base = Math.multiplyExact(retryInitialMillis, 1L << shift); }
        catch (ArithmeticException e) { base = retryMaxMillis; }
        base = Math.min(retryMaxMillis, base);
        double spread = base * retryJitterRatio;
        long jitter = spread == 0 ? 0 : (long) ((random.nextDouble() * 2 - 1) * spread);
        return Math.max(1, Math.min(retryMaxMillis, base + jitter));
    }

    private String safe(Exception e) {
        String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        return message.replaceAll("(?i)(accesskey|secret|password|token|private[_ -]?key)[^ ,;]*", "[REDACTED]")
                .replaceAll("https?://[^ ]+", "[ENDPOINT_REDACTED]");
    }

    boolean awaitTermination(Duration timeout) throws InterruptedException {
        return terminated.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        if (!accepting.compareAndSet(true, false)) return;
        synchronized (wakeup) { wakeup.notifyAll(); }
        if (!started.get()) terminated.countDown();
        if (started.get() && Thread.currentThread() != thread) {
            try {
                if (!terminated.await(shutdownGraceSeconds, TimeUnit.SECONDS)) {
                    thread.interrupt();
                    terminated.await(Math.min(5, shutdownGraceSeconds), TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                thread.interrupt();
                Thread.currentThread().interrupt();
            }
        }
        if (closeableTransport != null) {
            try { closeableTransport.close(); }
            catch (Exception e) { error.accept("关闭节点状态客户端失败: " + safe(e)); }
        }
    }
}

package cn.kjky.collector.status;

import cn.kjky.collector.outbox.OutboxDelivery;
import cn.kjky.collector.outbox.OutboxStore;
import cn.kjky.collector.outbox.OutboxStoredBatch;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** 在 OutboxStore 同一互斥区中读取 pending/retry/BLOCKED 的一致统计。 */
public record OutboxStatistics(Bucket pending, Bucket retry, Bucket blocked) {
    public static OutboxStatistics capture(OutboxStore outbox, Instant now) throws IOException {
        Objects.requireNonNull(outbox, "outbox 不能为空");
        Objects.requireNonNull(now, "now 不能为空");
        synchronized (outbox) {
            List<OutboxStoredBatch> all = new ArrayList<>(outbox.pendingBatches());
            all.addAll(outbox.retryBatches());
            Mutable pending = new Mutable();
            Mutable retry = new Mutable();
            Mutable blocked = new Mutable();
            for (OutboxStoredBatch batch : all) {
                Mutable target = switch (batch.delivery().state()) {
                    case RETRY -> retry;
                    case BLOCKED -> blocked;
                    case PENDING, SPLIT -> pending;
                };
                target.add(batch, directoryBytes(batch));
            }
            return new OutboxStatistics(pending.freeze(now), retry.freeze(now), blocked.freeze(now));
        }
    }

    private static long directoryBytes(OutboxStoredBatch batch) throws IOException {
        try (var files = Files.walk(batch.directory())) {
            return files.filter(Files::isRegularFile).mapToLong(path -> {
                try { return Files.size(path); }
                catch (IOException e) { throw new SizeFailure(e); }
            }).sum();
        } catch (SizeFailure e) {
            throw e.cause;
        }
    }

    public record Bucket(long count, long bytes, long oldestAgeSeconds) {}

    private static final class Mutable {
        long count;
        long bytes;
        Instant oldest;

        void add(OutboxStoredBatch batch, long size) {
            count++;
            bytes = Math.addExact(bytes, size);
            Instant created = batch.delivery().createdAt();
            if (oldest == null || created.isBefore(oldest)) oldest = created;
        }

        Bucket freeze(Instant now) {
            long age = oldest == null ? 0 : Math.max(0, Duration.between(oldest, now).getSeconds());
            return new Bucket(count, bytes, age);
        }
    }

    private static final class SizeFailure extends RuntimeException {
        final IOException cause;
        SizeFailure(IOException cause) { this.cause = cause; }
    }
}

package cn.kjky.collector.testbed.cc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;

final class BatchStore {
    enum ResultType { COMMITTED, ALREADY_COMMITTED, CONFLICT }

    record CommitResult(ResultType type, Instant committedAt) {
    }

    private final Path receivedDirectory;
    private final Clock clock;

    BatchStore(Path stateDirectory, Clock clock) throws IOException {
        this.receivedDirectory = stateDirectory.toAbsolutePath().normalize().resolve("received");
        this.clock = clock;
        Files.createDirectories(receivedDirectory);
    }

    synchronized CommitResult commit(BatchValidator.ValidatedBatch batch,
                                     ClientCertificateIdentity certificate) throws IOException {
        Path target = pathFor(batch.batchId());
        if (Files.exists(target)) return compareExisting(target, batch);

        Instant committedAt = clock.instant();
        ObjectNode stored = JsonSupport.MAPPER.createObjectNode();
        stored.put("storage_schema_version", "1.0");
        stored.put("batch_id", batch.batchId());
        stored.put("content_sha256", batch.contentSha256());
        stored.put("record_count", batch.recordCount());
        stored.put("committed_at", committedAt.toString());
        ObjectNode peer = stored.putObject("client_certificate");
        peer.put("fingerprint_sha256", certificate.fingerprintSha256());
        peer.put("subject", certificate.subjectSummary());
        peer.put("not_after", certificate.notAfter().toString());
        stored.set("batch", batch.json());
        byte[] bytes = JsonSupport.write(stored);

        Path temporary = Files.createTempFile(receivedDirectory, ".commit-", ".tmp");
        boolean moved = false;
        try {
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                throw new IOException("state filesystem does not support atomic batch publication", e);
            }
            moved = true;
            forceDirectoryBestEffort();
            return new CommitResult(ResultType.COMMITTED, committedAt);
        } catch (java.nio.file.FileAlreadyExistsException e) {
            return compareExisting(target, batch);
        } finally {
            if (!moved) Files.deleteIfExists(temporary);
        }
    }

    synchronized ArrayNode list(boolean includePayload) throws IOException {
        ArrayNode result = JsonSupport.MAPPER.createArrayNode();
        try (var files = Files.list(receivedDirectory)) {
            for (Path path : files.filter(p -> p.getFileName().toString().endsWith(".json"))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString())).toList()) {
                JsonNode stored = JsonSupport.MAPPER.readTree(Files.readAllBytes(path));
                if (includePayload) {
                    result.add(stored);
                } else {
                    ObjectNode summary = result.addObject();
                    summary.put("batch_id", stored.path("batch_id").asText());
                    summary.put("content_sha256", stored.path("content_sha256").asText());
                    summary.put("record_count", stored.path("record_count").asInt());
                    summary.put("committed_at", stored.path("committed_at").asText());
                    summary.set("client_certificate", stored.path("client_certificate"));
                }
            }
        }
        return result;
    }

    synchronized Counts counts() throws IOException {
        long batches = 0;
        long events = 0;
        try (var files = Files.list(receivedDirectory)) {
            for (Path path : files.filter(p -> p.getFileName().toString().endsWith(".json")).toList()) {
                JsonNode stored = JsonSupport.MAPPER.readTree(Files.readAllBytes(path));
                batches++;
                events += stored.path("record_count").asLong();
            }
        }
        return new Counts(batches, events);
    }

    synchronized void deleteCommitted() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(receivedDirectory)) {
            for (Path path : stream) {
                if (Files.isRegularFile(path)) Files.delete(path);
            }
        }
        forceDirectoryBestEffort();
    }

    private CommitResult compareExisting(Path target, BatchValidator.ValidatedBatch batch) throws IOException {
        JsonNode existing = JsonSupport.MAPPER.readTree(Files.readAllBytes(target));
        if (!batch.batchId().equals(existing.path("batch_id").asText())) {
            throw new IOException("persisted batch key collision");
        }
        Instant committedAt = Instant.parse(existing.path("committed_at").asText());
        return batch.contentSha256().equals(existing.path("content_sha256").asText())
                ? new CommitResult(ResultType.ALREADY_COMMITTED, committedAt)
                : new CommitResult(ResultType.CONFLICT, committedAt);
    }

    private Path pathFor(String batchId) {
        return receivedDirectory.resolve(JsonSupport.sha256(batchId.getBytes(java.nio.charset.StandardCharsets.UTF_8)) + ".json");
    }

    private void forceDirectoryBestEffort() {
        try (FileChannel channel = FileChannel.open(receivedDirectory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
            // Windows and some filesystems do not expose directory fsync. File fsync + atomic move still apply.
        }
    }

    record Counts(long batches, long events) {
    }
}

package cn.kjky.collector.runtime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/** 原子保存最后一次成功应用的远程运行策略。 */
public final class RuntimePolicyStore {
    public static final String FILE_NAME = "runtime-policy.json";
    public static final String SCHEMA_VERSION = "1.0";

    private final Path file;
    private final Path partial;
    private final ObjectMapper json = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    private RuntimePolicy current;

    public RuntimePolicyStore(Path file) throws IOException {
        this.file = Objects.requireNonNull(file, "file 不能为空").toAbsolutePath().normalize();
        if (this.file.getParent() == null) throw new IllegalArgumentException("运行策略文件必须有父目录");
        this.partial = this.file.resolveSibling(this.file.getFileName() + ".partial");
        Files.createDirectories(this.file.getParent());
        if (Files.exists(partial)) {
            throw new IOException("发现未完成的运行策略临时文件，已失败关闭: " + partial);
        }
        if (Files.exists(this.file)) current = readAndValidate();
    }

    public synchronized Optional<RuntimePolicy> load() {
        return Optional.ofNullable(current);
    }

    public synchronized void save(RuntimePolicy policy) throws IOException {
        validate(policy);
        byte[] bytes = json.writeValueAsBytes(policy);
        try (FileChannel channel = FileChannel.open(partial,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) channel.write(buffer);
            channel.force(true);
        }
        try {
            Files.move(partial, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            throw new IOException("运行策略所在文件系统不支持原子替换: " + file, e);
        }
        current = policy;
    }

    public Path file() {
        return file;
    }

    private RuntimePolicy readAndValidate() throws IOException {
        try {
            RuntimePolicy policy = json.readValue(file.toFile(), RuntimePolicy.class);
            validate(policy);
            return policy;
        } catch (Exception e) {
            throw new IOException("运行策略状态文件损坏，已失败关闭: " + file, e);
        }
    }

    private void validate(RuntimePolicy policy) throws IOException {
        if (policy == null || !SCHEMA_VERSION.equals(policy.schemaVersion())) {
            throw new IOException("不支持或缺失的运行策略 Schema");
        }
        if (policy.configVersion() < 1) throw new IOException("config_version 必须大于 0");
        if (policy.requestedQuotaBytes() <= 0 || policy.effectiveQuotaBytes() <= 0) {
            throw new IOException("运行策略配额必须为正数");
        }
        if (policy.requestedQuotaBytes() != policy.effectiveQuotaBytes()) {
            throw new IOException("成功持久化的远程配额必须保持期望值与生效值一致");
        }
        if (policy.appliedAt() == null) throw new IOException("运行策略缺少 applied_at");
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = false)
    public record RuntimePolicy(String schemaVersion, long configVersion,
                                long requestedQuotaBytes, long effectiveQuotaBytes,
                                Instant appliedAt) {
        public RuntimePolicy {
            Objects.requireNonNull(schemaVersion, "schemaVersion 不能为空");
            Objects.requireNonNull(appliedAt, "appliedAt 不能为空");
        }

        public static RuntimePolicy quota(long configVersion, long quotaBytes, Instant appliedAt) {
            return new RuntimePolicy(SCHEMA_VERSION, configVersion, quotaBytes, quotaBytes, appliedAt);
        }
    }
}

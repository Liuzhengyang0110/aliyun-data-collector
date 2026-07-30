package cn.kjky.collector.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 从 YAML 文件读取 {@link CollectorConfig}。
 * 未定义字段会直接报错，避免现场因字段拼写错误而静默使用默认值。
 */
public final class ConfigLoader {
    /** YAML 解析器；开启未知字段检查。 */
    private final ObjectMapper yaml = new ObjectMapper(new YAMLFactory())
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    /**
     * 加载配置文件。
     *
     * @param path YAML 配置文件路径
     * @return 反序列化后的采集配置
     * @throws IOException 文件读取或 YAML 解析失败
     * @throws IllegalArgumentException path 不是普通文件
     */
    public CollectorConfig load(Path path) throws IOException {
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("配置文件不存在: " + path.toAbsolutePath());
        }
        return yaml.readValue(path.toFile(), CollectorConfig.class);
    }
}

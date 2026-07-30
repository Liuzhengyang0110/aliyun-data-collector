package cn.kjky.collector.config;

/**
 * 凭据环境变量读取器。
 * 实际凭据只在创建 API 客户端时进入内存，不进入 YAML、日志或 manifest。
 */
public final class SecretResolver {
    /**
     * 读取必需的环境变量。
     *
     * @param envName 环境变量名，而不是凭据实际值
     * @return 环境变量保存的凭据
     * @throws IllegalStateException 环境变量不存在或为空
     */
    public String required(String envName) {
        // value 是当前 Java 进程继承到的环境变量值。
        String value = System.getenv(envName);
        if (value == null || value.trim().isEmpty()) throw new IllegalStateException("环境变量未设置: " + envName);
        return value;
    }

    /**
     * 读取可选环境变量。
     *
     * @param envName 环境变量名；为空时表示不启用该可选凭据
     * @return 环境变量值；未配置或为空时返回 null
     */
    public String optional(String envName) {
        if (envName == null || envName.trim().isEmpty()) return null;
        String value = System.getenv(envName);
        return value == null || value.trim().isEmpty() ? null : value;
    }
}

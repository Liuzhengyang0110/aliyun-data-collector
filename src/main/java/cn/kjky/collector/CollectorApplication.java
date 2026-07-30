package cn.kjky.collector;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.config.ConfigLoader;
import cn.kjky.collector.config.ConfigValidator;
import cn.kjky.collector.service.CollectorEngine;
import cn.kjky.collector.service.ManifestRebuilder;
import cn.kjky.collector.service.OutputValidator;

import java.nio.file.Paths;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 程序命令行入口。
 * <p>
 * 负责解析命令和参数、加载并校验配置，然后把具体工作交给
 * {@link CollectorEngine}。该类不直接访问 ARMS 或 SLS API。
 * </p>
 */
public final class CollectorApplication {
    /** 工具类不允许实例化。 */
    private CollectorApplication() {}

    /**
     * JVM 入口。执行命令并在失败时向操作系统返回非零退出码。
     *
     * @param args 命令行参数；第一个元素是命令，后续元素是
     *             {@code --config}、{@code --start}、{@code --end} 等选项
     */
    public static void main(String[] args) {
        // exit 是 run 方法约定的进程退出码：0 成功，非 0 表示不同类型的失败。
        int exit = run(args);
        if (exit != 0) System.exit(exit);
    }

    /**
     * 执行一条完整命令，便于主方法和单元测试共同调用。
     *
     * @param args 原始命令行参数
     * @return 退出码：0 成功、1 一般错误、2 配置错误、3 输出校验错误
     */
    static int run(String[] args) {
        try {
            if (args.length == 0 || "--help".equals(args[0]) || "help".equals(args[0])) {
                help();
                return 0;
            }
            // command 决定后续分支；统一转小写，避免用户输入大小写造成差异。
            String command = args[0].toLowerCase();
            // options 保存去掉“--”后的参数名和值，例如 config -> config/site.yaml。
            Map<String, String> options = options(args);
            String configPath = options.get("config");
            if (configPath == null) throw new IllegalArgumentException("缺少 --config <YAML文件>");
            // config 是 YAML 反序列化后的完整运行配置，后续服务只使用这个对象。
            CollectorConfig config = new ConfigLoader().load(Paths.get(configPath));
            // 只有实际访问 API 的命令才要求当前进程已设置凭据环境变量。
            boolean requireSecrets = Arrays.asList("probe", "discover", "collect", "resume").contains(command);
            List<String> errors = new ConfigValidator().validate(config, requireSecrets);
            if (!errors.isEmpty()) {
                System.err.println("配置校验失败:");
                errors.forEach(e -> System.err.println("- " + e));
                return 2;
            }
            // engine 统一编排发现、探测、预演和采集流程。
            CollectorEngine engine = new CollectorEngine(config);
            if ("validate-config".equals(command)) {
                System.out.println("配置有效（未读取凭据内容）");
                return 0;
            }
            if ("probe".equals(command)) {
                System.out.println(engine.probe());
                return 0;
            }
            if ("discover".equals(command)) {
                System.out.println(engine.discover());
                return 0;
            }
            if ("dry-run".equals(command)) {
                Instant[] timeRange = range(options);
                System.out.println(engine.dryRun(timeRange[0], timeRange[1]));
                return 0;
            }
            if ("collect".equals(command) || "resume".equals(command)) {
                Instant[] timeRange = range(options);
                engine.collect(timeRange[0], timeRange[1]);
                System.out.println("采集完成: " + Paths.get(config.outputRoot).toAbsolutePath().normalize());
                return 0;
            }
            if ("validate-output".equals(command)) {
                List<String> validation = new OutputValidator().validate(
                        Paths.get(config.outputRoot).toAbsolutePath().normalize());
                if (validation.isEmpty()) {
                    System.out.println("输出校验通过");
                    return 0;
                }
                validation.forEach(e -> System.err.println("- " + e));
                return 3;
            }
            if ("build-manifest".equals(command)) {
                System.out.println("已生成: " + new ManifestRebuilder().rebuild(config));
                return 0;
            }
            throw new IllegalArgumentException("未知命令: " + command);
        } catch (Exception e) {
            System.err.println("执行失败: " + safe(e));
            return 1;
        }
    }

    /**
     * 把 {@code --名称 值} 形式的命令行选项转换成 Map。
     *
     * @param args 包含命令和选项的原始参数；索引 0 的命令会被跳过
     * @return 保持输入顺序的参数 Map，键不包含前缀 {@code --}
     * @throws IllegalArgumentException 参数不是 {@code --key value} 形式时抛出
     */
    private static Map<String, String> options(String[] args) {
        // result 使用 LinkedHashMap，调试时能保持用户输入的参数顺序。
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 1; i < args.length; i++) {
            // key 是带“--”的原始参数名；下一项必须是它的值。
            String key = args[i];
            if (!key.startsWith("--")) throw new IllegalArgumentException("无法识别的参数: " + key);
            if (i + 1 >= args.length || args[i + 1].startsWith("--")) throw new IllegalArgumentException("参数缺少值: " + key);
            result.put(key.substring(2), args[++i]);
        }
        return result;
    }

    /**
     * 读取并校验采集开始、结束时间。
     *
     * @param options 命令行选项 Map，需要包含 start 和 end
     * @return 长度为 2 的数组：索引 0 是开始时刻，索引 1 是结束时刻
     * @throws IllegalArgumentException 缺少时间、开始不早于结束或包含亚秒精度时抛出
     */
    private static Instant[] range(Map<String, String> options) {
        String start = options.get("start");
        String end = options.get("end");
        if (start == null || end == null) throw new IllegalArgumentException("该命令需要 --start 和 --end（ISO-8601，必须含时区）");
        // s、e 都转换成 UTC 时间轴上的 Instant，原输入可以使用 +08:00 等偏移量。
        Instant s = instant(start);
        Instant e = instant(end);
        if (!s.isBefore(e)) throw new IllegalArgumentException("--start 必须早于 --end");
        if (s.getNano() != 0 || e.getNano() != 0) throw new IllegalArgumentException("SLS 为秒级查询，--start 和 --end 请使用整秒时间");
        return new Instant[]{s, e};
    }

    /**
     * 解析 ISO-8601 时间。优先接受 UTC 的 Instant 格式，再接受带偏移量格式。
     *
     * @param value 时间字符串，例如 {@code 2026-07-27T09:00:00+08:00}
     * @return 对应的绝对时刻
     * @throws java.time.format.DateTimeParseException 两种格式都无法解析时抛出
     */
    private static Instant instant(String value) {
        try { return Instant.parse(value); }
        catch (Exception ignored) { return OffsetDateTime.parse(value).toInstant(); }
    }

    /**
     * 生成适合输出到终端的异常摘要，并脱敏可能出现的凭据关键词。
     *
     * @param e 待摘要的异常
     * @return “异常类型: 脱敏消息”格式的字符串
     */
    private static String safe(Exception e) {
        String message = e.getMessage() == null ? "" : e.getMessage();
        return e.getClass().getSimpleName() + ": " + message.replaceAll("(?i)(accesskey|secret|password|token)[^ ,;]*", "[REDACTED]");
    }

    /** 打印支持的命令和基本用法。 */
    private static void help() {
        System.out.println(
                "阿里云原生只读数据采集程序\n" +
                "用法: java -jar aliyun-data-collector.jar <command> --config <site.yaml> " +
                "[--start <ISO时间> --end <ISO时间>]\n\n" +
                "command:\n" +
                "  validate-config  校验配置结构，不访问 API\n" +
                "  probe            只读连通性与权限探测\n" +
                "  discover         列出 ARMS 应用和 SLS Logstore\n" +
                "  dry-run          展示分窗和采集任务，不访问 API\n" +
                "  collect          执行采集\n" +
                "  resume           按 checkpoint 断点续采\n" +
                "  validate-output  校验文件存在性、大小和 SHA-256\n" +
                "  build-manifest   从 raw 文件重建辅助 manifest");
    }
}

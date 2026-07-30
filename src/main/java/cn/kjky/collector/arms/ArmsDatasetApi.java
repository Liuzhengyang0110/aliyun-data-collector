package cn.kjky.collector.arms;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.config.SecretResolver;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/** 通过 JDK HttpClient 调用 ARMS Dataset 表单查询接口。 */
public final class ArmsDatasetApi implements ArmsApi {
    /** Dataset URL 和凭据变量名配置。 */
    private final CollectorConfig.ArmsConfig config;
    /** 复用连接的 JDK HTTP 客户端。 */
    private final HttpClient client;
    /** Dataset 用户名，同时作为请求中的 _userId。 */
    private final String username;
    /** 可选 HTTP Basic 密码；现场接口不要求时为 null。 */
    private final String password;
    /** 单次 HTTP 请求超时秒数。 */
    private final int timeoutSeconds;

    /**
     * 创建 Dataset API 客户端。
     *
     * @param config ARMS Dataset 配置
     * @param secrets 环境变量凭据读取器
     * @param timeoutSeconds 建连和单次请求超时秒数
     */
    public ArmsDatasetApi(CollectorConfig.ArmsConfig config, SecretResolver secrets, int timeoutSeconds) {
        this.config = config;
        this.username = secrets.required(config.dataset.usernameEnv);
        this.password = secrets.optional(config.dataset.passwordEnv);
        this.timeoutSeconds = timeoutSeconds;
        this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(timeoutSeconds)).build();
    }

    /**
     * 把参数编码为 application/x-www-form-urlencoded 并调用 Dataset URL。
     *
     * @param action 只允许 QueryDataset，其他动作会被安全拦截
     * @param parameters Dataset ID、时间范围、Measures 和额外参数
     * @return 原始 HTTP 响应正文和请求元数据
     * @throws Exception URL、网络、中断或 HTTP 错误
     */
    @Override
    public ApiResponse call(String action, Map<String, String> parameters) throws Exception {
        if (!"QueryDataset".equals(action)) throw new SecurityException("Dataset 模式仅允许 QueryDataset");
        // form 复制调用方参数后补充 _userId，避免修改原 Map。
        Map<String, String> form = new LinkedHashMap<>(parameters);
        form.put("_userId", username);
        // body 保存 URL 编码后的表单正文。
        StringBuilder body = new StringBuilder();
        form.forEach((k, v) -> {
            if (v == null) return;
            if (!body.isEmpty()) body.append('&');
            body.append(enc(k)).append('=').append(enc(v));
        });
        // builder 先构造公共请求头；若有密码，后续再加入 Basic Authorization。
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(config.dataset.url))
                .timeout(Duration.ofSeconds(Math.max(1, timeoutSeconds)))
                .header("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()));
        if (password != null) {
            // credential 是 username:password 的 Base64，仅用于当前请求头，绝不输出。
            String credential = Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
            builder.header("Authorization", "Basic " + credential);
        }
        // response 使用 UTF-8 字符串保留 Dataset 返回的原始 JSON。
        HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() == 408 || response.statusCode() == 429 || response.statusCode() >= 500) {
            throw new java.io.IOException("Dataset HTTP 暂时失败: " + response.statusCode());
        }
        if (response.statusCode() >= 400) throw new IllegalStateException("Dataset HTTP 请求失败: " + response.statusCode());
        return new ApiResponse(response.body(), response.statusCode(), response.headers().firstValue("x-acs-request-id").orElse(null));
    }

    /**
     * 对表单键和值执行 UTF-8 URL 编码。
     *
     * @param value 原始表单文本
     * @return URL 编码后的文本
     */
    private String enc(String value) { return URLEncoder.encode(value, StandardCharsets.UTF_8); }
}

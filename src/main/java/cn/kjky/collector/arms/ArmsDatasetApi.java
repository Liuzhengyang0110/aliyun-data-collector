package cn.kjky.collector.arms;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.config.SecretResolver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/** 通过 JDK HttpClient 调用 ARMS Dataset 表单查询接口。 */
public final class ArmsDatasetApi implements ArmsApi {
    /** Dataset URL 和凭据变量名配置。 */
    private final CollectorConfig.ArmsConfig config;
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
            if (body.length() > 0) body.append('&');
            try {
                body.append(enc(k)).append('=').append(enc(v));
            } catch (java.io.UnsupportedEncodingException e) {
                throw new IllegalStateException("JVM 不支持 UTF-8", e);
            }
        });

        // Java 8 没有 java.net.http.HttpClient，因此使用标准库 HttpURLConnection。
        HttpURLConnection connection = (HttpURLConnection) new URL(config.dataset.url).openConnection();
        int timeoutMillis = (int) Math.min(Integer.MAX_VALUE, Math.max(1, timeoutSeconds) * 1000L);
        connection.setConnectTimeout(timeoutMillis);
        connection.setReadTimeout(timeoutMillis);
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
        if (password != null) {
            // credential 是 username:password 的 Base64，仅用于当前请求头，绝不输出。
            String credential = Base64.getEncoder().encodeToString(
                    (username + ":" + password).getBytes(StandardCharsets.UTF_8));
            connection.setRequestProperty("Authorization", "Basic " + credential);
        }

        try {
            byte[] requestBody = body.toString().getBytes(StandardCharsets.UTF_8);
            connection.setFixedLengthStreamingMode(requestBody.length);
            try (OutputStream out = connection.getOutputStream()) {
                out.write(requestBody);
            }

            int status = connection.getResponseCode();
            InputStream responseStream = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
            String responseBody = responseStream == null ? "" : readUtf8(responseStream);
            if (status == 408 || status == 429 || status >= 500) {
                throw new IOException("Dataset HTTP 暂时失败: " + status);
            }
            if (status >= 400) throw new IllegalStateException("Dataset HTTP 请求失败: " + status);
            return new ApiResponse(responseBody, status, connection.getHeaderField("x-acs-request-id"));
        } finally {
            connection.disconnect();
        }
    }

    /**
     * 对表单键和值执行 UTF-8 URL 编码。
     *
     * @param value 原始表单文本
     * @return URL 编码后的文本
     */
    private String enc(String value) throws java.io.UnsupportedEncodingException {
        return URLEncoder.encode(value, "UTF-8");
    }

    /**
     * 读取并关闭 HTTP 响应流。
     *
     * @param input 响应输入流
     * @return UTF-8 响应正文
     * @throws IOException 读取失败
     */
    private String readUtf8(InputStream input) throws IOException {
        try (InputStream in = input; ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int count;
            while ((count = in.read(buffer)) != -1) {
                out.write(buffer, 0, count);
            }
            return new String(out.toByteArray(), StandardCharsets.UTF_8);
        }
    }
}

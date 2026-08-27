package cn.kjky.collector.arms;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.config.SecretResolver;
import com.aliyuncs.CommonRequest;
import com.aliyuncs.CommonResponse;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.http.ProtocolType;
import com.aliyuncs.profile.DefaultProfile;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.util.Map;

/** 使用阿里云核心 SDK 签名并调用 ARMS RPC API。 */
public final class ArmsRpcApi implements ArmsApi {
    /** 仅用于从 RPC JSON 正文回退提取 RequestId。 */
    private static final ObjectMapper JSON = new ObjectMapper();
    /** ARMS Endpoint、API/资源地域、协议、产品、请求头和 API 版本配置。 */
    private final CollectorConfig.ArmsConfig config;
    /** 阿里云 RPC SDK 客户端。 */
    private final DefaultAcsClient client;
    /** 从 endpoint 中提取出的纯域名，供 CommonRequest 使用。 */
    private final String domain;

    /**
     * 创建 RPC 客户端，并从环境变量读取 AccessKey。
     *
     * @param config ARMS RPC 配置
     * @param secrets 环境变量凭据读取器
     */
    public ArmsRpcApi(CollectorConfig.ArmsConfig config, SecretResolver secrets) {
        this.config = config;
        // accessKeyId、accessKeySecret 仅保存在构造过程和 SDK 凭据对象中。
        String accessKeyId = secrets.required(config.accessKeyIdEnv);
        String accessKeySecret = secrets.required(config.accessKeySecretEnv);
        // apiRegionId 是专有云 API 网关/SDK 路由地域；它与接口查询参数
        // RegionId（资源地域）可能不同，例如 bjdc-1 与 zj-3。
        this.domain = domain(config.endpoint);
        DefaultProfile.addEndpoint(config.apiRegionId, config.product, this.domain);
        // profile 绑定 API 路由地域与签名凭据。
        DefaultProfile profile = DefaultProfile.getProfile(config.apiRegionId, accessKeyId, accessKeySecret);
        this.client = new DefaultAcsClient(profile);
        this.client.setAutoRetry(false);
    }

    /**
     * 组装并执行一条 POST RPC 请求。
     *
     * @param action 已由上层白名单检查的 ARMS 动作
     * @param parameters 动作特有参数；空值不会发送
     * @return ARMS 原始响应及请求元数据
     * @throws Exception SDK 签名、网络或服务端错误
     */
    @Override
    public ApiResponse call(String action, Map<String, String> parameters) throws Exception {
        CommonRequest request = buildRequest(config, domain, action, parameters);
        // response.data 保留服务端原始 JSON，不在适配器层做业务转换。
        CommonResponse response = client.getCommonResponse(request);
        String requestId = requestId(response);
        return new ApiResponse(response.getData(), response.getHttpStatus(), requestId);
    }

    /**
     * 按专有云 ARMS 文档组装请求，供正式调用和单元测试复用。
     * apiRegionId 只用于 SDK/网关路由；RegionId 作为业务查询参数发送。
     */
    static CommonRequest buildRequest(CollectorConfig.ArmsConfig config, String domain,
                                      String action, Map<String, String> parameters) {
        CommonRequest request = new CommonRequest();
        request.setSysMethod(MethodType.POST);
        request.setSysProtocol(ProtocolType.valueOf(config.protocol.toUpperCase()));
        request.setSysDomain(domain);
        request.setSysVersion(config.version);
        request.setSysProduct(config.product);
        request.setSysAction(action);
        request.setSysRegionId(config.apiRegionId);
        request.putQueryParameter("RegionId", config.regionId);
        putHeader(request, "x-acs-organizationid", config.organizationId);
        putHeader(request, "x-acs-resourcegroupid", config.resourceGroupId);
        parameters.forEach((k, v) -> {
            if (v != null && !v.trim().isEmpty()) request.putQueryParameter(k, v);
        });
        return request;
    }

    /** 非空现场值才写入请求头，兼容不要求组织/资源组的环境。 */
    private static void putHeader(CommonRequest request, String name, String value) {
        if (value != null && !value.trim().isEmpty() && !value.startsWith("__REQUIRED")) {
            request.putHeadParameter(name, value.trim());
        }
    }

    /** 关闭 SDK HTTP 连接池。 */
    @Override public void close() { client.shutdown(); }

    /**
     * 获取用于现场排障的请求 ID。部分 HTTP 实现会保留响应头原始大小写，
     * 所以依次尝试常见写法；若响应头不可用，再从标准 RPC JSON 正文读取。
     */
    private String requestId(CommonResponse response) {
        if (response.getHttpResponse() != null) {
            for (String name : new String[]{"x-acs-request-id", "X-Acs-Request-Id", "X-ACS-REQUEST-ID"}) {
                String value = response.getHttpResponse().getHeaderValue(name);
                if (value != null && !value.trim().isEmpty()) return value;
            }
        }
        try {
            JsonNode root = JSON.readTree(response.getData());
            for (String name : new String[]{"RequestId", "RequestID", "requestId"}) {
                JsonNode value = root == null ? null : root.get(name);
                if (value != null && value.isValueNode() && !value.asText().trim().isEmpty()) return value.asText();
            }
        } catch (Exception ignored) {
            // 原始响应仍会按原逻辑返回；RequestId 缺失不能使采集失败。
        }
        return null;
    }

    /**
     * 把可能带协议和路径的 Endpoint 转成 SDK 需要的域名。
     *
     * @param endpoint YAML 中配置的 Endpoint
     * @return 不含协议和路径的 authority/domain
     */
    private String domain(String endpoint) {
        // value 是去除首尾空白后的 Endpoint。
        String value = endpoint.trim();
        if (value.contains("://")) return URI.create(value).getAuthority();
        int slash = value.indexOf('/');
        return slash < 0 ? value : value.substring(0, slash);
    }
}

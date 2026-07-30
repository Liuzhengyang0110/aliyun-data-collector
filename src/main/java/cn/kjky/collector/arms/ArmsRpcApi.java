package cn.kjky.collector.arms;

import cn.kjky.collector.config.CollectorConfig;
import cn.kjky.collector.config.SecretResolver;
import com.aliyuncs.CommonRequest;
import com.aliyuncs.CommonResponse;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.http.ProtocolType;
import com.aliyuncs.profile.DefaultProfile;

import java.net.URI;
import java.util.Map;

/** 使用阿里云核心 SDK 签名并调用 ARMS RPC API。 */
public final class ArmsRpcApi implements ArmsApi {
    /** ARMS Endpoint、协议、RegionId、产品和 API 版本配置。 */
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
        // profile 绑定 RegionId 与签名凭据。
        DefaultProfile profile = DefaultProfile.getProfile(config.regionId, accessKeyId, accessKeySecret);
        this.client = new DefaultAcsClient(profile);
        this.client.setAutoRetry(false);
        this.domain = domain(config.endpoint);
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
        // request 包含 SDK 所需的公共字段和调用方传入的动作参数。
        CommonRequest request = new CommonRequest();
        request.setSysMethod(MethodType.POST);
        request.setSysProtocol(ProtocolType.valueOf(config.protocol.toUpperCase()));
        request.setSysDomain(domain);
        request.setSysVersion(config.version);
        request.setSysProduct(config.product);
        request.setSysAction(action);
        request.setSysRegionId(config.regionId);
        request.putQueryParameter("RegionId", config.regionId);
        parameters.forEach((k, v) -> {
            if (v != null && !v.trim().isEmpty()) request.putQueryParameter(k, v);
        });
        // response.data 保留服务端原始 JSON，不在适配器层做业务转换。
        CommonResponse response = client.getCommonResponse(request);
        String requestId = response.getHttpResponse() == null ? null : response.getHttpResponse().getHeaderValue("x-acs-request-id");
        return new ApiResponse(response.getData(), response.getHttpStatus(), requestId);
    }

    /** 关闭 SDK HTTP 连接池。 */
    @Override public void close() { client.shutdown(); }

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

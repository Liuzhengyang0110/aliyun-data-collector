package cn.kjky.collector.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * YAML 配置的根对象。
 * 所有现场相关值都从该对象读取，避免把 Endpoint、PID、Project 等写死在代码中。
 */
public class CollectorConfig {
    /** 配置及输出清单使用的结构版本。 */
    public String schemaVersion = "0.1.1";
    /** 项目编号，用于文件名和 manifest。 */
    public String projectCode;
    /** 实验环境编号，用于区分不同现场环境。 */
    public String envCode;
    /** 采集批次编号；同一输出目录只能保存同一批次。 */
    public String batchId;
    /** 触发本次采集的测试案例编号。 */
    public String caseId;
    /** 现场解释时间时使用的时区名称。 */
    public String timezone = "Asia/Shanghai";
    /** 兼容旧配置的输出根目录；新配置使用 storage.dataRoot。 */
    public String outputRoot;
    /** 采集端身份，用于后续上传、心跳和任务领取。 */
    public CollectorIdentityConfig collector = new CollectorIdentityConfig();
    /** 通用运行参数。 */
    public RuntimeConfig runtime = new RuntimeConfig();
    /** 单个任务窗口的分页硬限制，达到限制但未证明完整时失败。 */
    public PaginationLimits pagination = new PaginationLimits();
    /** 常驻模式下的数据源轮询调度参数。 */
    public SchedulerConfig scheduler = new SchedulerConfig();
    /** 向 cc-ingest 主动上传时使用的参数。 */
    public UploadConfig upload = new UploadConfig();
    /** 中控通信的 TLS 与客户端身份参数。 */
    public SecurityConfig security = new SecurityConfig();
    /** 本地数据、Outbox 和保留策略。 */
    public StorageConfig storage = new StorageConfig();
    /** 主动领取中控任务和发送心跳的参数。 */
    public ControlConfig control = new ControlConfig();
    /** ARMS 数据源参数。 */
    public ArmsConfig arms = new ArmsConfig();
    /** SLS 数据源参数。 */
    public SlsConfig sls = new SlsConfig();

    /** 返回当前配置实际使用的数据根目录，并兼容旧 outputRoot。 */
    public String effectiveDataRoot() {
        if (storage != null && storage.dataRoot != null && !storage.dataRoot.isBlank()) return storage.dataRoot;
        if (outputRoot != null && !outputRoot.isBlank()) return outputRoot;
        return "./data-package";
    }

    /** 返回开发模式 raw 相对于数据根目录的目录。 */
    public String effectiveRawDirectory() {
        if (storage != null && storage.rawPersistence != null
                && storage.rawPersistence.directory != null && !storage.rawPersistence.directory.isBlank()) {
            return storage.rawPersistence.directory;
        }
        return "raw";
    }

    /** 返回水位等常驻状态相对于数据根目录的目录。 */
    public String effectiveStateDirectory() {
        if (storage != null && storage.stateDir != null && !storage.stateDir.isBlank()) return storage.stateDir;
        return "state";
    }

    /** 当前配置是否允许保存开发调试用 raw 文件。 */
    public boolean rawPersistenceEnabled() {
        return storage != null && storage.rawPersistence != null && storage.rawPersistence.enabled;
    }

    /** 采集端在中控中的稳定身份。 */
    public static class CollectorIdentityConfig {
        /** 采集实例唯一标识。 */
        public String collectorId;
        /** 现场或集群站点标识。 */
        public String siteId;
        /** 采集器类型；本项目固定为 ALIYUN。 */
        public String collectorType = "ALIYUN";
    }

    /** 控制时间分窗、超时、重试和轮询行为的参数。 */
    public static class RuntimeConfig {
        /** 部署模式：DEVELOPMENT 保留调试能力，PRODUCTION 执行生产安全约束。 */
        public String deploymentMode = "DEVELOPMENT";
        /** 单个逻辑采集窗口的秒数。 */
        public int windowSeconds = 600;
        /** HTTP 建连和请求的超时秒数。 */
        public int requestTimeoutSeconds = 30;
        /** 首次调用之外允许的最大重试次数。 */
        public int maxRetries = 3;
        /** 指数退避的基础毫秒数。 */
        public long retryBaseMillis = 500;
        /** 云 API 查询退避的最大毫秒数，防止长时间故障时无上限增长。 */
        public long retryMaxDelayMillis = 30000;
        /** 云 API 查询退避的随机抖动比例，0.2 表示上下 20%。 */
        public double retryJitterRatio = 0.2d;
        /** SLS 未完成查询的最大轮询次数。 */
        public int maxPollAttempts = 10;
        /** 两次 SLS 轮询之间的等待毫秒数。 */
        public long pollIntervalMillis = 1000;
    }

    public static class PaginationLimits {
        public int maxPages = 1000;
        public long maxRecords = 1000000;
    }

    /** 本地响应适配策略；JSON Pointer 必须按现场文档配置，不推测专有字段。 */
    public static class PageMapping {
        public String totalPointer;
        public String positionPointer;
        public String hasMorePointer;
        public String truncatedPointer;
        public String nextCursorPointer;
        /** 仅 Dataset CURSOR 模式使用；不会自动选择请求参数名。 */
        public String cursorParameter;
        /** Dataset 默认要求响应提供结束证据；CURSOR 需要显式游标映射。 */
        public String strategy = "SINGLE_RESPONSE";
    }

    /** 常驻模式的公共调度参数和各数据源调度参数。 */
    public static class SchedulerConfig {
        /** 每轮调度允许的正负随机抖动秒数。 */
        public int jitterSeconds = 3;
        /** 停止时等待正在执行的采集安全结束的最长秒数。 */
        public int shutdownGraceSeconds = 60;
        /** 补偿查询的执行间隔秒数。 */
        public int compensationIntervalSeconds = 600;
        /** 每次补偿向前回看的秒数。 */
        public int compensationLookbackSeconds = 1800;
        /** ARMS 的轮询参数。 */
        public SourceScheduleConfig arms = new SourceScheduleConfig(30, 90, 300);
        /** SLS 的轮询参数。 */
        public SourceScheduleConfig sls = new SourceScheduleConfig(20, 30, 120);
    }

    /** 批量上传到 cc-ingest 的边界参数。 */
    public static class UploadConfig {
        /** 是否启用主动上传；当前阶段默认关闭。 */
        public boolean enabled;
        /** cc-ingest 批量接口完整 URL。 */
        public String ingestUrl;
        /** 单批最大事件数。 */
        public int maxRecords = 1000;
        /** 单批 gzip 后最大字节数。 */
        public long maxCompressedBytes = 2L * 1024 * 1024;
        /** 单批 gzip 前 JSON 允许的最大字节数，避免中控解压后超限。 */
        public long maxRequestBytes = 10L * 1024 * 1024;
        /** 最早事件在组批器中的最大等待毫秒数。 */
        public long maxWaitMillis = 5000;
        /** 同时允许上传的批次数。 */
        public int maxInFlightBatches = 2;
        /** 建立连接的超时秒数。 */
        public int connectTimeoutSeconds = 10;
        /** 完整上传请求的超时秒数。 */
        public int requestTimeoutSeconds = 30;
        /** 可重试失败的首次退避毫秒数。 */
        public long retryInitialDelayMillis = 1000;
        /** 指数退避允许达到的最大毫秒数。 */
        public long retryMaxDelayMillis = 60000;
        /** 退避随机抖动比例，0.2 表示在计算值上下 20% 内随机。 */
        public double retryJitterRatio = 0.2d;
        /** 单批达到该尝试次数时产生告警，但仍继续保留并重试。 */
        public int retryWarningAttempts = 10;
    }

    /** mTLS 客户端证书、私钥或 KeyStore/TrustStore 配置。 */
    public static class SecurityConfig {
        /** 是否启用 mTLS。 */
        public boolean mtlsEnabled;
        /** 客户端身份模式：PEM 或 KEYSTORE。 */
        public String clientAuthMode = "KEYSTORE";
        /** PEM 模式客户端证书路径。 */
        public String clientCertificatePath;
        /** PEM 模式私钥路径。 */
        public String privateKeyPath;
        /** KEYSTORE 模式客户端 KeyStore 路径。 */
        public String keyStorePath;
        /** KeyStore 类型：PKCS12 或 JKS。 */
        public String keyStoreType = "PKCS12";
        /** 保存 KeyStore 密码的环境变量名。 */
        public String keyStorePasswordEnv;
        /** 服务端信任库路径。 */
        public String trustStorePath;
        /** TrustStore 类型：PKCS12 或 JKS。 */
        public String trustStoreType = "PKCS12";
        /** 保存 TrustStore 密码的环境变量名。 */
        public String trustStorePasswordEnv;
        /** 客户端证书剩余有效期小于等于该天数时产生告警。 */
        public int certificateExpiryWarningDays = 30;
        /** 检查 Secret Volume 文件更新和证书有效期的周期秒数。 */
        public int certificateReloadIntervalSeconds = 30;
    }

    /** 数据根目录、Outbox、状态目录和容量保护策略。 */
    public static class StorageConfig {
        /** 开发输出或生产 Outbox/状态文件的数据根目录。 */
        public String dataRoot;
        /** 相对于 dataRoot 的 Outbox 目录。 */
        public String outboxDir = "outbox";
        /** 相对于 dataRoot 的采集水位和上传水位目录。 */
        public String stateDir = "state";
        /** 开发模式 raw 持久化配置。 */
        public RawPersistenceConfig rawPersistence = new RawPersistenceConfig();
        /** 达到该磁盘使用百分比后告警并抑制非必要补偿。 */
        public int diskWarningWatermarkPercent = 80;
        /** 达到该磁盘使用百分比后暂停创建新批次。 */
        public int diskCriticalWatermarkPercent = 90;
        /** 容量从临界状态下降到该百分比后恢复采集。 */
        public int diskResumeWatermarkPercent = 75;
        /** Outbox/PVC 容量检查周期，单位为秒。 */
        public int diskCheckIntervalSeconds = 10;
        /** 与 PVC 物理保护相互独立的 Outbox 逻辑配额。 */
        public OutboxQuotaConfig outboxQuota = new OutboxQuotaConfig();
    }

    /** Outbox 逻辑配额的本地安全边界和滞回水位。 */
    public static class OutboxQuotaConfig {
        /** 尚未应用远程策略时的本地期望配额，默认 10 GiB。 */
        public long defaultBytes = 10L * 1024 * 1024 * 1024;
        /** 中控或本地策略允许设置的最小配额，默认 1 GiB。 */
        public long minBytes = 1024L * 1024 * 1024;
        /** 中控或本地策略允许设置的最大配额，默认 20 GiB。 */
        public long maxBytes = 20L * 1024 * 1024 * 1024;
        /** PVC 中始终留给文件系统、状态文件和上传过程的安全空间，默认 1 GiB。 */
        public long reservedFreeBytes = 1024L * 1024 * 1024;
        /** 逻辑配额告警水位。 */
        public int warningPercent = 80;
        /** 逻辑配额临界水位。 */
        public int criticalPercent = 90;
        /** 从逻辑临界状态恢复采集的水位。 */
        public int resumePercent = 75;
        /** Outbox 全量校正及物理安全上限复核周期，单位为秒。 */
        public int checkIntervalSeconds = 10;
    }

    /** 开发阶段可选的原始响应落盘策略。 */
    public static class RawPersistenceConfig {
        /** DEVELOPMENT 默认开启；PRODUCTION 必须关闭。 */
        public boolean enabled = true;
        /** 相对于 dataRoot 的 raw 目录。 */
        public String directory = "raw";
    }

    /** 主动领取任务和上报心跳的配置。 */
    public static class ControlConfig {
        /** 是否启用中控任务与心跳。 */
        public boolean enabled;
        /** 领取任务的完整 URL。 */
        public String taskClaimUrl;
        /** 回传任务状态的完整 URL 模板，必须包含 {job_id}。 */
        public String jobStatusUrl;
        /** P1-02 节点状态上报 URL。 */
        public String statusReportUrl;
        /** 心跳间隔秒数。 */
        public int heartbeatIntervalSeconds = 30;
        /** 状态快速变化合并后的最小上报间隔毫秒数。 */
        public long statusChangeMinIntervalMillis = 1000;
        /** 任务轮询间隔秒数。 */
        public int taskPollIntervalSeconds = 7;
        /** 任务轮询的正负随机抖动秒数。 */
        public int taskPollJitterSeconds = 2;
        /** 控制请求建连超时秒数。 */
        public int connectTimeoutSeconds = 10;
        /** 控制请求超时秒数。 */
        public int requestTimeoutSeconds = 30;
        /** 控制服务暂时不可用时的初始退避毫秒数。 */
        public long retryInitialDelayMillis = 1000;
        /** 控制服务暂时不可用时的最大退避毫秒数。 */
        public long retryMaxDelayMillis = 30000;
        /** 退避随机抖动比例，取值 0..1。 */
        public double retryJitterRatio = 0.2d;
    }

    /** 单个数据源的轮询频率、数据可见性延迟和首次回看范围。 */
    public static class SourceScheduleConfig {
        /** 两轮采集之间的基础间隔秒数。 */
        public int pollIntervalSeconds;
        /** 查询终点相对当前时间向前偏移的秒数。 */
        public int visibilityDelaySeconds;
        /** 没有历史水位时首次采集向前回看的秒数。 */
        public int initialLookbackSeconds;

        /** 供 YAML 反序列化使用。 */
        public SourceScheduleConfig() {}

        /** 创建带默认值的数据源调度配置。 */
        public SourceScheduleConfig(int pollIntervalSeconds, int visibilityDelaySeconds, int initialLookbackSeconds) {
            this.pollIntervalSeconds = pollIntervalSeconds;
            this.visibilityDelaySeconds = visibilityDelaySeconds;
            this.initialLookbackSeconds = initialLookbackSeconds;
        }
    }

    /** ARMS RPC 或 Dataset 采集配置。 */
    public static class ArmsConfig {
        /** 是否启用 ARMS。 */
        public boolean enabled;
        /** 访问模式：RPC 或 DATASET。 */
        public String mode = "RPC";
        /** RPC API 域名；现场填写，不包含固定公有云假设。 */
        public String endpoint;
        /** RPC 请求协议：HTTP 或 HTTPS。 */
        public String protocol = "HTTP";
        /** ARMS 地域标识。 */
        public String regionId;
        /** RPC 产品名，通常为 ARMS。 */
        public String product = "ARMS";
        /** 现场 ARMS API 版本。 */
        public String version;
        /** 保存 AccessKey ID 的环境变量名。 */
        public String accessKeyIdEnv = "ARMS_ACCESS_KEY_ID";
        /** 保存 AccessKey Secret 的环境变量名。 */
        public String accessKeySecretEnv = "ARMS_ACCESS_KEY_SECRET";
        /** 链路搜索每页数量，接口上限为 100。 */
        public int pageSize = 100;
        /** 搜索到 TraceID 后是否继续查询完整 Trace 详情。 */
        public boolean collectTraceDetails = true;
        /** 需要执行的链路查询任务。 */
        public List<TraceQuery> traceQueries = new ArrayList<>();
        /** 需要执行的指标查询任务。 */
        public List<MetricQuery> metricQueries = new ArrayList<>();
        /** DATASET 模式的专用配置。 */
        public DatasetConfig dataset = new DatasetConfig();
    }

    /** 一条 ARMS 链路搜索任务。空的筛选字段不会发送给 API。 */
    public static class TraceQuery {
        public PageMapping pagination = new PageMapping();
        /** 任务唯一名称，用于 checkpoint。 */
        public String name;
        /** 现有命名规范中的记录类型。 */
        public String recordType;
        /** ARMS 应用 PID。 */
        public String pid;
        /** 可选服务名筛选条件。 */
        public String serviceName;
        /** 可选操作/接口名筛选条件。 */
        public String operationName;
        /** 可选 TraceID 精确筛选条件。 */
        public String traceId;
        /** 可选最小耗时。 */
        public Long minDuration;
        /** 可选最大耗时。 */
        public Long maxDuration;
        /** 专有云版本需要的额外 RPC 参数。 */
        public Map<String, String> parameters = new LinkedHashMap<>();
        /** 原始记录转标准事件时使用的字段准入和身份字段。 */
        public NormalizationConfig normalization = new NormalizationConfig();
    }

    /** 一条 ARMS 指标查询任务。 */
    public static class MetricQuery {
        public PageMapping pagination = new PageMapping();
        /** 任务唯一名称，用于 checkpoint。 */
        public String name;
        /** 现有命名规范中的记录类型。 */
        public String recordType;
        /** 仅支持 QueryMetric；QueryMetricByPage 协议未确认，配置校验拒绝。 */
        public String action = "QueryMetric";
        /** ARMS 指标名称。 */
        public String metric;
        /** 需要返回的测量值，编码为 Measures.1、Measures.2 等。 */
        public List<String> measures = new ArrayList<>();
        /** 需要返回的维度，编码为 Dimensions.1、Dimensions.2 等。 */
        public List<String> dimensions = new ArrayList<>();
        /** ARMS 自定义过滤表达式。 */
        public List<String> customFilters = new ArrayList<>();
        /** 结构化过滤条件，例如 pid 和 regionId。 */
        public Map<String, String> filters = new LinkedHashMap<>();
        /** 现场版本的额外 RPC 参数；可覆盖非受保护字段，禁止覆盖时间窗和 Limit。 */
        public Map<String, String> parameters = new LinkedHashMap<>();
        /** 指标数据片间隔；实际单位以现场 API 文档为准。 */
        public int interval = 60;
        /** 单次指标查询的返回上限。 */
        public int limit = 10000;
        /** 原始记录转标准事件时使用的字段准入和身份字段。 */
        public NormalizationConfig normalization = new NormalizationConfig();
    }

    /** ARMS Dataset 接口连接参数。 */
    public static class DatasetConfig {
        /** 完整 Dataset HTTP URL。 */
        public String url;
        /** 保存 Dataset 用户名的环境变量名。 */
        public String usernameEnv = "ARMS_DATASET_USERNAME";
        /** 保存可选 HTTP Basic 密码的环境变量名。 */
        public String passwordEnv = "ARMS_DATASET_PASSWORD";
        /** 需要执行的 Dataset 查询任务。 */
        public List<DatasetQuery> queries = new ArrayList<>();
    }

    /** 一条 ARMS Dataset 查询任务。 */
    public static class DatasetQuery {
        public PageMapping pagination = new PageMapping();
        /** 任务唯一名称，用于 checkpoint。 */
        public String name;
        /** 现有命名规范中的记录类型。 */
        public String recordType;
        /** 现场提供的 Dataset ID。 */
        public String datasetId;
        /** Dataset 返回数据的时间间隔秒数。 */
        public int intervalInSec = 60;
        /** 需要返回的测量值，现场文档规定最多 3 个。 */
        public List<String> measures = new ArrayList<>();
        /** 额外表单参数。 */
        public Map<String, String> parameters = new LinkedHashMap<>();
        /** 原始记录转标准事件时使用的字段准入和身份字段。 */
        public NormalizationConfig normalization = new NormalizationConfig();
    }

    /** SLS 连接和项目配置。 */
    public static class SlsConfig {
        /** 是否启用 SLS。 */
        public boolean enabled;
        /** SLS API Endpoint。 */
        public String endpoint;
        /** 保存 SLS AccessKey ID 的环境变量名。 */
        public String accessKeyIdEnv = "SLS_ACCESS_KEY_ID";
        /** 保存 SLS AccessKey Secret 的环境变量名。 */
        public String accessKeySecretEnv = "SLS_ACCESS_KEY_SECRET";
        /** 保存可选 STS Security Token 的环境变量名。 */
        public String stsTokenEnv = "SLS_STS_TOKEN";
        /** 需要发现或采集的 SLS Project。 */
        public List<SlsProject> projects = new ArrayList<>();
    }

    /** 一个 SLS Project 及其目标 Logstore。 */
    public static class SlsProject {
        /** SLS Project 名称。 */
        public String project;
        /** 该 Project 下需要采集的 Logstore。 */
        public List<SlsLogstore> logstores = new ArrayList<>();
    }

    /** 一个 SLS Logstore 查询任务。 */
    public static class SlsLogstore {
        /** Logstore 名称。 */
        public String logstore;
        /** 现有命名规范中的记录类型。 */
        public String recordType;
        /** SLS Topic 筛选；空字符串表示不限 Topic。 */
        public String topic = "";
        /** SLS 查询表达式；空字符串表示查询时间范围内全部日志。 */
        public String query = "";
        /** 每页日志数量，当前限制为 1 到 100。 */
        public int pageSize = 100;
        /** 是否按时间倒序返回日志。 */
        public boolean reverse;
        /** 原始记录转标准事件时使用的字段准入和身份字段。 */
        public NormalizationConfig normalization = new NormalizationConfig();
    }

    /**
     * 单个采集任务的标准化策略。
     * 空列表表示使用对应转换器的保守内置规则；配置后只准入列出的 payload 字段。
     */
    public static class NormalizationConfig {
        /** 允许进入标准事件 payload 的原始字段名；字段名匹配忽略大小写。 */
        public List<String> payloadFields = new ArrayList<>();
        /** 现场能够保证稳定的原始记录身份字段，按配置顺序参与 event_id。 */
        public List<String> identityFields = new ArrayList<>();
    }
}

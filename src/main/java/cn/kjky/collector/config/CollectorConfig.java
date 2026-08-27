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
    /** 所有 raw、manifest、checkpoint 和日志的根目录。 */
    public String outputRoot = "./data-package";
    /** 通用运行参数。 */
    public RuntimeConfig runtime = new RuntimeConfig();
    /** ARMS 数据源参数。 */
    public ArmsConfig arms = new ArmsConfig();
    /** SLS 数据源参数。 */
    public SlsConfig sls = new SlsConfig();

    /** 控制时间分窗、超时、重试和轮询行为的参数。 */
    public static class RuntimeConfig {
        /** 单个逻辑采集窗口的秒数。 */
        public int windowSeconds = 600;
        /** HTTP 建连和请求的超时秒数。 */
        public int requestTimeoutSeconds = 30;
        /** 首次调用之外允许的最大重试次数。 */
        public int maxRetries = 3;
        /** 指数退避的基础毫秒数。 */
        public long retryBaseMillis = 500;
        /** SLS 未完成查询的最大轮询次数。 */
        public int maxPollAttempts = 10;
        /** 两次 SLS 轮询之间的等待毫秒数。 */
        public long pollIntervalMillis = 1000;
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
        /** 专有云 API 网关/SDK 路由地域，例如 bjdc-1。 */
        public String apiRegionId;
        /** ARMS 资源所在地域，作为接口查询参数 RegionId 发送，例如 zj-3。 */
        public String regionId;
        /** 可选组织 ID；非空时写入 x-acs-organizationid 请求头。 */
        public String organizationId;
        /** 可选资源组 ID；非空时写入 x-acs-resourcegroupid 请求头。 */
        public String resourceGroupId;
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
    }

    /** 一条 ARMS 指标查询任务。 */
    public static class MetricQuery {
        /** 任务唯一名称，用于 checkpoint。 */
        public String name;
        /** 现有命名规范中的记录类型。 */
        public String recordType;
        /** 指标 API 动作：QueryMetric 或 QueryMetricByPage。 */
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
        /** 现场版本的额外 RPC 参数；同名时覆盖程序生成值。 */
        public Map<String, String> parameters = new LinkedHashMap<>();
        /** 指标数据片间隔；实际单位以现场 API 文档为准。 */
        public int interval = 60;
        /** 单次指标查询的返回上限。 */
        public int limit = 10000;
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
    }
}

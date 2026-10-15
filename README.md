# 阿里云原生数据采集程序

本程序面向 Apsara Stack/阿里云专有云现场环境，用 Java 只读调用 ARMS 和 SLS API。开发模式可将链路、指标和日志保存为本地 raw JSON；生产模式将其标准化、组批并经 Outbox 主动上传到 `cc-ingest`。程序既支持指定时间范围的一次性采集，也支持常驻采集与上传。

本文按照“出发前准备—现场配置—权限探测—短时间试采—正式采集—结果校验—断点恢复”的顺序说明完整使用方法。

## 1. 已实现的能力

- ARMS RPC 模式：应用发现、链路搜索、链路详情和指标查询。
- ARMS Dataset 模式：通过 Dataset 查询接口采集数据。
- SLS 模式：发现 Logstore，并按时间窗口、offset 分页查询日志。
- ARMS/SLS 分页具备最大页数/记录数、重复页、位置推进和总数一致性保护；无法判断完整性时失败关闭，不推进采集水位。
- 按固定时间长度分窗，避免一次请求时间跨度过大。
- ARMS RPC、ARMS Dataset、SLS 使用彼此独立的查询错误分类和有上限抖动退避；SLS 未完成查询自动轮询。
- 每完成一页即落盘，不需要等全部采集结束才保存。
- 使用 checkpoint 记录进度，支持中断后继续采集。
- 提供 `daemon` 常驻模式，按独立周期轮询 ARMS 和 SLS。
- 按“当前时间减可见性延迟”确定查询终点，并持久化各数据源成功水位。
- 单轮 API 失败不结束常驻进程；失败轮次不推进水位，后续轮次自动重试。
- 调度支持随机抖动、系统时间回拨保护和停止信号下的优雅退出。
- 配置模型已覆盖采集端身份、补偿参数、Ingest 上传、mTLS、Outbox 存储和中控任务参数，并提供完整边界校验。
- 已冻结 `cc-ingest` v1 批量协议，并提供 gzip HTTP 客户端、可靠回执校验和本地模拟接口测试。
- 已冻结 `cc-control` v1 控制、任务结果和节点状态协议；已实现主动任务领取、幂等状态回传、人工暂停/恢复、远程 Outbox 配额调整，以及独立节点状态心跳与变化补发。
- 已实现独立 `BatchUploader`：Outbox 原子认领、配置化并发、带抖动退避、Retry-After、BLOCKED/认证暂停、413 确定性拆分，以及上传水位更新后删除。
- `daemon` 已将采集循环和上传循环独立串联：任一侧暂时失败不会停止另一侧，空队列按通知等待，到期 retry 按 `next_retry_at` 自动恢复。
- 已实现 PVC 文件式 Outbox：批次目录原子发布、pending/retry/BLOCKED 状态、启动恢复和单进程并发认领。
- 已实现生产 PVC 物理容量与 Outbox 逻辑配额双重保护：独立的 NORMAL/WARNING/CRITICAL/RECOVERING 滞回状态机、硬配额原子预留、临界暂停、恢复采集和上传持续运行。
- 已实现独立 `UploadWatermarkStore`：按 `poll_id` 保存连续提交边界、稀疏 ACK、末批和完成状态，并在完成后压缩逐批证据。
- 已实现 ARMS Trace/Span/Metric/Dataset 与 SLS 日志到 `StandardEvent` 的纯转换组件，包括稳定 `event_id`、多格式时间、字段准入和无原值告警。
- 已实现采集端 mTLS：从只读 Secret Volume 加载 PEM 或 KeyStore 身份和 TrustStore，强制服务端证书链与主机名校验，支持到期告警和安全轮换。
- 自动生成 manifest，记录文件大小、记录数、Request ID 和 SHA-256。
- 使用 `.partial` 临时文件和原子改名，避免把未写完的文件当成正式结果。
- 配置、原始文件、日志和 manifest 中不保存 AccessKey Secret。

## 2. 安全边界

程序只提供查询入口，不提供资源修改入口。

- ARMS 只允许：`ListTraceApps`、`SearchTracesByPage`、`GetTrace`、`QueryMetric`、`QueryDataset`。`QueryMetricByPage` 因现场分页协议未确认，在配置校验和运行入口均被拒绝。
- SLS 只调用：`ListLogStores`、`GetLogs`。
- 代码中没有创建、更新、删除、写日志、创建索引、分裂或合并 Shard 的调用。
- AccessKey、密码和 STS Token 只从运行进程的环境变量读取。
- Endpoint、RegionId、API Version、PID、Dataset ID、Project、Logstore、查询条件和命名字段均由 YAML 配置提供。

即使现场账号权限较大，程序本身也不会主动调用写入或删除接口。现场仍建议使用最小权限的只读账号。

## 3. 项目目录

```text
aliyun-data-collector/
├─ config/
│  ├─ site.example.yaml          ARMS RPC + SLS 配置模板
│  └─ arms-dataset.example.yaml  ARMS Dataset 配置模板
├─ deploy/                       Kubernetes 生产部署模板与说明
├─ src/                          Java 源代码和测试
├─ target/                       Maven 构建产物
├─ Dockerfile                    Java 17 内部基础镜像运行制品
├─ pom.xml                       Maven 项目配置
└─ README.md                     本说明
```

默认可执行文件为：

```text
target/aliyun-data-collector-0.1.0-SNAPSHOT.jar
```

## 4. 运行环境

运行程序需要：

- Windows PowerShell。
- JDK 17 或更高版本。
- 能访问现场 ARMS/SLS Endpoint 的网络。
- 已开通相应只读 API 权限的 AccessKey。
- 已填写完成的现场 YAML 配置。

检查 Java：

```powershell
java -version
```

如果已经有构建好的 JAR，现场机器不需要 Maven。只有修改代码或重新打包时才需要 Maven 3.9+：

```powershell
mvn clean package
```

构建过程会运行单元测试；成功后在 `target` 目录生成 JAR。

协议测试使用 `src/test/resources/contracts/` 中随代码提交的 OpenAPI 契约快照，不依赖被忽略的独立 `docs/` 文档仓库。更新协议时，应从文档仓库同步对应快照并运行协议测试。

Kubernetes 生产部署必须使用现场内部 JRE 17 基础镜像的固定 digest；Dockerfile 不提供公网基础镜像默认值，也不会在运行阶段下载依赖。完整的镜像构建、Secret/PVC、网络策略、安全上下文和现场验收说明见 [deploy/README.md](deploy/README.md)。当前尚无可靠的 Kubernetes liveness/readiness 接口，部署模板明确保留 TODO，不以 Java 进程存在或镜像内未确认的命令冒充健康检查。

查看程序帮助：

```powershell
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar --help
```

## 5. 现场需要确认的信息

首次运行前，需要从控制台和甲方文档确认以下内容。

### 5.1 ARMS RPC

- ARMS API Endpoint。
- 使用 HTTP 还是 HTTPS。
- RegionId。
- API Version；专有云版本不能直接假设与公有云一致。
- 应用 PID。
- 确认 `QueryMetric` 的返回上限；如果现场仅提供 `QueryMetricByPage`，需先提供分页协议并实现适配，当前版本不能启用。
- 需要采集的 Metric、Measures、Dimensions、Filters。
- `IntervalInSec` 在现场版本中的实际单位和最小值。

### 5.2 ARMS Dataset

- 完整 Dataset URL。
- Dataset ID。
- 用户名或 `_userId`。
- 是否需要 HTTP Basic 密码。
- Measures、额外查询参数和时间间隔。
- 完整结束标志、总数、截断标志、游标字段和请求参数、最大返回条数；没有完整性证据时不会按单页成功处理。

### 5.3 SLS

- SLS Endpoint。
- Project 名称。
- Logstore 名称。
- Logstore 是否已经建立索引。
- Topic 和查询表达式。
- AccessKey 是否具有 Project、Logstore 和日志读取权限。

### 5.4 本次采集批次

- `projectCode`：项目编号。
- `envCode`：实验环境编号。
- `batchId`：本次采集批次。
- `caseId`：测试案例编号。
- `recordType`：沿用现有命名规范中的记录类型。
- `collector.collectorId`：采集实例的稳定唯一标识。
- `collector.siteId`：现场或集群站点标识。
- `storage.dataRoot`：本批次数据保存位置。

建议不同批次使用不同的 `storage.dataRoot`，避免把多批数据混在同一个 manifest 中。旧配置中的 `outputRoot` 仍可作为兼容回退，但不能与 `storage.dataRoot` 同时指向不同目录。

## 6. 创建现场配置

### 6.1 RPC + SLS 模式

复制模板：

```powershell
Copy-Item config/site.example.yaml config/site.yaml
```

编辑 `config/site.yaml`，替换所有 `__REQUIRED_ON_SITE__`。不要把 AccessKey 实际值写入 YAML。

配置结构示意：

```yaml
projectCode: "kjky20250400"
envCode: "test01"
batchId: "batch001"
caseId: "case01"
timezone: "Asia/Shanghai"

collector:
  collectorId: "aliyun-collector-site01"
  siteId: "site01"
  collectorType: "ALIYUN"

runtime:
  deploymentMode: "DEVELOPMENT"
  windowSeconds: 600
  maxRetries: 3
  retryBaseMillis: 500
  retryMaxDelayMillis: 30000
  retryJitterRatio: 0.2
  maxPollAttempts: 10

scheduler:
  jitterSeconds: 3
  shutdownGraceSeconds: 60
  compensationIntervalSeconds: 600
  compensationLookbackSeconds: 1800
  arms:
    pollIntervalSeconds: 30
    visibilityDelaySeconds: 90
    initialLookbackSeconds: 300
  sls:
    pollIntervalSeconds: 20
    visibilityDelaySeconds: 30
    initialLookbackSeconds: 120

upload:
  enabled: false
  ingestUrl: null
  maxRecords: 1000
  maxCompressedBytes: 2097152
  maxRequestBytes: 10485760
  maxWaitMillis: 5000
  maxInFlightBatches: 2
  connectTimeoutSeconds: 10
  requestTimeoutSeconds: 30
  retryInitialDelayMillis: 1000
  retryMaxDelayMillis: 60000
  retryJitterRatio: 0.2
  retryWarningAttempts: 10

security:
  mtlsEnabled: false
  clientAuthMode: "KEYSTORE"
  clientCertificatePath: null
  privateKeyPath: null
  keyStorePath: null
  keyStoreType: "PKCS12"
  keyStorePasswordEnv: null
  trustStorePath: null
  trustStoreType: "PKCS12"
  trustStorePasswordEnv: null
  certificateExpiryWarningDays: 30
  certificateReloadIntervalSeconds: 30

storage:
  dataRoot: "./data-package/batch001"
  outboxDir: "outbox"
  stateDir: "state"
  rawPersistence:
    enabled: true
    directory: "raw"
  diskWarningWatermarkPercent: 80
  diskCriticalWatermarkPercent: 90
  diskResumeWatermarkPercent: 75
  diskCheckIntervalSeconds: 10
  outboxQuota:
    defaultBytes: 10737418240
    minBytes: 1073741824
    maxBytes: 21474836480
    reservedFreeBytes: 1073741824
    warningPercent: 80
    criticalPercent: 90
    resumePercent: 75
    checkIntervalSeconds: 10

control:
  enabled: false
  taskClaimUrl: null
  jobStatusUrl: null
  statusReportUrl: null # 启用 control 时填写固定 /collector/v1/status 地址
  heartbeatIntervalSeconds: 30
  statusChangeMinIntervalMillis: 1000
  taskPollIntervalSeconds: 7
  taskPollJitterSeconds: 2
  connectTimeoutSeconds: 10
  requestTimeoutSeconds: 30
  retryInitialDelayMillis: 1000
  retryMaxDelayMillis: 30000
  retryJitterRatio: 0.2

arms:
  enabled: true
  mode: "RPC"
  endpoint: "现场ARMS地址"
  protocol: "HTTP"
  regionId: "现场RegionId"
  product: "ARMS"
  version: "现场API版本"
  accessKeyIdEnv: "ARMS_ACCESS_KEY_ID"
  accessKeySecretEnv: "ARMS_ACCESS_KEY_SECRET"
  traceQueries:
    - name: "all-traces"
      recordType: "topology_edge"
      pid: "现场应用PID"
      normalization:
        payloadFields: []
        identityFields: ["TraceID"]
  metricQueries:
    - name: "service-metric"
      recordType: "flow_feature"
      action: "QueryMetric"
      metric: "现场Metric"
      measures: ["现场Measure"]
      normalization:
        payloadFields: ["timestamp", "value", "metric", "dimensions"]
        identityFields: []
      filters:
        pid: "现场应用PID"
        regionId: "现场RegionId"

sls:
  enabled: true
  endpoint: "现场SLS地址"
  accessKeyIdEnv: "SLS_ACCESS_KEY_ID"
  accessKeySecretEnv: "SLS_ACCESS_KEY_SECRET"
  projects:
    - project: "现场Project"
      logstores:
        - logstore: "现场Logstore"
          recordType: "flow_record"
          topic: ""
          query: ""
          pageSize: 100
          normalization:
            payloadFields: ["message", "method", "path", "status", "trace_id", "service_id"]
            identityFields: ["request_id"]
```

模板中包含更多可选字段，应以模板和甲方 API 文档为准。

`upload.enabled`、`security.mtlsEnabled` 和 `control.enabled` 默认关闭。启用 `upload.enabled` 后，`daemon` 会同时启动相互独立的采集循环和上传循环，持续消费 Outbox 的 pending 与到期 retry。启用 `control.enabled` 时必须关闭 raw 并使用生产 Outbox 路径，同时填写固定路径的 `taskClaimUrl`、带唯一 `{job_id}` 占位符的 `jobStatusUrl` 和固定路径的 `statusReportUrl`。默认任务轮询基线为 7 秒、正负 2 秒抖动，即约 5～9 秒；节点心跳默认 30 秒一次，状态连续变化按 `statusChangeMinIntervalMillis` 合并限频；通信失败按 1～30 秒有界抖动退避。mTLS 已同时接入上传、任务控制与节点状态客户端；PEM 模式使用 `clientCertificatePath` 和 `privateKeyPath`，KEYSTORE 模式使用 `keyStorePath`，两种模式不能混用，密码只通过环境变量名引用。

`runtime.requireManualStart` 默认 `false`，保持现有自动运行行为。受控部署可显式设为 `true`：它只适用于 `daemon`，要求 `control.enabled=true` 且关闭 `storage.rawPersistence.enabled`。首次干净启动会先原子持久化全局人工暂停（`config_version=0`），然后才启动调度；控制领取、状态上报和 Outbox 上传仍独立运行。已有暂停状态按原策略恢复。若暂停状态缺失但状态目录或 Outbox 已有文件，程序会按旧状态迁移场景失败关闭，要求先对账旧版本/未决任务并显式处理，不能静默重置版本。一次性 `collect`/`resume` 命令保持原行为；它们不属于页面控制路径，启用该开关时不能运行。

接口字段、哈希算法、状态码和中控端实现要求见 [cc-ingest v1 批量上传接口契约](docs/cc-ingest-v1接口契约.md)，机器可读定义见 [OpenAPI 3.1 文件](docs/openapi/cc-ingest-v1.openapi.yaml)。`upload.ingestUrl` 必须使用固定路径 `/collector/v1/events:batch`。

控制任务、任务结果、节点心跳、远程 Outbox 逻辑配额及 mTLS 身份绑定规则见 [cc-control v1 接口契约](docs/cc-control-v1接口契约.md)，机器可读定义见 [cc-control OpenAPI 3.1 文件](docs/openapi/cc-control-v1.openapi.yaml)。P1-01 已接入 `/collector/v1/jobs:claim` 与 `/collector/v1/jobs/{job_id}/status`，P1-02 已接入 `/collector/v1/status`；采集端只主动出站连接，不开放入站控制或状态 HTTP 接口。首批只执行 `PAUSE_COLLECTION`、`RESUME_COLLECTION` 和 `SET_OUTBOX_QUOTA`，拒绝 Shell、任意路径/URL、AccessKey、私钥及未批准参数。

控制任务每次先把 RECEIVED/RUNNING/终态写入 `state/control-jobs.json`，再尽力回传。相同 `job_id` 和相同内容只重发已保存状态；内容冲突、过期、身份不匹配、未知命令、非法参数和旧 `config_version` 均被拒绝。人工暂停保存在 `state/manual-pause-state.json`，最后有效远程配额保存在 `state/runtime-policy.json`。这些文件均使用同目录临时文件、`fsync` 和同文件系统原子替换，损坏时失败关闭。控制服务断开不会结束 daemon，也不会清除最后有效策略；上传循环在人工暂停或容量 CRITICAL 时继续排空 Outbox。

节点状态由独立 `StatusReporter` 线程生成并主动发送，不依赖采集、上传或控制任务成功。快照包含采集端身份和启动时间、最新配置版本/控制任务摘要、ARMS/SLS 成功失败与任务级采集水位、上传水位与在途数、pending/retry/BLOCKED 数量/字节/最老年龄，以及 PVC 物理容量和 Outbox 逻辑配额。总体状态为 `RUNNING`、`PAUSED` 或 `DEGRADED`；人工暂停、物理容量暂停、逻辑配额暂停和上传鉴权暂停按冻结协议中的不同 `status_reasons` 组合表达。

人工暂停/恢复、容量四态变化、上传鉴权暂停/恢复、BLOCKED 出现、远程配额生效或拒绝会触发变化补发；200 ms 轻量变更检测与默认 1000 ms 最小变化间隔用于合并状态风暴。上报失败只在状态线程内有界退避，恢复后重新生成并发送最新快照，不将心跳写入业务 Outbox，也不补发历史快照。载荷只包含有限状态分类和统计，不读取或发送 AccessKey、密码、私钥、Secret 路径、完整 Endpoint、原始响应或 `StandardEvent.payload`。中控应按最后接收时间推导 `OFFLINE`，采集端不会伪造离线状态。

### 6.2 开发与生产模式

同一套程序通过 `runtime.deploymentMode` 和 `storage.rawPersistence.enabled` 切换运行方式：

```yaml
# 开发或现场排障：保留原始响应
runtime:
  deploymentMode: "DEVELOPMENT"
storage:
  rawPersistence:
    enabled: true
    directory: "raw"
```

```yaml
# 集群生产部署：禁止持久化原始响应
runtime:
  deploymentMode: "PRODUCTION"
storage:
  rawPersistence:
    enabled: false
    directory: "raw" # 仅保留配置结构，不会创建目录
```

`PRODUCTION` 模式只允许关闭 raw；若配置为开启，`validate-config` 会直接报错。生产采集链路已将 ARMS/SLS 响应转换为标准事件，经流式组批器原子写入 `outbox/pending`，然后才推进任务级 `state/collection-watermarks.json`。该路径不创建 raw、manifest 或页面 checkpoint。启用上传后，独立上传循环只执行“pending/到期 retry → `BatchUploader` → 回执处理 → 上传水位 → 成功删除”，不读取 raw、manifest 或页面 checkpoint。

采集循环和上传循环拥有独立线程及异常边界：`cc-ingest` 暂时不可用时采集继续产生 Outbox 批次，ARMS/SLS 查询失败时已有批次仍可上传。队列为空时上传循环等待 Outbox 变更；未来 retry 按最早 `next_retry_at` 唤醒。401/403 会暂停上传但不会停止采集，凭据修复后可通过应用内 `resumeUploadAfterAuthenticationRepair()` 恢复，或重启进程重新加载 pending/retry。生产模式启用上传时强制启用 mTLS，不存在 trust-all 或关闭主机名校验的配置入口。

### 6.3 ARMS Dataset 模式

复制 Dataset 模板：

```powershell
Copy-Item config/arms-dataset.example.yaml config/site.yaml
```

Dataset 模式下：

- `arms.mode` 设置为 `DATASET`。
- `_userId` 自动取 `usernameEnv` 指定的环境变量。
- 只有现场网关要求 HTTP Basic 时，才设置密码环境变量。
- `sls.enabled` 可以保持 `false`，也可以补充 SLS 配置后同时采集。

### 6.4 标准事件字段准入

每个 ARMS 查询任务和 SLS Logstore 都可以配置独立的标准化策略：

```yaml
normalization:
  payloadFields: ["message", "method", "path", "status", "trace_id"]
  identityFields: ["request_id"]
```

- `payloadFields` 只允许列出的顶层字段进入 `payload`；为空时使用对应转换器的保守内置字段集合。
- `identityFields` 用于指定现场稳定业务 ID；找不到时转换器使用事件时间与规范 payload 生成稳定 `event_id`。
- 未准入字段不会上传，只产生不含字段值的结构化告警。
- SLS 字段由现场定义，生产部署前应根据数据分级和研究需要显式调整准入清单。

完整别名、时间、稳定 ID、告警和安全规则见 [标准事件字段映射说明](docs/标准事件字段映射说明.md)。生产 daemon 已将转换器和组批器接入 Outbox；修改这些字段会影响随后生成的 pending 批次，但不会自动把历史 raw 文件上传。

### 6.5 配置 cc-ingest mTLS

生产模式启用上传时必须同时启用 mTLS，并把 `cc-ingest` 地址配置为证书 SAN 能够匹配的 HTTPS 地址：

```yaml
runtime:
  deploymentMode: "PRODUCTION"
upload:
  enabled: true
  ingestUrl: "https://cc-ingest.cc-system.svc.cluster.local/collector/v1/events:batch"
security:
  mtlsEnabled: true
  clientAuthMode: "KEYSTORE"
  keyStorePath: "/var/run/secrets/collector-tls/client.p12"
  keyStoreType: "PKCS12"
  keyStorePasswordEnv: "COLLECTOR_KEYSTORE_PASSWORD"
  trustStorePath: "/var/run/secrets/collector-tls/trust.p12"
  trustStoreType: "PKCS12"
  trustStorePasswordEnv: "COLLECTOR_TRUSTSTORE_PASSWORD"
  certificateExpiryWarningDays: 30
  certificateReloadIntervalSeconds: 30
```

PEM 模式将 `clientAuthMode` 改为 `PEM`，设置 `clientCertificatePath` 和 `privateKeyPath`，并清空 KeyStore 字段。私钥必须为未加密 PKCS#8 `PRIVATE KEY` 格式；TrustStore 仍使用 PKCS12 或 JKS。证书、私钥和 TrustStore 应通过 Kubernetes Secret Volume 以只读方式挂载，KeyStore/TrustStore 密码通过 Secret 环境变量注入，不能写入 YAML、镜像或 Git。

客户端启动时检查证书有效期、clientAuth 用途和证书/私钥匹配关系。运行中每隔 `certificateReloadIntervalSeconds` 检查 Secret 文件内容；发现更新后先完整加载并校验候选材料，再原子切换后续 HTTPS 请求。候选材料损坏时继续使用上一份仍有效证书，不修改或清空 Outbox；上一份证书也已失效时上传失败关闭，批次继续保留。距离到期不足 `certificateExpiryWarningDays` 时产生告警。

TLS 使用配置的 TrustStore 验证服务端证书链，并显式启用 HTTPS 主机名校验。日志只记录客户端证书 SHA-256 指纹、`CN` 主体摘要和到期时间，不输出证书正文、私钥、密码或 Secret 文件路径。`collector_id` 与客户端证书身份的绑定仍须由 `cc-ingest` 服务端校验，不能由采集端自行证明。

## 7. 配置凭据环境变量

程序不会从 YAML 读取 AccessKey 实际值。必须在启动 Java 的同一个 PowerShell 会话中设置：

```powershell
$env:ARMS_ACCESS_KEY_ID='<实际ARMS AccessKey ID>'
$env:ARMS_ACCESS_KEY_SECRET='<实际ARMS AccessKey Secret>'

$env:SLS_ACCESS_KEY_ID='<实际SLS AccessKey ID>'
$env:SLS_ACCESS_KEY_SECRET='<实际SLS AccessKey Secret>'
```

使用 SLS STS 临时凭据时，再设置：

```powershell
$env:SLS_STS_TOKEN='<实际Security Token>'
```

Dataset 模式使用：

```powershell
$env:ARMS_DATASET_USERNAME='<实际用户名>'
$env:ARMS_DATASET_PASSWORD='<实际密码>'
```

如果现场 Dataset 不需要密码，可以不设置 `ARMS_DATASET_PASSWORD`。

启用 KeyStore mTLS 时还需要设置密码环境变量；变量名必须与 `security` 配置一致。在 Kubernetes 中应使用 Secret `secretKeyRef` 注入，不要把实际密码写进 ConfigMap：

```powershell
$env:COLLECTOR_KEYSTORE_PASSWORD='<实际KeyStore密码>'
$env:COLLECTOR_TRUSTSTORE_PASSWORD='<实际TrustStore密码>'
```

PEM 模式不读取私钥密码，私钥文件必须是未加密 PKCS#8；TrustStore 密码环境变量仍然必需。

检查变量是否存在，不要输出 Secret 内容：

```powershell
Test-Path Env:ARMS_ACCESS_KEY_ID
Test-Path Env:ARMS_ACCESS_KEY_SECRET
Test-Path Env:SLS_ACCESS_KEY_ID
Test-Path Env:SLS_ACCESS_KEY_SECRET
Test-Path Env:COLLECTOR_KEYSTORE_PASSWORD
Test-Path Env:COLLECTOR_TRUSTSTORE_PASSWORD
```

这些变量默认只在当前 PowerShell 会话有效。关闭窗口后需要重新设置。

如果 ARMS 和 SLS 使用同一组 AccessKey，可以给四个变量设置相同值，也可以在 YAML 中让 ARMS 和 SLS 引用相同的环境变量名。

## 8. 时间参数说明

`dry-run`、`collect` 和 `resume` 需要同时指定 `--start`、`--end`。`daemon` 不接受这两个参数，它根据调度配置和持久化水位自动计算连续时间范围。

格式必须是包含时区的 ISO-8601 整秒时间，例如：

```text
2026-07-27T09:00:00+08:00
2026-07-27T01:00:00Z
```

以下格式不建议或不支持：

```text
2026-07-27 09:00:00       # 没有时区
2026-07-27T09:00:00.123Z  # 包含毫秒，SLS 为秒级查询
```

程序把时间范围理解为逻辑上的 `[start, end)`，即包含开始时间、不包含结束时间。调用使用闭区间的 API 时，会从结束时间减去 1 毫秒或 1 秒，减少相邻窗口边界重复。

## 9. 命令总览

| 命令 | 是否访问 API | 是否要求凭据 | 是否写原始数据 | 主要用途 |
|---|---:|---:|---:|---|
| `validate-config` | 否 | 否 | 否 | 检查 YAML 是否完整、字段是否合法 |
| `probe` | 是 | 是 | 否 | 验证 Endpoint、凭据和基本只读权限 |
| `discover` | 是 | 是 | 否 | 查询 ARMS 应用和 SLS Logstore，辅助回填配置 |
| `dry-run` | 否 | 否 | 否 | 查看时间分窗、任务和输出目录 |
| `collect` | 是 | 是 | 开发模式写 raw；生产模式写 pending | 执行离线时间段采集 |
| `resume` | 是 | 是 | 开发模式使用 checkpoint；生产模式重查并幂等写 pending | 继续开发离线采集，或重建生产范围内的稳定批次 |
| `daemon` | 是 | 是 | 开发模式写 raw；生产模式写 pending | 常驻增量轮询，并在启用 upload 时主动上传 pending/retry |
| `validate-output` | 否 | 否 | 否 | 校验 manifest、文件大小、SHA-256 和 `.partial` 文件 |
| `build-manifest` | 否 | 否 | 只写辅助清单 | 原 manifest 丢失时，从 raw 文件重建辅助 manifest |

所有命令都需要 `--config`。只有 `dry-run`、`collect` 和 `resume` 需要 `--start` 和 `--end`。

## 10. 各命令含义与用法

### 10.1 `validate-config`：校验配置

```powershell
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar `
  validate-config --config config/site.yaml
```

它会检查：

- 必填字段是否为空或仍是 `__REQUIRED_ON_SITE__`。
- 项目、环境、批次、任务名和记录类型是否适合用于文件名。
- ARMS 模式和指标 Action 是否受支持。
- 采集端身份、页大小、重试次数、轮询和补偿时间是否合理。
- 上传 URL、批量记录数、压缩大小、请求大小、并发数和超时是否合法。
- PEM/KeyStore 是否互斥，证书、私钥、信任库、密码环境变量名、到期告警天数和重载周期是否完整合法；生产上传是否已启用 mTLS。
- 运行模式、数据根目录、Outbox/状态/raw 相对路径、raw 开关和告警/临界磁盘水位是否合法。
- 每个采集任务的 payload 准入字段和稳定身份字段是否为空、重复或超过数量限制。
- 控制功能启用时，任务领取 URL 和心跳、轮询间隔是否完整。
- 是否至少启用了 ARMS 或 SLS。

该命令不访问 API，也不要求 AccessKey 环境变量存在。建议每次修改 YAML 后先执行一次。

### 10.2 `probe`：最小权限与连通性探测

```powershell
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar `
  probe --config config/site.yaml
```

RPC 模式下，ARMS 调用 `ListTraceApps`；SLS 调用 `ListLogStores`。Dataset 模式会使用第一个 Dataset 任务查询最近 60 秒，只验证接口能否工作，不保存原始采集文件。

如果失败，优先检查：

- Endpoint、协议和 RegionId。
- AccessKey 是否在当前 PowerShell 中设置。
- AccessKey 是否具有只读 API 权限。
- 现场网络、DNS、防火墙或代理。
- ARMS API Version 是否正确。

### 10.3 `discover`：发现现场资源

```powershell
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar `
  discover --config config/site.yaml
```

输出包括：

- ARMS 可见应用及 PID。
- 配置中各 SLS Project 下可见的 Logstore。

根据输出回填或修正 `pid`、`project`、`logstore`。Dataset 模式没有统一的应用发现接口，需要按甲方文档填写 Dataset ID。

### 10.4 `dry-run`：预演采集计划

```powershell
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar `
  dry-run --config config/site.yaml `
  --start 2026-07-27T09:00:00+08:00 `
  --end 2026-07-27T10:00:00+08:00
```

该命令展示：

- 采集开始和结束时间。
- `windowSeconds` 和分窗数量。
- ARMS 链路、指标或 Dataset 任务。
- SLS Project/Logstore。
- 最终输出目录。

它不访问 API、不写采集数据，适合在正式采集前确认范围和任务数量。

### 10.5 `collect`：执行采集

```powershell
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar `
  collect --config config/site.yaml `
  --start 2026-07-27T09:00:00+08:00 `
  --end 2026-07-27T10:00:00+08:00
```

执行过程：

1. 按 `windowSeconds` 拆分时间范围。
2. 每个窗口先执行 ARMS，再执行 SLS。
3. ARMS 链路按页搜索，并按 TraceID 查询详情。
4. ARMS 指标或 Dataset 按任务查询。
5. SLS 按 Project、Logstore、offset 分页查询。
6. 暂时性网络错误、408、429、5xx 等按配置重试。
7. 开发模式下，每一页成功后立即写入 raw 文件。
8. 开发模式更新 manifest 和 checkpoint；生产模式将标准事件流式组批，原子写入 Outbox pending，当前任务窗口全部成功后才更新任务级采集水位。

程序当前按窗口和采集源串行执行，以降低现场接口压力。命令结束并显示“采集完成”后，再执行输出校验。

### 10.6 `resume`：断点续采

中断、断网或修复权限后，使用与原采集完全相同的配置、开始时间和结束时间：

```powershell
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar `
  resume --config config/site.yaml `
  --start 2026-07-27T09:00:00+08:00 `
  --end 2026-07-27T10:00:00+08:00
```

开发 raw 模式下，程序读取 `checkpoints/checkpoint.json`，跳过已完整完成的任务窗口和已保存的 Trace 详情。尚未完成的 Trace/SLS 窗口从第一页重新查询和保存，以重建总数、重复页与终止条件的核对依据；不能仅凭旧页面 checkpoint 跳过核对。可能产生重复 raw 分片，但不覆盖已有文件。

生产模式关闭 raw 时不使用页面级 checkpoint；`resume` 会按给定时间范围重新查询，并依靠稳定 `event_id`、`poll_id`、`batch_id` 与 Outbox 幂等写入恢复。只有每个任务窗口的全部批次已进入 pending，才会推进其任务级采集水位。

如果程序在“文件已经落盘、checkpoint 尚未写入”之间中断，恢复时可能再次采集该页，但会使用新的分片号，不会覆盖已有原始文件。这种设计优先保证原始数据不丢失，后续可按时间、Request ID 或内容哈希去重。

不要删除 checkpoint 后直接重跑同一批次，除非明确希望重新采集。

### 10.7 `daemon`：常驻轮询

```powershell
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar `
  daemon --config config/site.yaml
```

常驻模式的时间窗计算规则如下：

- ARMS 默认每 30 秒执行一轮，查询终点为 `now - 90 秒`。
- SLS 默认每 20 秒执行一轮，查询终点为 `now - 30 秒`。
- 开发模式使用过渡性 `state/poll-watermarks.json`，按 ARMS/SLS 整个数据源锚定首次起点和推进水位，仍以 raw 完整落盘为条件。
- 生产模式使用 `state/collection-watermarks.json`，分别保存 `ARMS/TRACE/<任务>`、`ARMS/METRIC/<任务>`、`ARMS/DATASET/<任务>` 和 `SLS/LOG/<Project>/<Logstore>` 的首次起点与成功窗口上界。
- 生产任务只有在该窗口全部分页成功、所有标准事件完成组批且所有批次已原子进入 `pending` 后，才推进到 `window_end`；成功的空窗口也会推进。页面级 checkpoint 不参与生产路径。
- 单轮失败只记录错误，不退出进程，也不推进水位。
- 系统时间回拨导致查询终点未超过水位时，本轮等待，不倒退水位。
- 每轮带有 `jitterSeconds` 随机抖动，降低多个任务或实例同时请求云平台的概率。

停止进程时，调度器先停止创建新轮询，再等待正在执行的采集结束，最长等待 `shutdownGraceSeconds`。生产 daemon 会独立运行采集、容量监控、可选上传、控制和节点状态循环；采集或容量暂停不会停止上传，控制/状态/上传故障也不会终止采集进程。当前阶段尚未实现最近 30 分钟补偿查询和长周期对账。

### 10.8 `validate-output`：校验输出

```powershell
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar `
  validate-output --config config/site.yaml
```

它会检查：

- `manifest/manifest.json` 是否存在。
- manifest 中每个文件是否存在。
- 文件大小是否与 manifest 一致。
- SHA-256 是否与 manifest 一致。
- 是否残留 `.partial` 未完成文件。
- manifest 中是否存在重复路径或越界路径。

显示“输出校验通过”后，才能认为本批次文件在本地保存完整。

### 10.9 `build-manifest`：重建辅助清单

```powershell
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar `
  build-manifest --config config/site.yaml
```

仅在开启 raw 的开发模式中使用。原 manifest 丢失或损坏时，它会扫描 `storage.rawPersistence.directory` 指定的目录并生成：

```text
manifest/manifest-rebuilt.json
```

从文件名和文件内容可以恢复路径、时间窗、分片号、大小和 SHA-256，但无法可靠恢复的记录数、Request ID 会留空或记为 `-1`。该文件是辅助恢复结果，不自动覆盖正式 `manifest.json`。

## 11. 推荐的完整现场流程

### 阶段一：到达现场后确认环境

1. 确认 Java 能运行。
2. 确认采集机器能够访问 ARMS/SLS Endpoint。
3. 在控制台开通只读 API 权限。
4. 确认 SLS Logstore 已建立所需索引。
5. 确认 API Version、RegionId、PID、Project 和 Logstore。

### 阶段二：填写配置和凭据

1. 复制配置模板为 `config/site.yaml`。
2. 填写所有现场参数。
3. 为本批次设置独立 `batchId` 和 `storage.dataRoot`。
4. 在当前 PowerShell 设置 AccessKey 环境变量。
5. 执行 `validate-config`。

### 阶段三：探测与回填

按顺序执行：

```powershell
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar validate-config --config config/site.yaml
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar probe --config config/site.yaml
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar discover --config config/site.yaml
```

根据 `discover` 输出修正 PID、Project 和 Logstore，再次执行 `validate-config`。

### 阶段四：开发模式预演和短时间试采

先预演 10 分钟：

```powershell
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar `
  dry-run --config config/site.yaml `
  --start 2026-07-27T09:00:00+08:00 `
  --end 2026-07-27T09:10:00+08:00
```

确认无误后试采相同的短时间范围：

```powershell
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar `
  collect --config config/site.yaml `
  --start 2026-07-27T09:00:00+08:00 `
  --end 2026-07-27T09:10:00+08:00
```

使用 `DEVELOPMENT` 模式且开启 raw 后，检查：

- `raw/arms`、`raw/sls` 是否生成文件。
- JSON 内容是否为目标应用和目标环境。
- 文件名中的项目、环境、批次、记录类型和时间是否正确。
- `manifest.json` 是否记录文件、SHA-256 和 Request ID。
- `collector.log` 是否存在错误。
- 执行 `validate-output` 是否通过。

建议短时间试采使用独立批次或独立 `storage.dataRoot`。确认后再创建正式批次，避免试采数据混入正式结果。

### 阶段五：开发或现场验证采集

1. 确认正式 `batchId`、`caseId` 和 `storage.dataRoot`。
2. 使用 `dry-run` 核对完整时间范围和窗口数。
3. 执行 `collect`。
4. 保持终端窗口和网络连接，不要关闭 PowerShell。
5. 观察控制台输出和 `logs/collector.log`。
6. 采集完成后执行 `validate-output`。
7. 备份整个输出目录，而不是只复制 `raw`。

### 阶段六：开发模式发生中断时

1. 不要删除已经生成的 raw、manifest 和 checkpoint。
2. 根据错误信息修复网络、权限或配置。
3. 确认仍使用原来的 `storage.dataRoot`、批次和时间范围。
4. 执行 `resume`。
5. 完成后再次执行 `validate-output`。

## 12. 输出目录与 PVC 设计

开发和现场排障模式可以保留 raw、manifest、checkpoint 和日志，便于核对接口响应与恢复离线采集：

```text
data-package/batch001/
├─ raw/
│  ├─ arms/                 ARMS 原始 JSON 响应
│  └─ sls/                  SLS 查询信封和日志键值内容
├─ checkpoints/
│  └─ checkpoint.json       离线采集已完成的请求和任务窗口
├─ state/
│  └─ poll-watermarks.json        开发 daemon 各数据源 raw 完整落盘的时间上界
├─ manifest/
│  ├─ manifest.json         正式文件清单
│  └─ manifest-rebuilt.json 可选的辅助重建清单
└─ logs/
   └─ collector.log         运行过程和错误日志
```

开发模式交付或备份时，建议保留整个批次目录，包括 raw、checkpoint、state、manifest 和 logs。

P0-06 已实现生产 PVC 的 Outbox 基础目录。生产 PVC 只保存“尚未被中控确认的数据”和恢复所需水位，不保存 raw、manifest、committed、归档目录或长期日志：

```text
data/
├─ outbox/
│  ├─ writing/              正在原子生成的批次
│  ├─ pending/              等待提交或等待确认的批次
│  └─ retry/                可重试失败的批次
└─ state/
   ├─ collection-watermarks.json  ARMS/SLS 采集水位
   ├─ upload-watermarks.json      中控已确认的提交水位
   ├─ capacity-state.json         PVC 物理容量保护状态（保留原 1.0 Schema）
   ├─ outbox-quota-state.json     Outbox 逻辑配额滞回状态
   ├─ runtime-policy.json         最后一次成功生效的远程配额策略
   ├─ manual-pause-state.json     全局及 ARMS/SLS 人工暂停策略
   └─ control-jobs.json           已领取任务摘要、最新状态和配置版本
```

每个批次使用 `batch-<SHA-256(batch_id)>` 作为安全目录名，目录中包含三类文件：

```text
pending/batch-<hash>/
├─ batch.json               提交给 cc-ingest 的规范化、不可变 UTF-8 JSON
├─ outbox.json              不可变批次描述和 batch.json 字节哈希
└─ delivery.json            可变状态、尝试次数、错误分类和下次重试时间
```

写入流程是：先在 `writing` 创建以 `.partial` 结尾的批次目录，分别强制写入三个完整文件，再将整个目录通过同文件系统原子移动发布到 `pending`。`BatchUploader` 只能认领 `pending` 中的 `PENDING` 批次和 `retry` 中已经到期的批次；同一进程内的多个工作线程不会认领同一批次，实际并发不超过 `upload.maxInFlightBatches`。不可重试错误只把 `delivery.json` 标记为 `BLOCKED`，批次仍留在 `pending`。

`content_sha256` 是规范化事件数组的哈希；`outbox.json` 另外保存 `batch.json` 精确序列化字节的 SHA-256。后续上传器必须读取同一份不可变批次内容再进行 gzip，重试不得改变 `batch_id`、事件内容或这两个哈希。

P0-07 的 `BatchBuilder` 使用和上传客户端完全相同的 UTF-8 JSON 与 gzip 编码计算大小。它最多缓存一个待封装批次，按 `upload.maxRecords`、`upload.maxRequestBytes`、`upload.maxCompressedBytes` 和 `upload.maxWaitMillis` 切分；查询窗口结束时由 `finish()` 写出 `is_last_batch=true` 的尾批。同一 `poll_id` 内序号从 0 递增，`batch_id` 由采集上下文、序号和规范化事件内容稳定计算，重试或重启重建不会改变。生产采集路径会把每个已构建批次原子写入 Outbox pending。

若中控返回 413，上传器调用组批器按原事件顺序二分当前批次，并为子批生成确定性 ID。全部子批原子进入 Outbox 后，父批才切换为 `SPLIT`；父批保存完整 child ID 列表，子批保存 `parent_batch_id`。子批逐个可靠提交并在父批中记录确认，所有子批完成后才以逻辑父批推进上传水位并删除父批确认信息。重启后会复用相同子批 ID，不会重新生成不同身份。

启动恢复会校验 pending/retry 的文件集、Schema、业务字段和双重哈希。writing 中三个文件完整的批次会恢复到对应稳定目录，明确不完整的 `.partial` 批次会删除，并由尚未推进的采集水位重新查询。已经发布但校验失败的 pending/retry 批次不会自动删除，Outbox 会失败关闭并保留现场供人工处理。

生产语义中，查询窗的全部标准批次原子进入 `pending` 后才推进 `collection-watermarks.json`；该文件按任务保存，而不是按整个 ARMS/SLS 数据源保存。`PollingWatermarkStore` 仍只用于开发 raw 模式的过渡性 `poll-watermarks.json`，不参与生产完成条件。

P0-09 已提供独立 `UploadWatermarkStore`，默认文件为 `state/upload-watermarks.json`。每个 `poll_id` 保存 `schema_version`、`poll_id`、`continuous_sequence`、`sparse_acks`、`last_batch_sequence`、`completed` 和 `updated_at`。序号从 0 开始，因此 `continuous_sequence=-1` 表示尚未形成连续 ACK。乱序 ACK 先进入 `sparse_acks`，缺口补齐后自动折叠；末批可由 `is_last_batch` 或可选 `batch_count` 推导。未完成 poll 暂存最小 `ack_evidence`，用于识别同序号异批次或异哈希；poll 完成后清空逐批证据，只保留压缩边界。

`BatchUploader` 只有在 `IngestClient` 确认 HTTP 200、状态为 `COMMITTED`/`ALREADY_COMMITTED`，且 Schema、`batch_id`、`content_sha256`、`committed_at` 全部匹配后，才调用 `UploadWatermarkStore.acknowledge`。状态更新使用同目录 `.partial`、`FileChannel.force(true)` 和同文件系统原子替换；水位成功后批次先原子移入 writing 下的瞬态 `.deleting` 目录再删除，重启会幂等清理该残留。水位写入失败或任何不确定回执都会保留 Outbox 数据。

可重试失败在 `delivery.json` 保存 `attempt_count`、`last_error` 和 `next_retry_at`。429 使用整数秒 `Retry-After`；408、500/502/503、HTTP 超时、断连和响应丢失使用原 `batch_id` 按带抖动指数退避重试。达到 `retryWarningAttempts` 只产生有限字段告警，不删除或停止可重试批次。400/422 和 409 分别以 VALIDATION/CONFLICT 标记 `BLOCKED`；401/403 暂停上传器并产生 HIGH 状态，但不改写批次。

`daemon` 启动时复用采集管道的同一个 `OutboxStore`，确保进程内认领互斥。上传循环每轮最多认领 `upload.maxInFlightBatches` 个批次；单批 `BLOCKED` 会被后续扫描跳过，不妨碍其他 pending/retry。启动时先恢复容量、配额、人工暂停和任务状态，再独立启动上传、控制、采集和状态循环。关闭时先停止状态上报和控制领取，再停止新采集、上传及容量监控；控制或状态服务不可达不会传播为其他循环的退出条件。

生产模式每隔 `diskCheckIntervalSeconds` 检查 Outbox 所在 PVC 文件系统的总字节和可用字节，并使用以下状态机：

- `NORMAL`：低于告警水位，正常采集和上传。
- `WARNING`：达到 `diskWarningWatermarkPercent`，记录告警但继续采集和上传；下降到 resume 水位前保持 WARNING。
- `CRITICAL`：达到 `diskCriticalWatermarkPercent`，或容量检测失败时失败关闭；新的 ARMS/SLS 查询窗口不再启动，采集水位不推进，上传循环继续消费已有 pending/retry。
- `RECOVERING`：从 CRITICAL 下降到 `diskResumeWatermarkPercent` 后进入并恢复采集；下一次检查仍处于恢复水位时回到 NORMAL，重新升高时按 WARNING/CRITICAL 规则处理。

必须满足 `diskResumeWatermarkPercent < diskWarningWatermarkPercent < diskCriticalWatermarkPercent`。状态首次检查和状态切换会原子保存到 `state/capacity-state.json`，Pod 重启后先恢复原状态再同步检查当前容量。容量保护从不删除 pending、retry 或 BLOCKED 批次；只有上传器获得可靠提交回执并更新上传水位后才允许删除。开启 raw 的开发模式不启动生产 PVC 容量监控，开发 raw 数据不参与该状态机。

Outbox 逻辑配额由 `storage.outboxQuota` 独立配置。首次应用远程策略前使用 `defaultBytes`；实际可用上限同时受本地 `maxBytes` 和 `PVC 总容量 - reservedFreeBytes` 约束，并且不得低于 `minBytes`。远程请求低于最小值、高于最大值或超过 PVC 安全上限时由 `applyRequestedQuota` 明确拒绝并保留旧策略；成功策略原子写入 `state/runtime-policy.json`，重启后恢复。策略文件或临时文件损坏会失败关闭，不会回退到更宽松的 YAML 默认值。

逻辑使用量只统计 `outbox/writing`、`pending`、`retry` 下的批次内容、描述文件和 `delivery.json`，因此 BLOCKED 批次也计入；raw、manifest、普通日志、采集/上传水位和容量状态不计入。启动时执行全量扫描，Outbox 创建、投递元数据替换和可靠提交后的删除使用精确差量维护，并按 `checkIntervalSeconds` 周期全量校正。新批次进入 writing 前会原子预留空间；预留失败会终止当前窗口且不推进采集水位，已有 pending/retry/BLOCKED 保持不变。上传需要的 retry/BLOCKED 元数据、413 子批处理和可靠提交后的空间释放不受采集 CRITICAL 门禁阻断。

逻辑配额使用 `resumePercent < warningPercent < criticalPercent` 的独立滞回状态机。配额降低到当前使用量以下会立即进入 CRITICAL；只有上传释放空间至 resume 水位后才恢复。生产采集在调度入口及每个实际查询窗口开始前同时检查物理与逻辑状态，任一 CRITICAL 都会暂停新查询，而提高逻辑配额不能绕过物理 PVC CRITICAL。逻辑配额只限制采集端 Outbox，不会创建、扩容或缩容 Kubernetes PVC，也没有自动删除未提交数据的路径。

当前版本已实现 P0-03 标准事件转换、P0-04 任务级采集水位、P0-06 文件式 Outbox、P0-07 流式确定性组批、P0-08 独立上传器、P0-09 上传水位联动、P0-10 的生产采集与 daemon 上传线程串联、P0-11 采集端 mTLS、P1-01 首批控制闭环、P1-02 节点状态主动上报，以及 P1-03 的本地 Outbox 逻辑配额和 PVC 双重容量门控。P1-00 已冻结控制、状态和远程容量策略协议；尚未实现 P1-01 第二阶段补采/指定批次重传/非敏感配置重载、P1-04 Kubernetes liveness/readiness 部署接入，也尚未完成中控端证书身份绑定、inode/文件数保护及长时间故障恢复验收。

## 13. 文件命名

文件名严格沿用现有规范：

```text
{project_code}_{env_code}_{batch_id}_{collector_id}_{record_type}_{start_utc}_{end_utc}_p{part}.json
```

示例：

```text
kjky20250400_test01_batch001_arms_topology_edge_20260727T010000Z_20260727T011000Z_p0001.json
```

说明：

- 文件名中的时间统一转换为 UTC。
- `collector_id` 当前为 `arms` 或 `sls`。
- `record_type` 完全来自 YAML，不在程序中增加新的命名规范。
- `p0001` 是分片号，同一采集源、记录类型和时间窗内依次增加。
- 恢复采集时，分片号从 manifest 中已有最大值继续，不覆盖旧文件。

## 14. ARMS 与 SLS 输出差异

### ARMS

ARMS RPC/Dataset 返回内容按响应原样保存。链路搜索响应、Trace 详情和指标响应均会落盘。

### SLS

SLS SDK 会先解码响应，程序再包装为 JSON 信封，其中包含：

- Project、Logstore。
- 查询开始和结束时间。
- Topic、Query、offset、line、reverse。
- 查询是否完成。
- Request ID、扫描字节数和耗时。
- 原始查询结果和日志列表。

每条日志的字段使用有序 `contents` 数组保存，可以保留重复键。

## 15. 重试、停止和恢复规则

ARMS RPC、ARMS Dataset 和 SLS 查询统一输出以下四类结果，但各自使用独立 `RetryExecutor`；查询侧不会复用 `UploadRetryPolicy`，也不会与 Ingest、Control 或 Status 共享退避状态：

| 分类 | 典型错误 | 当前调用内处理 |
| --- | --- | --- |
| `RETRYABLE` | 网络连接/重置、请求超时、HTTP 408、500/502/503/504、SDK 明确的临时服务端错误 | 按有上限的抖动指数退避重试 |
| `RATE_LIMITED` | HTTP 429、SDK Throttling/RateLimit/FlowControl | 优先使用服务端 `Retry-After`，否则使用本地退避 |
| `AUTHENTICATION` | HTTP 401/403、无效 AccessKey/签名/Token、明确权限不足 | 不在当前窗口高频重试，立即失败并等待凭据或授权修复 |
| `NON_RETRYABLE` | HTTP 400/404、非法参数、配置错误、资源不存在、Schema/响应结构错误 | 立即失败 |

第 `n` 次失败后的本地基础等待为：

```text
B(n) = min(retryMaxDelayMillis, retryBaseMillis * 2^(n-1))
D(n) = min(retryMaxDelayMillis, round(B(n) * (1 - j + 2*j*U)))
```

其中 `j=retryJitterRatio`，`U` 为 `[0,1]` 的独立随机数。服务端给出整数秒或 RFC 1123 日期格式的 `Retry-After` 时，等待时间优先使用该建议并限制在 `retryMaxDelayMillis` 内。随机抖动会使同时恢复的任务分散请求；退避上限避免长时间故障时无限增长。`retryBaseMillis` 必须大于 0，`retryMaxDelayMillis` 必须不小于基础等待且最多为 3600000 ms。

日志只记录外部系统、任务、尝试次数、分类、安全错误码和下次等待时间；不记录 AccessKey、Token、密码、完整响应正文或原始事件。达到 `maxRetries + 1` 次总尝试后，当前窗口失败且不推进采集水位；daemon 仍保持运行，下一调度周期可从原水位重新尝试。

一次性 `collect/resume` 失败时程序会：

1. 把错误写入 manifest 的 `errors`。
2. 把错误写入 `logs/collector.log`。
3. 保留已经成功落盘的数据。
4. 以非零状态码退出。

修复问题后使用 `resume`，不需要从头开始。

`daemon` 模式中，单轮失败不会结束进程。该轮水位保持不变，调度器在下一周期继续尝试；正常停止时则先停止创建新轮询，并等待正在进行的落盘操作安全结束。

### 15.1 分页完整性与失败关闭（P1-06）

所有限制作用于单个任务的一个固定查询窗口，而非上传批次：

```yaml
pagination:
  maxPages: 1000
  maxRecords: 1000000
```

`maxPages` 允许 1～100000，`maxRecords` 允许 1～100000000。若达到上限时仍需继续查询，任务失败；只有最后一页已提供完整结束证据时，才能恰好在上限处成功。不要仅靠调大限制处理异常，应先核对协议或缩短查询窗口。

- **Trace**：优先使用返回总数、页码、`hasMore` 等证据；没有这些元数据时，兼容“返回条数小于请求 `pageSize`”结束。刚好满页且无明确终止证据时继续请求，最终空页可以合法结束。总数变化、累计数量超过总数、终止标志互相矛盾、返回页码不匹配或服务端返回的页大小与请求不符时失败。当前只发送已使用的 `PageNumber`/`PageSize`；出现非空游标而无法使用页码策略解释时失败，不猜测新的请求字段。
- **QueryMetric**：仍为单次查询，但到达 `limit` 且无明确结束证据、或响应显示还有数据时失败，建议缩小时间窗。`QueryMetricByPage` 当前一律禁止，不能通过改 action 绕过此保护。
- **Dataset**：默认 `SINGLE_RESPONSE`，必须由总数等于实际条数、明确 `hasMore=false`、`truncated=false` 等响应证据证明结束。无证据的数组（包括单纯空数组）不能证明完整，会报 `PAGINATION_COMPLETENESS_UNKNOWN`。明确完整的空窗口仍正常推进至 `window_end`。
- **SLS**：窗口开始时复制 Project/Logstore、时间、Topic、Query、pageSize、reverse 等参数，整个分页过程保持不变。offset 按返回数量递增；重复页、空页但仍有更多数据、总数异常和分页上限均失败关闭。SDK 的 `completed` 仅表示当前查询执行完成，不代表整窗数据已读完。

页内容摘要与 Request ID 分离；即使 Request ID 不同、对象字段顺序或整页记录顺序变化，相同记录组成的重复页也会被拒绝。这样可能保守拒绝完全相同的合法日志页，需通过现场稳定记录标识、查询粒度或后续协议适配解决，不能直接关闭保护来推进水位。

#### 现场响应字段适配

ARMS 每个 Trace/Metric/Dataset 任务均可配置 `pagination.totalPointer`、`positionPointer`、`hasMorePointer`、`truncatedPointer`、`nextCursorPointer`。这些是 **JSON Pointer**，必须按现场实际文档填写；显式配置的字段缺失或类型错误时失败，不静默回退。总数和位置须为非负 JSON 整数，状态为 JSON 布尔值。`positionPointer` 当前对应从 1 开始的页码；不同语义需要额外适配。

未配置路径时，仅保守检查根对象及 `PageBean`、`Data`、`data`、`pagination` 信封中的常见标记：`TotalCount/totalCount/Total/total`、`PageNumber/pageNumber/Page/page`、`HasMore/hasMore/has_more`、`Truncated/truncated/IsTruncated/isTruncated`、`NextToken/nextToken/NextCursor/nextCursor`。这些只是矛盾/截断防护，不代表已经核实现场字段；不会扫描业务记录并将其字段当作分页协议。自定义路径和已识别信封标记互相矛盾时也失败。

Dataset 提供可配置的 `CURSOR` 策略。只有拿到现场文档后，才能设置 `strategy: CURSOR`，并明确 `nextCursorPointer`（响应游标 JSON Pointer）与 `cursorParameter`（下一页表单参数名）。首请求不携带游标，后续仅回传该字段；响应游标必须每页存在，非空字符串表示继续，空字符串或 null 表示结束。重复游标、缺少游标、空页但有后续游标、游标与总数/截断标志矛盾均失败。未配置策略时即使看到非空游标，也不会猜测参数名继续请求。上述策略只经过模拟协议测试，尚未经过现场 API 验证。

`parameters` 禁止覆盖 Trace 的 `StartTime/EndTime/PageNumber/PageSize`、Metric 的 `StartTime/EndTime/Limit`、Dataset 的 `minTime/maxTime`；Dataset 游标参数也不能预设或覆盖业务/身份参数。其他已支持的非保护参数仍保持原有透传规则。

SLS 内部 `QueryResult` 可由经过验证的适配器补充 `total/offset/hasMore`，但当前 `SlsSdkApi` 没有已核实的整窗总数和 offset 回显，保留为未知，使用短页结束规则；不得把本页日志数量冒充整窗总数。

#### 水位、日志和已知限制

任一页失败、触发保护或无法判定完整时，都不会调用该任务的 `finish()`，也不会推进任务采集水位；之前已进入 pending 的批次保留。后续重查依靠既有稳定事件/批次身份处理重复，不改变标准事件、cc-ingest 或中控控制协议。

每个实际执行的任务窗口输出一条 `QUERY_SUMMARY` JSON，包含 `source_key`、`window_start/window_end`、`page_count`、`record_count`、`request_id_sha256_sample` 和 `complete`。页数/条数统计已解析的查询结果（包括触发核对失败的页），Trace 的条数统计搜索记录而不重复累计详情 Span；Request ID 只保存最多 16 个 SHA-256 样本，详情请求也参与摘要。失败时 `complete=false`；只有分页核对、标准化、Outbox/水位完成后才为 true（开发 raw 模式为输出及 checkpoint 完成）。摘要不含响应正文、查询表达式、游标或凭据。采集调用的 SDK 异常与 JSON 解析异常不会附带原始消息/异常 cause 对外传播。

固定 SLS 时间窗口不是服务端快照，窗口内迟到日志、删除、排序变化仍可能影响 offset。现有模拟测试验证参数冻结、变化总数/重复页被拒绝、持续满页有界停止，不能证明现场无漏数。须结合可见性延迟、现场短窗口对账验证排序/上限/可见性；补偿与长周期对账仍属于待实现的 P0-05。未知字段或服务端未公开的硬截断限制必须先确认并适配，不能宣称已完成现场验收。

## 16. 退出码

| 退出码 | 含义 |
|---:|---|
| `0` | 命令成功完成 |
| `1` | 一般执行失败，例如 API、网络、文件或参数异常 |
| `2` | YAML 配置校验失败 |
| `3` | 输出文件校验失败 |

在脚本中可以通过 `$LASTEXITCODE` 判断结果：

```powershell
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar validate-config --config config/site.yaml
$LASTEXITCODE
```

## 17. 常见问题排查

### 17.1 提示环境变量未设置

原因通常是 AccessKey 在另一个终端窗口中设置，或者环境变量名与 YAML 不一致。

处理：在启动 Java 的同一个 PowerShell 中重新设置，并用 `Test-Path Env:变量名` 检查。

### 17.2 返回 401 或 403

检查：

- AccessKey ID/Secret 是否正确。
- AccessKey 是否已启用。
- 是否有 ARMS/SLS 对应只读权限。
- 专有云是否要求组织级 AccessKey、个人 AccessKey 或 STS。

### 17.3 ARMS 应用或 PID 不存在

检查 RegionId、API Version、PID 和当前账号可见范围。先执行 `discover` 获取实际应用列表。

### 17.4 ARMS 指标参数非法或无数据

检查：

- `action` 是否为当前支持的 `QueryMetric`；`QueryMetricByPage` 会被配置校验明确拒绝。
- Metric、Measures、Dimensions 是否受现场版本支持。
- Filters 是否同时包含 PID 和 RegionId。
- `IntervalInSec` 的单位是秒还是毫秒。
- 时间范围内是否确实有应用流量。

先用单个指标、短时间范围验证，再逐步增加任务。

### 17.5 SLS 提示没有索引

程序不会自动创建索引。需要在控制台确认目标 Logstore 已建立索引，且查询字段在索引范围内。

### 17.6 SLS 查询一直未完成

尝试：

- 缩短 `windowSeconds`。
- 缩小时间范围。
- 收紧 Query 条件。
- 增加 `maxPollAttempts`。
- 检查现场 SLS 集群负载。

### 17.7 出现 429 或超时

程序会自动重试。仍然失败时，可缩短时间窗口、减少同时配置的任务，或降低单次查询范围。

### 17.8 输出目录提示已有其他项目或批次

说明实际数据根目录中已有不同 `projectCode` 或 `batchId` 的 manifest。请为新批次修改 `storage.dataRoot`，不要删除或覆盖旧批次数据；旧 YAML 则修改兼容字段 `outputRoot`。

### 17.9 出现 `.partial` 文件

说明某个文件写入过程中发生中断。保留现场数据，先执行 `resume`，完成后运行 `validate-output`。如果 `.partial` 仍存在，应人工核对后再处理，不要直接把它当成正式 JSON。

### 17.10 采集结束但文件数量为零

检查：

- 配置中的任务列表是否为空。
- 时间范围内是否有实验流量。
- PID、Project、Logstore 和 Query 是否正确。
- manifest 是否记录了返回 0 条的请求。
- `collector.log` 是否存在错误。

## 18. Apsara Stack 适配注意事项

- 以甲方提供的专有云文档和现场控制台为准，不直接套用公有云 Endpoint 或 API Version。
- Apsara Stack 可能只开放 HTTP，应按现场协议配置，不擅自切换 HTTPS。
- ARMS 指标数组按 RPC 形式编码为 `Measures.1`、`Filters.1.Key/Value` 等。
- 不同 ARMS 版本的额外参数可以放入任务的 `parameters` 中透传。
- SLS 查询依赖已有索引，程序不会为了采集而修改 Logstore 配置。
- `probe`、`discover` 都是只读操作，但仍可能产生少量查询扫描量。
- 正式采集前必须进行短时间、小范围试采。

## 19. 数据与凭据保护建议

- 不要把 `config/site.yaml` 中加入实际 Secret。
- 不要把 AccessKey 写进命令行参数、脚本文件或聊天记录。
- 不要把客户端私钥、KeyStore、TrustStore 或其密码加入镜像构建上下文和 Git；只通过只读 Secret Volume 和 Secret 环境变量提供。
- 不要在日志中输出证书正文、私钥、密码或 Secret 挂载路径；排障只使用证书指纹、`CN` 摘要和到期时间。
- 不要把包含真实 AccessKey 的终端截图发送给无关人员。
- 采集完成后关闭 PowerShell，或清理当前会话的环境变量。
- 复制和交付数据时，保留 manifest，用 SHA-256 验证完整性。
- raw 数据可能包含接口、IP、SQL、错误信息等敏感内容，应按项目要求加密保存和传输。

清理当前 PowerShell 会话中的凭据变量：

```powershell
Remove-Item Env:ARMS_ACCESS_KEY_ID -ErrorAction SilentlyContinue
Remove-Item Env:ARMS_ACCESS_KEY_SECRET -ErrorAction SilentlyContinue
Remove-Item Env:SLS_ACCESS_KEY_ID -ErrorAction SilentlyContinue
Remove-Item Env:SLS_ACCESS_KEY_SECRET -ErrorAction SilentlyContinue
Remove-Item Env:SLS_STS_TOKEN -ErrorAction SilentlyContinue
Remove-Item Env:ARMS_DATASET_USERNAME -ErrorAction SilentlyContinue
Remove-Item Env:ARMS_DATASET_PASSWORD -ErrorAction SilentlyContinue
Remove-Item Env:COLLECTOR_KEYSTORE_PASSWORD -ErrorAction SilentlyContinue
Remove-Item Env:COLLECTOR_TRUSTSTORE_PASSWORD -ErrorAction SilentlyContinue
```

## 20. 最短现场操作清单

在已经确认配置内容的情况下，可以按下面的顺序执行：

```powershell
# 1. 设置 AccessKey 环境变量
$env:ARMS_ACCESS_KEY_ID='<现场值>'
$env:ARMS_ACCESS_KEY_SECRET='<现场值>'
$env:SLS_ACCESS_KEY_ID='<现场值>'
$env:SLS_ACCESS_KEY_SECRET='<现场值>'

# 2. 校验配置
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar validate-config --config config/site.yaml

# 3. 探测权限与网络
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar probe --config config/site.yaml

# 4. 发现应用和 Logstore
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar discover --config config/site.yaml

# 5. 预演正式时间范围
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar dry-run --config config/site.yaml --start 2026-07-27T09:00:00+08:00 --end 2026-07-27T10:00:00+08:00

# 6. 正式采集
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar collect --config config/site.yaml --start 2026-07-27T09:00:00+08:00 --end 2026-07-27T10:00:00+08:00

# 7. 校验输出
java -jar target/aliyun-data-collector-0.1.0-SNAPSHOT.jar validate-output --config config/site.yaml
```

如果第 6 步中断，保留全部输出，使用相同配置和时间范围把 `collect` 改为 `resume`。

# 阿里云原生数据采集程序

本程序面向 Apsara Stack/阿里云专有云现场环境，用 Java 只读调用 ARMS 和 SLS API。开发模式可将链路、指标和日志保存为本地 raw JSON；生产模式将其标准化、组批并经 Outbox 主动上传到 `cc-ingest`。程序既支持指定时间范围的一次性采集，也支持常驻采集与上传。

本文按照“出发前准备—现场配置—权限探测—短时间试采—正式采集—结果校验—断点恢复”的顺序说明完整使用方法。

## 1. 已实现的能力

- ARMS RPC 模式：应用发现、链路搜索、链路详情和指标查询。
- ARMS Dataset 模式：通过 Dataset 查询接口采集数据。
- SLS 模式：发现 Logstore，并按时间窗口、offset 分页查询日志。
- 按固定时间长度分窗，避免一次请求时间跨度过大。
- API 暂时性失败自动重试，SLS 未完成查询自动轮询。
- 每完成一页即落盘，不需要等全部采集结束才保存。
- 使用 checkpoint 记录进度，支持中断后继续采集。
- 提供 `daemon` 常驻模式，按独立周期轮询 ARMS 和 SLS。
- 按“当前时间减可见性延迟”确定查询终点，并持久化各数据源成功水位。
- 单轮 API 失败不结束常驻进程；失败轮次不推进水位，后续轮次自动重试。
- 调度支持随机抖动、系统时间回拨保护和停止信号下的优雅退出。
- 配置模型已覆盖采集端身份、补偿参数、Ingest 上传、mTLS、Outbox 存储和中控任务参数，并提供完整边界校验。
- 已冻结 `cc-ingest` v1 批量协议，并提供 gzip HTTP 客户端、可靠回执校验和本地模拟接口测试。
- 已实现独立 `BatchUploader`：Outbox 原子认领、配置化并发、带抖动退避、Retry-After、BLOCKED/认证暂停、413 确定性拆分，以及上传水位更新后删除。
- `daemon` 已将采集循环和上传循环独立串联：任一侧暂时失败不会停止另一侧，空队列按通知等待，到期 retry 按 `next_retry_at` 自动恢复。
- 已实现 PVC 文件式 Outbox：批次目录原子发布、pending/retry/BLOCKED 状态、启动恢复和单进程并发认领。
- 已实现生产 Outbox/PVC 容量保护：NORMAL/WARNING/CRITICAL/RECOVERING 滞回状态机、临界暂停、恢复采集和上传持续运行。
- 已实现独立 `UploadWatermarkStore`：按 `poll_id` 保存连续提交边界、稀疏 ACK、末批和完成状态，并在完成后压缩逐批证据。
- 已实现 ARMS Trace/Span/Metric/Dataset 与 SLS 日志到 `StandardEvent` 的纯转换组件，包括稳定 `event_id`、多格式时间、字段准入和无原值告警。
- 已实现采集端 mTLS：从只读 Secret Volume 加载 PEM 或 KeyStore 身份和 TrustStore，强制服务端证书链与主机名校验，支持到期告警和安全轮换。
- 自动生成 manifest，记录文件大小、记录数、Request ID 和 SHA-256。
- 使用 `.partial` 临时文件和原子改名，避免把未写完的文件当成正式结果。
- 配置、原始文件、日志和 manifest 中不保存 AccessKey Secret。

## 2. 安全边界

程序只提供查询入口，不提供资源修改入口。

- ARMS 只允许：`ListTraceApps`、`SearchTracesByPage`、`GetTrace`、`QueryMetric`、`QueryMetricByPage`、`QueryDataset`。
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
├─ src/                          Java 源代码和测试
├─ target/                       Maven 构建产物
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
- 使用 `QueryMetric` 还是 `QueryMetricByPage`。
- 需要采集的 Metric、Measures、Dimensions、Filters。
- `IntervalInSec` 在现场版本中的实际单位和最小值。

### 5.2 ARMS Dataset

- 完整 Dataset URL。
- Dataset ID。
- 用户名或 `_userId`。
- 是否需要 HTTP Basic 密码。
- Measures、额外查询参数和时间间隔。

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

control:
  enabled: false
  taskClaimUrl: null
  heartbeatIntervalSeconds: 30
  taskPollIntervalSeconds: 10
  requestTimeoutSeconds: 30

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

`upload.enabled`、`security.mtlsEnabled` 和 `control.enabled` 默认关闭。启用 `upload.enabled` 后，`daemon` 会同时启动相互独立的采集循环和上传循环，持续消费 Outbox 的 pending 与到期 retry。mTLS 已接入上传客户端；PEM 模式使用 `clientCertificatePath` 和 `privateKeyPath`，KEYSTORE 模式使用 `keyStorePath`，两种模式不能混用，密码只通过环境变量名引用。任务领取仍将在对应后续任务中实现。

接口字段、哈希算法、状态码和中控端实现要求见 [cc-ingest v1 批量上传接口契约](docs/cc-ingest-v1接口契约.md)，机器可读定义见 [OpenAPI 3.1 文件](docs/openapi/cc-ingest-v1.openapi.yaml)。`upload.ingestUrl` 必须使用固定路径 `/collector/v1/events:batch`。

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

开发 raw 模式下，程序读取 `checkpoints/checkpoint.json`，跳过已经成功的页面、Trace 详情和任务窗口。

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

停止进程时，调度器先停止创建新轮询，再等待正在执行的采集结束，最长等待 `shutdownGraceSeconds`。生产 daemon 会独立运行采集、容量监控和可选上传循环；采集或容量暂停不会停止上传，上传故障也不会终止采集进程。当前阶段尚未实现最近 30 分钟补偿查询和长周期对账。

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
   └─ capacity-state.json         最近一次持久化的容量保护状态
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

`daemon` 启动时复用采集管道的同一个 `OutboxStore`，确保进程内认领互斥。上传循环每轮最多认领 `upload.maxInFlightBatches` 个批次；单批 `BLOCKED` 会被后续扫描跳过，不妨碍其他 pending/retry。启动时 Outbox 自动恢复 writing/pending/retry，随后上传循环立即处理可发送批次。关闭时先停止并等待采集，再停止上传循环；宽限期内允许当前查询和在途上传完成。

生产模式每隔 `diskCheckIntervalSeconds` 检查 Outbox 所在 PVC 文件系统的总字节和可用字节，并使用以下状态机：

- `NORMAL`：低于告警水位，正常采集和上传。
- `WARNING`：达到 `diskWarningWatermarkPercent`，记录告警但继续采集和上传；下降到 resume 水位前保持 WARNING。
- `CRITICAL`：达到 `diskCriticalWatermarkPercent`，或容量检测失败时失败关闭；新的 ARMS/SLS 查询窗口不再启动，采集水位不推进，上传循环继续消费已有 pending/retry。
- `RECOVERING`：从 CRITICAL 下降到 `diskResumeWatermarkPercent` 后进入并恢复采集；下一次检查仍处于恢复水位时回到 NORMAL，重新升高时按 WARNING/CRITICAL 规则处理。

必须满足 `diskResumeWatermarkPercent < diskWarningWatermarkPercent < diskCriticalWatermarkPercent`。状态首次检查和状态切换会原子保存到 `state/capacity-state.json`，Pod 重启后先恢复原状态再同步检查当前容量。容量保护从不删除 pending、retry 或 BLOCKED 批次；只有上传器获得可靠提交回执并更新上传水位后才允许删除。开启 raw 的开发模式不启动生产 PVC 容量监控，开发 raw 数据不参与该状态机。

当前版本已实现 P0-03 标准事件转换、P0-04 任务级采集水位、P0-06 文件式 Outbox、P0-07 流式确定性组批、P0-08 独立上传器、P0-09 上传水位联动、P0-10 的生产采集与 daemon 上传线程串联、P0-11 采集端 mTLS，以及 Outbox/PVC 字节容量门控。尚未完成的是中控端证书身份与 `collector_id` 绑定、积压指标、inode/文件数保护和长时间故障恢复验收。

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

自动重试主要针对：

- 网络连接异常。
- 请求超时。
- HTTP 408、429。
- HTTP 5xx。
- SDK 返回的限流、服务器或暂时性网络错误。

权限错误、参数错误和资源不存在通常立即失败，不反复重试。一次性 `collect/resume` 失败时程序会：

1. 把错误写入 manifest 的 `errors`。
2. 把错误写入 `logs/collector.log`。
3. 保留已经成功落盘的数据。
4. 以非零状态码退出。

修复问题后使用 `resume`，不需要从头开始。

`daemon` 模式中，单轮失败不会结束进程。该轮水位保持不变，调度器在下一周期继续尝试；正常停止时则先停止创建新轮询，并等待正在进行的落盘操作安全结束。

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

- `action` 是 `QueryMetric` 还是 `QueryMetricByPage`。
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

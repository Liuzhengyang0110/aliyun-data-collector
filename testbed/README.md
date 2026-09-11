# aliyun-data-collector 独立集成测试床

> **严禁部署到生产环境。严禁使用真实 AccessKey、真实证书或真实业务数据。**

本目录是 `aliyun-data-collector` 的独立 Maven 测试床，包含可运行的 `mock-cc-ingest` 和 `mock-arms`。它用于在没有真实 control-center 和真实 ARMS 时，对“模拟 ARMS → 采集 → Outbox → mTLS 上传 → 回执/水位/删除”的链路及故障恢复做黑盒验证。

它不是真实 control-center，也不能替代真实中控的数据库持久化、多副本一致性、Kafka、网关和灾备测试。Mock 环境的成功也不证明与真实阿里云或专有云 API 完全兼容。

## 目录

```text
testbed/
├─ pom.xml                         独立 Maven 聚合项目，不在根 pom.xml 中
├─ mock-cc-ingest/                 Java 17 HTTPS/mTLS Mock 服务
├─ mock-arms/                      Java 17 HTTP ARMS RPC/Dataset Mock 服务
├─ config/collector-mock.example.yaml
├─ scenarios/                      可直接提交的故障场景示例
├─ scripts/
│  ├─ generate-test-pki.ps1
│  ├─ start-testbed.ps1
│  ├─ stop-testbed.ps1
│  └─ run-smoke-test.ps1           完整链路与故障恢复测试
├─ docker-compose.yml              两个隔离的本地 Mock 服务
├─ .gitignore
└─ README.md
```

`.generated`、`.state`、私钥、KeyStore、TrustStore、日志、PID 和接收数据均被忽略。测试床没有进入采集端 `src/main/java`，也没有修改根 `pom.xml` 的默认构建。

## 前置条件

- JDK 17 或更高版本；本项目源码目标为 Java 17。
- Maven 3.9 或兼容版本。
- PowerShell 7。
- Docker 方式另需 Docker Compose v2。
- 所有命令从仓库根目录 `D:\Project\aliyun-data-collector` 执行。

## 生成测试证书

```powershell
.\testbed\scripts\generate-test-pki.ps1
```

脚本只使用 JDK 自带的 `keytool`，默认写入 `testbed/.generated/pki`，生成：

- 含 `localhost`、`mock-cc-ingest` 和 `127.0.0.1` SAN 的服务端证书；
- 正常客户端证书、轮换客户端证书、过期客户端证书；
- 不受信任 CA、客户端和服务端证书；
- 服务端/采集端 TrustStore；
- `collector_id` 到客户端证书 SHA-256 指纹的测试映射。

默认密码 `testbed-changeit` 是明显的测试值，只通过环境变量传给运行进程。删除 `.generated/pki` 后可重新生成；不要复制或改作生产证书。

## Maven 构建与直接运行

```powershell
mvn -f testbed/pom.xml test
mvn -f testbed/pom.xml package
.\testbed\scripts\start-testbed.ps1
```

等价的 JAR 启动方式：

```powershell
$env:MOCK_CC_KEYSTORE_PASSWORD = 'testbed-changeit'
$env:MOCK_CC_TRUSTSTORE_PASSWORD = 'testbed-changeit'
java -jar testbed/mock-cc-ingest/target/mock-cc-ingest-0.1.0-SNAPSHOT-all.jar `
  --bind 127.0.0.1 `
  --port 9443 `
  --state-dir testbed/.state/mock-cc-ingest `
  --key-store testbed/.generated/pki/server.p12 `
  --trust-store testbed/.generated/pki/server-trust.p12 `
  --collector-cert-map testbed/.generated/pki/collector-cert-map.json

$env:MOCK_ARMS_EXPECTED_ACCESS_KEY_ID = 'testbed-access-key-id'
$env:MOCK_ARMS_DATASET_USERNAME = 'testbed-user'
$env:MOCK_ARMS_DATASET_PASSWORD = 'testbed-password'
java -jar testbed/mock-arms/target/mock-arms-0.1.0-SNAPSHOT-all.jar `
  --bind 127.0.0.1 --port 9410
```

停止本地服务：

```powershell
.\testbed\scripts\stop-testbed.ps1
```

两个服务默认只监听 `127.0.0.1`。cc-ingest 必须显式提供服务端 KeyStore 和客户端 CA TrustStore；TLS 握手强制要求受信任且在有效期内的客户端证书。

## Docker Compose

先生成 PKI 并构建 JAR，再启动：

```powershell
.\testbed\scripts\generate-test-pki.ps1
mvn -f testbed/pom.xml package
$env:TESTBED_KEYSTORE_PASSWORD = 'testbed-changeit'
docker compose -f testbed/docker-compose.yml up -d --build
```

Compose 使用独立 bridge 网络，主机端口固定绑定 `127.0.0.1:9443` 和 `127.0.0.1:9410`。cc-ingest 状态使用独立命名卷，PKI 以只读 Volume 挂载且不会复制进镜像。容器内监听 `0.0.0.0` 仅用于 bridge 转发，不会通过主机其他网卡暴露。两个服务都使用 JAR 内置健康探针。每个服务默认限制为 1 CPU、256 MiB 内存；大结果集或 10 MiB 边界并发测试可临时提高到 512 MiB。

停止容器：

```powershell
docker compose -f testbed/docker-compose.yml down
```

如需同时删除测试接收卷，必须显式执行 `docker compose -f testbed/docker-compose.yml down -v`。

## 采集端测试配置

示例配置为 `testbed/config/collector-mock.example.yaml`，使用生产运行约束：

- `runtime.deploymentMode=PRODUCTION`；
- `storage.rawPersistence.enabled=false`（现有配置模型中对应“不持久化 raw”）；
- SLS 关闭；
- 主动上传开启，固定路径为 `https://localhost:9443/collector/v1/events:batch`；
- mTLS 开启，证书位于 `.generated/pki`；
- AccessKey 和所有密码字段只保存环境变量名；值均为明显测试值。

ARMS RPC 指向 `127.0.0.1:9410`，包含 Trace 搜索/详情和 Metric 测试任务。配置中仅出现环境变量名；脚本设置的 AccessKey、Dataset 用户和密码都是明显测试值。采集端当前配置模型使用 `storage.rawPersistence.enabled=false` 表达“不持久化 raw”。

## mock-arms 行为

RPC 入口接收现有阿里云 Java SDK 发出的 POST，从 query 或 form 中解析公共参数，白名单仅允许 `ListTraceApps`、`SearchTracesByPage`、`GetTrace`、`QueryMetric` 和 `QueryMetricByPage`。响应包含 `x-acs-request-id`。可通过 `MOCK_ARMS_EXPECTED_ACCESS_KEY_ID` 校验约定测试 AccessKeyId；不实现完整签名校验，也从不记录 `Signature`、AccessKey、Authorization 或 Secret。

Dataset 入口为 `POST /QueryDataset`，只接受 `application/x-www-form-urlencoded`，解析 `datasetId`、`minTime`、`maxTime`、`intervalInSec`、`measures`、`_userId`。设置 `MOCK_ARMS_DATASET_USERNAME`/`MOCK_ARMS_DATASET_PASSWORD` 后会检查测试 Basic Authentication。

Trace、Span、Metric 和 Dataset 数据均由请求时间范围和查询身份确定性生成，包含稳定 trace/span、service、instance 和时间字段。模块自身的 `src/main/resources/scenarios` 打包了脱敏参考样本，不依赖采集端测试资源的运行时相对路径。

管理接口：

- `GET /__admin/health`
- `GET /__admin/requests`（仅返回已脱敏参数和 Action 计数）
- `POST /__admin/reset`
- `POST /__admin/scenario`

支持 `NORMAL`、`EMPTY`、`PAGINATED`、`DUPLICATE_RECORDS`、`RATE_LIMITED`、`SERVER_ERROR`、`DELAY`、`MALFORMED_JSON` 和 `INVALID_SCHEMA`。`times` 用尽后自动恢复 `NORMAL`；还可用 `target_action`、`page_number`、`trace_id` 定向到特定调用。例如：

```powershell
Invoke-RestMethod http://127.0.0.1:9410/__admin/scenario -Method Post `
  -ContentType application/json `
  -Body (Get-Content testbed/scenarios/mock-arms-page-2-error.json -Raw)
```

## cc-ingest 行为

`POST /collector/v1/events:batch` 严格要求 `Content-Type: application/json` 和 `Content-Encoding: gzip`，并校验：

- cc-ingest v1 必填字段、类型、长度、枚举和 UTC 时间范围；
- `schema_version=1.0`；
- `record_count` 与 `events` 数量；
- 每个 `payload_sha256` 和整批 `content_sha256` 的规范 JSON SHA-256；
- 默认最多 1000 条、压缩后 2 MiB、解压后 10 MiB。

合法新批次先写同目录临时文件、强制刷盘，再以原子移动发布；文件系统不支持原子移动时失败关闭，不返回 `COMMITTED`。同 ID/同哈希返回 `ALREADY_COMMITTED`，同 ID/不同哈希返回 409 且不覆盖。

接收状态默认位于 `testbed/.state/mock-cc-ingest/received`。文件名使用 `batch_id` 的 SHA-256，避免批次 ID 参与路径解析。日志不记录请求 payload、Authorization、密码或私钥；客户端证书日志只包含 SHA-256 指纹、CN 摘要和到期时间。

## 管理接口与故障注入

所有管理接口也通过同一个 mTLS 端口访问：

- `GET /__admin/health`
- `GET /__admin/status`
- `GET /__admin/received`
- `GET /__admin/received?include_payload=true`
- `POST /__admin/reset?delete_committed=false`
- `POST /__admin/reset?delete_committed=true`
- `POST /__admin/scenario`

`reset` 必须显式给出 `delete_committed=true|false`；无参数请求会返回 400。`false` 只清场景和运行计数，`true` 才删除已提交测试数据。

故障场景 JSON 示例：

```json
{"scenario":"RATE_LIMITED","times":2,"retry_after_seconds":5}
```

```json
{"scenario":"DELAY","times":1,"delay_millis":1500}
```

支持 `NORMAL`、`ALREADY_COMMITTED`、`BAD_REQUEST`、`UNAUTHORIZED`、`FORBIDDEN`、`CONFLICT`、`PAYLOAD_TOO_LARGE`、`RATE_LIMITED`、`SERVICE_UNAVAILABLE`、`DELAY`、`DROP_BEFORE_COMMIT`、`DROP_AFTER_COMMIT`、`MALFORMED_RECEIPT`、`WRONG_BATCH_ID` 和 `WRONG_CONTENT_HASH`。`times` 指定持续的合法上传请求次数，用尽后自动恢复 `NORMAL`。

PowerShell 脚本使用 JAR 中的 `MockCcAdminClient` 调用管理接口，因为 Windows 自带的 Schannel curl 通常不能直接加载 PKCS#12 客户端身份。示例：

```powershell
$env:MOCK_CC_HEALTH_KEYSTORE_PATH = (Resolve-Path testbed/.generated/pki/client-1.p12).Path
$env:MOCK_CC_HEALTH_TRUSTSTORE_PATH = (Resolve-Path testbed/.generated/pki/collector-trust.p12).Path
$env:MOCK_CC_KEYSTORE_PASSWORD = 'testbed-changeit'
$env:MOCK_CC_TRUSTSTORE_PASSWORD = 'testbed-changeit'
java -cp testbed/mock-cc-ingest/target/mock-cc-ingest-0.1.0-SNAPSHOT-all.jar `
  cn.kjky.collector.testbed.cc.MockCcAdminClient `
  --url https://localhost:9443/__admin/status --method GET --body ''
```

## 完整 Smoke Test

```powershell
.\testbed\scripts\run-smoke-test.ps1
```

脚本构建两个 Mock 和真实采集端，使用虚构 ARMS 数据与测试证书启动实际 daemon，验证：

```text
mock-arms → ArmsRpcApi/Normalizer → pending Outbox → collector IngestClient → gzip/mTLS → mock-cc-ingest
→ COMMITTED → upload-watermarks.json → 删除已提交 Outbox
```

同一次执行还断言：cc 停止期间继续采集并保留 Outbox；恢复后自动补传；503 用尽后自动恢复；`DROP_AFTER_COMMIT` 使用同一 `batch_id` 重传并以 `ALREADY_COMMITTED` 收敛；daemon 重启后继续原 pending/retry；重复 ARMS 记录产生相同稳定 `event_id`。成功时输出接收批次/事件、累计重试、重复提交、重启前 Outbox、ARMS 请求数和最终剩余 Outbox。

脚本要求 9410/9443 空闲并自行管理进程；已有测试床请先运行 `stop-testbed.ps1`。

## 常见故障

- `Address already in use`：运行 `stop-testbed.ps1`，再检查 9443 是否被其他进程占用。
- Mock ARMS 404：检查 `Action` 是否在白名单；400 则检查 `Version`、`RegionId` 和动作必填参数。
- Mock ARMS 403：测试 `AccessKeyId` 与 `MOCK_ARMS_EXPECTED_ACCESS_KEY_ID` 不一致。
- Dataset 401：检查测试 Basic 用户/密码环境变量；管理端不会显示 Authorization。
- TLS 无响应：确认使用 `client-1.p12` 和 `collector-trust.p12`，密码环境变量已设置；无证书、不受信任、过期或主机名错误都应在握手阶段失败。
- HTTP 403：检查请求体 `collector_id` 是否为 `testbed-collector-01`，以及 `collector-cert-map.json` 是否对应当前客户端证书。
- HTTP 400：检查 gzip、Content-Type、必填字段、UTC 时间和两级 SHA-256。
- HTTP 409：相同 `batch_id` 已提交过不同内容；先查询 `received`，仅在确认是测试状态后显式 reset 删除。
- Smoke Test 中 collector 退出：查看 `.generated/run/smoke-collector.stderr.log`、`smoke-mock-arms.stderr.log`；不要把真实 Secret 写入日志或配置。
- 根项目测试在 Windows 系统临时目录清理时报 `AccessDeniedException`：将 `TEMP`、`TMP` 指向仓库内临时目录后运行；这不改变生产代码。

## 安全与能力边界

- 测试床不得部署到生产环境。
- 不得使用真实 AccessKey、证书和业务数据。
- Mock ARMS 不校验完整阿里云签名，不能替代真实阿里云/专有云的现场 API、网络、版本差异和权限联调。
- Mock cc-ingest 不能替代真实中控持久化、多副本和 Kafka 测试。
- 管理接口是本地测试能力，不应暴露到非本机网络。

# 阿里云原生数据采集程序

本程序面向 Apsara Stack/阿里云专有云现场环境，用 Java 只读调用 ARMS 和 SLS API，将链路、指标和日志数据保存为本地 JSON 文件。当前 `jdk-8` 分支不依赖 Spring Boot 运行环境，可作为独立可执行 JAR 使用；后续也可以把核心采集类接入 Spring Boot 平台。

本文按照“出发前准备—现场配置—权限探测—短时间试采—正式采集—结果校验—断点恢复”的顺序说明完整使用方法。

## 1. 已实现的能力

- ARMS RPC 模式：应用发现、链路搜索、链路详情和指标查询。
- ARMS Dataset 模式：通过 Dataset 查询接口采集数据。
- SLS 模式：发现 Logstore，并按时间窗口、offset 分页查询日志。
- 按固定时间长度分窗，避免一次请求时间跨度过大。
- API 暂时性失败自动重试，SLS 未完成查询自动轮询。
- 每完成一页即落盘，不需要等全部采集结束才保存。
- 使用 checkpoint 记录进度，支持中断后继续采集。
- 自动生成 manifest，记录文件大小、记录数、Request ID 和 SHA-256。
- 使用 `.partial` 临时文件和原子改名，避免把未写完的文件当成正式结果。
- 配置、原始文件、日志和 manifest 中不保存 AccessKey Secret。

## 2. 安全边界

程序只提供查询入口，不提供资源修改入口。

- ARMS 只允许：`ListTraceApps`、`SearchTracesByPage`、`GetTrace`、`QueryMetric`、`QueryMetricByPage`、`QueryDataset`。
- SLS 只调用：`ListLogStores`、`GetLogs`。
- 代码中没有创建、更新、删除、写日志、创建索引、分裂或合并 Shard 的调用。
- AccessKey、密码和 STS Token 只从运行进程的环境变量读取。
- Endpoint、ApiRegionId、RegionId、组织/资源组请求头、API Version、PID、Dataset ID、Project、Logstore、查询条件和命名字段均由 YAML 配置提供。

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
target/aliyun-data-collector-0.1.2-SNAPSHOT.jar
```

## 4. 运行环境

运行程序需要：

- Java 8 Update 192（`1.8.0_192`）或更高版本；仅运行程序时安装 JRE 即可。
- Windows PowerShell 或 Linux Shell。
- 能访问现场 ARMS/SLS Endpoint 的网络。
- 已开通相应只读 API 权限的 AccessKey。
- 已填写完成的现场 YAML 配置。

检查 Java：

```powershell
java -version
```

跳板机的输出中应能看到 `java version "1.8.0_192"`，或者更高的 Java 版本。还应确认执行 `java` 命令的机器就是实际运行采集程序、能够访问 ARMS/SLS Endpoint 的机器。

本分支使用 Java 8 API 编译，应用代码生成的 class 文件版本为 52。代码中未使用 record、switch 表达式、`java.net.http.HttpClient` 等 Java 9 及以上版本才提供的语法或 API。

如果已经有构建好的可执行 JAR，现场机器不需要 Maven，也不需要下载运行时依赖。只有修改代码或重新打包时才需要 JDK 和 Maven 3.9+：

```powershell
mvn clean package
```

构建过程会运行单元测试，并通过 Maven Shade Plugin 把运行依赖打入同一个 JAR；成功后在 `target` 目录生成可执行 JAR。

查看程序帮助：

```powershell
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar --help
```

## 5. 现场需要确认的信息

首次运行前，需要从控制台和甲方文档确认以下内容。

### 5.1 ARMS RPC

- ARMS API Endpoint。
- 使用 HTTP 还是 HTTPS。
- ApiRegionId：API 网关/SDK 路由地域，例如 `bjdc-1`。
- RegionId：ARMS 资源地域，作为接口查询参数发送，例如 `zj-3`。
- 是否需要 `x-acs-organizationid` 和 `x-acs-resourcegroupid` 请求头。
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
- `outputRoot`：本批次数据保存位置。

建议不同批次使用不同的 `outputRoot`，避免把多批数据混在同一个 manifest 中。

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
outputRoot: "./data-package/batch001"

runtime:
  windowSeconds: 600
  maxRetries: 3
  maxPollAttempts: 10

arms:
  enabled: true
  mode: "RPC"
  endpoint: "现场ARMS地址"
  protocol: "HTTP"
  apiRegionId: "现场API路由地域"
  regionId: "现场RegionId"
  organizationId: "现场组织ID；不要求时留空"
  resourceGroupId: "现场资源组ID；不要求时留空"
  product: "ARMS"
  version: "现场API版本"
  accessKeyIdEnv: "ARMS_ACCESS_KEY_ID"
  accessKeySecretEnv: "ARMS_ACCESS_KEY_SECRET"
  traceQueries:
    - name: "all-traces"
      recordType: "topology_edge"
      pid: "现场应用PID"
  metricQueries:
    - name: "service-metric"
      recordType: "flow_feature"
      action: "QueryMetric"
      metric: "现场Metric"
      measures: ["现场Measure"]
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
```

模板中包含更多可选字段，应以模板和甲方 API 文档为准。

### 6.2 ARMS Dataset 模式

复制 Dataset 模板：

```powershell
Copy-Item config/arms-dataset.example.yaml config/site.yaml
```

Dataset 模式下：

- `arms.mode` 设置为 `DATASET`。
- `_userId` 自动取 `usernameEnv` 指定的环境变量。
- 只有现场网关要求 HTTP Basic 时，才设置密码环境变量。
- `sls.enabled` 可以保持 `false`，也可以补充 SLS 配置后同时采集。

## 7. 配置 AccessKey 环境变量

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

检查变量是否存在，不要输出 Secret 内容：

```powershell
Test-Path Env:ARMS_ACCESS_KEY_ID
Test-Path Env:ARMS_ACCESS_KEY_SECRET
Test-Path Env:SLS_ACCESS_KEY_ID
Test-Path Env:SLS_ACCESS_KEY_SECRET
```

这些变量默认只在当前 PowerShell 会话有效。关闭窗口后需要重新设置。

如果 ARMS 和 SLS 使用同一组 AccessKey，可以给四个变量设置相同值，也可以在 YAML 中让 ARMS 和 SLS 引用相同的环境变量名。

## 8. 时间参数说明

`dry-run`、`collect` 和 `resume` 需要同时指定 `--start`、`--end`。

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
| `collect` | 是 | 是 | 是 | 执行正式采集 |
| `resume` | 是 | 是 | 是 | 使用 checkpoint 继续未完成采集 |
| `validate-output` | 否 | 否 | 否 | 校验 manifest、文件大小、SHA-256 和 `.partial` 文件 |
| `build-manifest` | 否 | 否 | 只写辅助清单 | 原 manifest 丢失时，从 raw 文件重建辅助 manifest |

所有命令都需要 `--config`。需要时间范围的命令还需要 `--start` 和 `--end`。

## 10. 各命令含义与用法

### 10.1 `validate-config`：校验配置

```powershell
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar `
  validate-config --config config/site.yaml
```

它会检查：

- 必填字段是否为空或仍是 `__REQUIRED_ON_SITE__`。
- 项目、环境、批次、任务名和记录类型是否适合用于文件名。
- ARMS 模式和指标 Action 是否受支持。
- 页大小、重试次数、轮询次数是否合理。
- 是否至少启用了 ARMS 或 SLS。

该命令不访问 API，也不要求 AccessKey 环境变量存在。建议每次修改 YAML 后先执行一次。

### 10.2 `probe`：最小权限与连通性探测

```powershell
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar `
  probe --config config/site.yaml
```

RPC 模式下，ARMS 调用 `ListTraceApps`；SLS 调用 `ListLogStores`。Dataset 模式会使用第一个 Dataset 任务查询最近 60 秒，只验证接口能否工作，不保存原始采集文件。

如果失败，优先检查：

- Endpoint、协议、ApiRegionId 和 RegionId。
- 组织 ID、资源组 ID 是否与开发者门户成功请求一致。
- AccessKey 是否在当前 PowerShell 中设置。
- AccessKey 是否具有只读 API 权限。
- 现场网络、DNS、防火墙或代理。
- ARMS API Version 是否正确。

### 10.3 `discover`：发现现场资源

```powershell
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar `
  discover --config config/site.yaml
```

输出包括：

- ARMS 可见应用及 PID。
- 配置中各 SLS Project 下可见的 Logstore。

根据输出回填或修正 `pid`、`project`、`logstore`。Dataset 模式没有统一的应用发现接口，需要按甲方文档填写 Dataset ID。

### 10.4 `dry-run`：预演采集计划

```powershell
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar `
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
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar `
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
7. 每一页成功后立即写入 raw 文件。
8. 计算 SHA-256、更新 manifest 和 checkpoint。

程序当前按窗口和采集源串行执行，以降低现场接口压力。命令结束并显示“采集完成”后，再执行输出校验。

### 10.6 `resume`：断点续采

中断、断网或修复权限后，使用与原采集完全相同的配置、开始时间和结束时间：

```powershell
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar `
  resume --config config/site.yaml `
  --start 2026-07-27T09:00:00+08:00 `
  --end 2026-07-27T10:00:00+08:00
```

程序读取 `checkpoints/checkpoint.json`，跳过已经成功的页面、Trace 详情和任务窗口。

如果程序在“文件已经落盘、checkpoint 尚未写入”之间中断，恢复时可能再次采集该页，但会使用新的分片号，不会覆盖已有原始文件。这种设计优先保证原始数据不丢失，后续可按时间、Request ID 或内容哈希去重。

不要删除 checkpoint 后直接重跑同一批次，除非明确希望重新采集。

### 10.7 `validate-output`：校验输出

```powershell
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar `
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

### 10.8 `build-manifest`：重建辅助清单

```powershell
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar `
  build-manifest --config config/site.yaml
```

仅在原 manifest 丢失或损坏时使用。它会扫描 `raw` 目录并生成：

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
3. 为本批次设置独立 `batchId` 和 `outputRoot`。
4. 在当前 PowerShell 设置 AccessKey 环境变量。
5. 执行 `validate-config`。

### 阶段三：探测与回填

按顺序执行：

```powershell
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar validate-config --config config/site.yaml
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar probe --config config/site.yaml
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar discover --config config/site.yaml
```

根据 `discover` 输出修正 PID、Project 和 Logstore，再次执行 `validate-config`。

### 阶段四：预演和短时间试采

先预演 10 分钟：

```powershell
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar `
  dry-run --config config/site.yaml `
  --start 2026-07-27T09:00:00+08:00 `
  --end 2026-07-27T09:10:00+08:00
```

确认无误后试采相同的短时间范围：

```powershell
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar `
  collect --config config/site.yaml `
  --start 2026-07-27T09:00:00+08:00 `
  --end 2026-07-27T09:10:00+08:00
```

试采后检查：

- `raw/arms`、`raw/sls` 是否生成文件。
- JSON 内容是否为目标应用和目标环境。
- 文件名中的项目、环境、批次、记录类型和时间是否正确。
- `manifest.json` 是否记录文件、SHA-256 和 Request ID。
- `collector.log` 是否存在错误。
- 执行 `validate-output` 是否通过。

建议短时间试采使用独立批次或独立 `outputRoot`。确认后再创建正式批次，避免试采数据混入正式结果。

### 阶段五：正式采集

1. 确认正式 `batchId`、`caseId` 和 `outputRoot`。
2. 使用 `dry-run` 核对完整时间范围和窗口数。
3. 执行 `collect`。
4. 保持终端窗口和网络连接，不要关闭 PowerShell。
5. 观察控制台输出和 `logs/collector.log`。
6. 采集完成后执行 `validate-output`。
7. 备份整个输出目录，而不是只复制 `raw`。

### 阶段六：发生中断时

1. 不要删除已经生成的 raw、manifest 和 checkpoint。
2. 根据错误信息修复网络、权限或配置。
3. 确认仍使用原来的 `outputRoot`、批次和时间范围。
4. 执行 `resume`。
5. 完成后再次执行 `validate-output`。

## 12. 输出目录与文件含义

```text
data-package/batch001/
├─ raw/
│  ├─ arms/                 ARMS 原始 JSON 响应
│  └─ sls/                  SLS 查询信封和日志键值内容
├─ checkpoints/
│  └─ checkpoint.json       已完成请求和任务窗口
├─ manifest/
│  ├─ manifest.json         正式文件清单
│  └─ manifest-rebuilt.json 可选的辅助重建清单
└─ logs/
   └─ collector.log         运行过程和错误日志
```

正式交付或备份时，建议保留整个批次目录，包括 raw、checkpoint、manifest 和 logs。

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

权限错误、参数错误和资源不存在通常立即失败，不反复重试。失败时程序会：

1. 把错误写入 manifest 的 `errors`。
2. 把错误写入 `logs/collector.log`。
3. 保留已经成功落盘的数据。
4. 以非零状态码退出。

修复问题后使用 `resume`，不需要从头开始。

## 16. 退出码

| 退出码 | 含义 |
|---:|---|
| `0` | 命令成功完成 |
| `1` | 一般执行失败，例如 API、网络、文件或参数异常 |
| `2` | YAML 配置校验失败 |
| `3` | 输出文件校验失败 |

在脚本中可以通过 `$LASTEXITCODE` 判断结果：

```powershell
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar validate-config --config config/site.yaml
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

说明 `outputRoot` 中已有不同 `projectCode` 或 `batchId` 的 manifest。请为新批次修改 `outputRoot`，不要删除或覆盖旧批次数据。

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
- ApiRegionId 用于 SDK/网关路由，RegionId 用于目标资源查询；两者不能相互替代。
- 现场文档要求组织/资源组时，程序分别发送 `x-acs-organizationid`、`x-acs-resourcegroupid` 请求头。
- ARMS 指标数组按 RPC 形式编码为 `Measures.1`、`Filters.1.Key/Value` 等。
- 不同 ARMS 版本的额外参数可以放入任务的 `parameters` 中透传。
- SLS 查询依赖已有索引，程序不会为了采集而修改 Logstore 配置。
- `probe`、`discover` 都是只读操作，但仍可能产生少量查询扫描量。
- 正式采集前必须进行短时间、小范围试采。

## 19. 数据与凭据保护建议

- 不要把 `config/site.yaml` 中加入实际 Secret。
- 不要把 AccessKey 写进命令行参数、脚本文件或聊天记录。
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
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar validate-config --config config/site.yaml

# 3. 探测权限与网络
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar probe --config config/site.yaml

# 4. 发现应用和 Logstore
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar discover --config config/site.yaml

# 5. 预演正式时间范围
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar dry-run --config config/site.yaml --start 2026-07-27T09:00:00+08:00 --end 2026-07-27T10:00:00+08:00

# 6. 正式采集
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar collect --config config/site.yaml --start 2026-07-27T09:00:00+08:00 --end 2026-07-27T10:00:00+08:00

# 7. 校验输出
java -jar target/aliyun-data-collector-0.1.2-SNAPSHOT.jar validate-output --config config/site.yaml
```

如果第 6 步中断，保留全部输出，使用相同配置和时间范围把 `collect` 改为 `resume`。

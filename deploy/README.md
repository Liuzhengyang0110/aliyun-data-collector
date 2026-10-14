# Kubernetes 部署说明

本目录提供阿里云采集端的生产部署模板。模板只用于生成和本地校验资源，不会连接或修改真实 Kubernetes 集群。当前基线为 Java 17、Spring Boot 3.5.7，禁止使用 Java 8 镜像或宣称 Java 8 兼容。

## 1. 构建镜像

先使用 Java 17 和离线 Maven 仓库生成可执行 JAR：

```powershell
mvn --offline test
mvn --offline package
```

Dockerfile 只复制已经生成的 JAR，运行阶段不会下载依赖。`RUNTIME_IMAGE` 没有默认值，必须传入现场内部 JRE 17 基础镜像的固定 digest；不得使用 tag 代替 digest，也不得使用公网镜像：

```powershell
$runtimeImage = "registry.intra.example/runtime/jre17@sha256:<64位真实摘要>"
$applicationImage = "registry.intra.example/security/aliyun-data-collector:0.1.0-SNAPSHOT"
docker build --build-arg "RUNTIME_IMAGE=$runtimeImage" --build-arg "APP_VERSION=0.1.0-SNAPSHOT" -t $applicationImage .
```

基础镜像至少需要提供 Java 17 运行时，并允许使用数字 UID/GID `10001:10001`。Dockerfile 不需要 shell、curl、wget 或包管理器。构建完成后，应把 `deployment.yaml` 的镜像占位符替换成推送到内部仓库后的应用镜像固定 digest。

镜像内应用目录是 `/opt/aliyun-data-collector`；生产 PVC 挂载在 `/var/lib/aliyun-data-collector`；可写临时目录是 `/tmp`；配置与证书分别挂载到 `/etc/aliyun-data-collector/site.yaml` 和 `/etc/aliyun-data-collector/tls`。除此之外根文件系统保持只读。

## 2. Secret 与 ConfigMap 边界

`configmap.yaml` 只包含非敏感参数。部署前必须替换采集器身份、ARMS/SLS Endpoint、查询范围和中控服务地址，但不得把 AccessKey、Token、证书、私钥或密码写进 ConfigMap。

Deployment 从 `aliyun-data-collector-credentials` 引用以下键：

- `ARMS_ACCESS_KEY_ID`
- `ARMS_ACCESS_KEY_SECRET`
- `SLS_ACCESS_KEY_ID`
- `SLS_ACCESS_KEY_SECRET`
- 可选的 `SLS_STS_TOKEN`
- `COLLECTOR_KEYSTORE_PASSWORD`
- `COLLECTOR_TRUSTSTORE_PASSWORD`

KeyStore 与 TrustStore 使用另一个 Secret `aliyun-data-collector-tls`，分别以 `client.p12` 和 `truststore.p12` 只读挂载。应在受控终端通过安全流程从文件创建 Secret，例如使用 `kubectl create secret generic ... --from-file`；不要复制 `secret.example.yaml` 中的字符串占位内容作为真实证书，也不要把生成后的 Secret YAML 提交到 Git。

如果现场改用 PEM 模式，应把客户端证书和未加密 PKCS#8 私钥作为只读 Secret 文件挂载，并同步修改 `security.clientAuthMode`、`clientCertificatePath` 和 `privateKeyPath`；TrustStore 仍单独挂载，密码仍通过 Secret 环境变量注入。

## 3. PVC 与生命周期

生产模式固定为 `deploymentMode=PRODUCTION`、`rawPersistence.enabled=false`、`upload.enabled=true`。应用在 PVC 中只持久化：

```text
/var/lib/aliyun-data-collector/
├─ outbox/
│  ├─ writing/
│  ├─ pending/
│  └─ retry/
└─ state/
```

生产配置不会创建 `raw`、`manifest`、`committed` 或归档目录。开发 raw 数据必须使用开发配置和独立调试目录，不能写入此生产 PVC。

ConfigMap 中的 `defaultBytes` 是没有远程策略时的默认 Outbox 逻辑配额，`minBytes` 与 `maxBytes` 是中控不能突破的本地安全边界。中控下发的最后有效策略保存在 PVC 的 `state/runtime-policy.json`，不会写回 ConfigMap，也不会改变 PVC 对象容量。示例 PVC 为 30 GiB、逻辑最大值为 20 GiB并预留 1 GiB；最终数值须根据现场数据量、故障恢复目标和压测结果确定。

Deployment 固定单副本并采用 `Recreate`，避免两个实例同时写同一 RWO PVC。`terminationGracePeriodSeconds=90`，大于应用 `shutdownGraceSeconds=60`；SIGTERM 直接发送到 Java 进程并触发现有优雅关闭流程。节点迁移、PVC 重新挂载、重建期间数据完整性仍须在真实集群验收。

## 4. 安全与网络

Pod 和容器均以非 root 身份运行，禁止提权，丢弃全部 Linux capabilities，使用 `RuntimeDefault` seccomp，根文件系统只读，并禁止自动挂载 ServiceAccount Token。模板没有使用 privileged、hostNetwork、hostPID 或 hostPath，也没有创建 Service、NodePort、LoadBalancer 或 Ingress。控制命令领取、执行结果、节点状态与数据上传都由采集端主动出站。

`network-policy.yaml` 先对命名空间默认拒绝全部入站和出站，再只为采集 Pod 放行：

- 集群 DNS 的 TCP/UDP 53；
- `cc-system` 命名空间中带指定标签的 `cc-ingest` 与 `cc-control-service` TCP 8443；
- 现场 ARMS/SLS 地址的 TCP 443。

原生 Kubernetes NetworkPolicy 不能按 FQDN 过滤外部 Endpoint。模板中的 `192.0.2.0/24` 和 `198.51.100.0/24` 是 RFC 5737 不可路由测试网段，只是明确占位符；部署前必须由网络管理员替换为现场受控 CIDR。如果 Endpoint 地址动态变化，需要使用支持 FQDN 策略的 CNI/出口网关方案，不能把本模板描述为已经实现 FQDN 级限制。模板默认不放行 NTP，因为 Pod 通常使用节点时间；只有现场确需 Pod 主动访问 NTP 时，才应为已批准的 NTP CIDR 增加 UDP 123 规则。

DNS Pod 标签、`cc-system` 命名空间、工作负载标签和服务端口同样必须按现场集群核对。NetworkPolicy 的真实阻断效果取决于现场 CNI，必须部署后单独验收。

## 5. 健康检查边界

当前 P1-02 已实现对中控的状态上报，但尚未提供可可靠区分“关键线程失活”和“外部依赖短暂不可达”的 Kubernetes liveness/readiness 接口或命令。因此 Deployment 暂不配置探针，也不使用 `java` 进程存在、curl、bash 或 wget 作为伪健康检查。

后续探针实现应满足：liveness 只反映进程与关键循环是否存活；readiness 反映配置、PVC 和关键状态文件是否可用。ARMS、SLS、`cc-ingest`、`cc-control-service` 暂时不可达以及人工暂停采集均不能直接导致 liveness 失败。完成该代码能力与测试后，再更新 Deployment 和 P1-02/P1-04 对应条目。

## 6. 本地渲染与现场步骤

`secret.example.yaml` 故意不在 `kustomization.yaml` 中，避免占位 Secret 被误应用。可在不连接集群的前提下执行：

```powershell
kubectl kustomize deploy
kubectl apply --dry-run=client -k deploy
```

第二条命令在某些 kubectl 版本中仍可能尝试访问 API discovery；如果现场前本机没有可用 schema，只以 `kubectl kustomize` 和项目自动化 YAML 测试作为本地静态检查，不得声称已完成集群验证。

现场部署前至少完成：

1. 把 PVC 的 StorageClass、容量和访问模式改为现场值。
2. 替换 ConfigMap 内全部 `replace-on-site` 与 `.internal.invalid` 值。
3. 使用内部 JRE 17 固定 digest 构建镜像，并把 Deployment 镜像改成内部应用镜像固定 digest。
4. 通过安全流程创建凭据与 TLS Secret，核对 KeyStore/TrustStore 密码环境变量。
5. 替换 ARMS/SLS CIDR、DNS 标签、中控命名空间/Pod 标签和端口。
6. 渲染资源并由管理员评审，再在隔离测试命名空间部署。
7. 完成 Pod 启停、SIGTERM、只读根、非 root、PVC 重挂载、NetworkPolicy 阻断和 Secret 轮换的现场验证。

CPU、内存和临时存储的示例 requests/limits 取值偏保守，只用于初始验证；最终值必须依据现场 ARMS/SLS 响应规模、并发批次、压缩开销和长时间压测结果确定。

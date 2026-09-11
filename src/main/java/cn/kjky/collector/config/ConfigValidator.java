package cn.kjky.collector.config;

import java.net.URI;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 对反序列化后的配置执行集中校验。
 * 所有错误尽量一次性返回，便于现场人员统一修正，而不是每次只看到一个问题。
 */
public final class ConfigValidator {
    /** 允许出现在文件名各字段中的字符规则。 */
    private static final Pattern TOKEN = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]*");
    /** 合法环境变量名规则。 */
    private static final Pattern ENV_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /**
     * 校验公共、ARMS 和 SLS 配置。
     *
     * @param c 待校验的根配置
     * @param requireSecrets 是否同时检查凭据环境变量；仅 API 命令需要
     * @return 所有发现的错误；空列表表示校验通过
     */
    public List<String> validate(CollectorConfig c, boolean requireSecrets) {
        // errors 汇总全部问题，避免第一个错误就中止校验。
        List<String> errors = new ArrayList<>();
        if (c == null) {
            errors.add("配置不能为空");
            return errors;
        }
        requiredToken(errors, "projectCode", c.projectCode);
        requiredToken(errors, "envCode", c.envCode);
        requiredToken(errors, "batchId", c.batchId);
        requiredToken(errors, "caseId", c.caseId);
        try { ZoneId.of(c.timezone); } catch (Exception e) { errors.add("timezone 无效: " + c.timezone); }
        validateCollector(c.collector, errors);
        validateRuntime(c.runtime, errors);
        validateScheduler(c, errors);
        validateUpload(c.upload, c.security, errors);
        boolean communicationSecretsRequired = requireSecrets
                && ((c.upload != null && c.upload.enabled) || (c.control != null && c.control.enabled));
        validateSecurity(c.security, errors, communicationSecretsRequired);
        if (c.runtime != null && "PRODUCTION".equalsIgnoreCase(c.runtime.deploymentMode)
                && c.upload != null && c.upload.enabled
                && (c.security == null || !c.security.mtlsEnabled)) {
            errors.add("PRODUCTION 模式启用上传时必须启用 security.mtlsEnabled");
        }
        validateStorage(c, errors);
        validateControl(c.control, c.security, errors);

        boolean armsEnabled = c.arms != null && c.arms.enabled;
        boolean slsEnabled = c.sls != null && c.sls.enabled;
        if (c.arms == null) errors.add("arms 不能为空");
        if (c.sls == null) errors.add("sls 不能为空");
        if (!armsEnabled && !slsEnabled) errors.add("至少启用 arms 或 sls 之一");

        // taskNames 用于发现同一类任务中的重复名称，防止 checkpoint 键冲突。
        Set<String> taskNames = new HashSet<>();
        if (armsEnabled) validateArms(c, errors, taskNames, requireSecrets);
        if (slsEnabled) validateSls(c, errors, taskNames, requireSecrets);
        return errors;
    }

    /** 校验采集端身份。 */
    private void validateCollector(CollectorConfig.CollectorIdentityConfig collector, List<String> errors) {
        if (collector == null) {
            errors.add("collector 不能为空");
            return;
        }
        requiredToken(errors, "collector.collectorId", collector.collectorId);
        requiredToken(errors, "collector.siteId", collector.siteId);
        if (!"ALIYUN".equalsIgnoreCase(collector.collectorType)) {
            errors.add("collector.collectorType 只能是 ALIYUN");
        }
    }

    /** 校验原有窗口、重试和 SLS 状态轮询参数。 */
    private void validateRuntime(CollectorConfig.RuntimeConfig runtime, List<String> errors) {
        if (runtime == null) {
            errors.add("runtime 不能为空");
            return;
        }
        if (!"DEVELOPMENT".equalsIgnoreCase(runtime.deploymentMode)
                && !"PRODUCTION".equalsIgnoreCase(runtime.deploymentMode)) {
            errors.add("runtime.deploymentMode 只能是 DEVELOPMENT 或 PRODUCTION");
        }
        if (runtime.windowSeconds < 1 || runtime.windowSeconds > 86400) errors.add("runtime.windowSeconds 必须在 1..86400");
        if (runtime.requestTimeoutSeconds <= 0 || runtime.requestTimeoutSeconds > 300) errors.add("runtime.requestTimeoutSeconds 必须在 1..300");
        if (runtime.maxRetries < 0 || runtime.maxRetries > 20) errors.add("runtime.maxRetries 必须在 0..20");
        if (runtime.retryBaseMillis < 0 || runtime.retryBaseMillis > 60000) errors.add("runtime.retryBaseMillis 必须在 0..60000");
        if (runtime.maxPollAttempts < 1 || runtime.maxPollAttempts > 1000) errors.add("runtime.maxPollAttempts 必须在 1..1000");
        if (runtime.pollIntervalMillis < 0 || runtime.pollIntervalMillis > 60000) errors.add("runtime.pollIntervalMillis 必须在 0..60000");
    }

    /** 校验常驻模式公共参数以及 ARMS/SLS 各自的轮询参数。 */
    private void validateScheduler(CollectorConfig c, List<String> errors) {
        if (c.scheduler == null) {
            errors.add("scheduler 不能为空");
            return;
        }
        if (c.scheduler.jitterSeconds < 0 || c.scheduler.jitterSeconds > 3600) errors.add("scheduler.jitterSeconds 必须在 0..3600");
        if (c.scheduler.shutdownGraceSeconds < 1 || c.scheduler.shutdownGraceSeconds > 3600) errors.add("scheduler.shutdownGraceSeconds 必须在 1..3600");
        if (c.scheduler.compensationIntervalSeconds < 1 || c.scheduler.compensationIntervalSeconds > 86400) errors.add("scheduler.compensationIntervalSeconds 必须在 1..86400");
        if (c.scheduler.compensationLookbackSeconds < 1 || c.scheduler.compensationLookbackSeconds > 2592000) errors.add("scheduler.compensationLookbackSeconds 必须在 1..2592000");
        if (c.scheduler.compensationLookbackSeconds < c.scheduler.compensationIntervalSeconds) {
            errors.add("scheduler.compensationLookbackSeconds 不能小于 compensationIntervalSeconds");
        }
        validateSourceSchedule("scheduler.arms", c.scheduler.arms, errors);
        validateSourceSchedule("scheduler.sls", c.scheduler.sls, errors);
    }

    /** 校验单个数据源的调度边界。 */
    private void validateSourceSchedule(String path, CollectorConfig.SourceScheduleConfig schedule, List<String> errors) {
        if (schedule == null) {
            errors.add(path + " 不能为空");
            return;
        }
        if (schedule.pollIntervalSeconds < 1 || schedule.pollIntervalSeconds > 86400) errors.add(path + ".pollIntervalSeconds 必须在 1..86400");
        if (schedule.visibilityDelaySeconds < 0 || schedule.visibilityDelaySeconds > 604800) errors.add(path + ".visibilityDelaySeconds 必须在 0..604800");
        if (schedule.initialLookbackSeconds < 1 || schedule.initialLookbackSeconds > 2592000) errors.add(path + ".initialLookbackSeconds 必须在 1..2592000");
    }

    /** 校验上传批量、容量、URL 和超时参数。 */
    private void validateUpload(CollectorConfig.UploadConfig upload, CollectorConfig.SecurityConfig security,
                                List<String> errors) {
        if (upload == null) {
            errors.add("upload 不能为空");
            return;
        }
        if (upload.enabled) required(errors, "upload.ingestUrl", upload.ingestUrl);
        if (configured(upload.ingestUrl)) {
            httpUrl(errors, "upload.ingestUrl", upload.ingestUrl);
            ingestPath(errors, upload.ingestUrl);
        }
        if (upload.maxRecords < 1 || upload.maxRecords > 10000) errors.add("upload.maxRecords 必须在 1..10000");
        if (upload.maxCompressedBytes < 1024 || upload.maxCompressedBytes > 100L * 1024 * 1024) {
            errors.add("upload.maxCompressedBytes 必须在 1024..104857600");
        }
        if (upload.maxRequestBytes < 1024 || upload.maxRequestBytes > 100L * 1024 * 1024) {
            errors.add("upload.maxRequestBytes 必须在 1024..104857600");
        }
        if (upload.maxCompressedBytes > upload.maxRequestBytes) {
            errors.add("upload.maxCompressedBytes 不能大于 maxRequestBytes");
        }
        if (upload.maxWaitMillis < 1 || upload.maxWaitMillis > 60000) errors.add("upload.maxWaitMillis 必须在 1..60000");
        if (upload.maxInFlightBatches < 1 || upload.maxInFlightBatches > 32) errors.add("upload.maxInFlightBatches 必须在 1..32");
        timeout(errors, "upload.connectTimeoutSeconds", upload.connectTimeoutSeconds);
        timeout(errors, "upload.requestTimeoutSeconds", upload.requestTimeoutSeconds);
        if (upload.retryInitialDelayMillis < 1 || upload.retryInitialDelayMillis > 3_600_000) {
            errors.add("upload.retryInitialDelayMillis 必须在 1..3600000");
        }
        if (upload.retryMaxDelayMillis < upload.retryInitialDelayMillis
                || upload.retryMaxDelayMillis > 86_400_000) {
            errors.add("upload.retryMaxDelayMillis 必须不小于 retryInitialDelayMillis 且不大于 86400000");
        }
        if (!Double.isFinite(upload.retryJitterRatio)
                || upload.retryJitterRatio < 0.0d || upload.retryJitterRatio > 1.0d) {
            errors.add("upload.retryJitterRatio 必须在 0..1");
        }
        if (upload.retryWarningAttempts < 1 || upload.retryWarningAttempts > 1_000_000) {
            errors.add("upload.retryWarningAttempts 必须在 1..1000000");
        }
        if (upload.enabled && security != null && security.mtlsEnabled) httpsUrl(errors, "upload.ingestUrl", upload.ingestUrl);
    }

    /** 校验 PEM/KeyStore 二选一及信任库配置。 */
    private void validateSecurity(CollectorConfig.SecurityConfig security, List<String> errors,
                                  boolean requireSecrets) {
        if (security == null) {
            errors.add("security 不能为空");
            return;
        }
        optionalPath(errors, "security.clientCertificatePath", security.clientCertificatePath);
        optionalPath(errors, "security.privateKeyPath", security.privateKeyPath);
        optionalPath(errors, "security.keyStorePath", security.keyStorePath);
        optionalPath(errors, "security.trustStorePath", security.trustStorePath);
        if (security.certificateExpiryWarningDays < 1 || security.certificateExpiryWarningDays > 3650) {
            errors.add("security.certificateExpiryWarningDays 必须在 1..3650");
        }
        if (security.certificateReloadIntervalSeconds < 1 || security.certificateReloadIntervalSeconds > 86400) {
            errors.add("security.certificateReloadIntervalSeconds 必须在 1..86400");
        }
        if (!security.mtlsEnabled) return;

        boolean pem = "PEM".equalsIgnoreCase(security.clientAuthMode);
        boolean keyStore = "KEYSTORE".equalsIgnoreCase(security.clientAuthMode);
        if (!pem && !keyStore) {
            errors.add("security.clientAuthMode 只能是 PEM 或 KEYSTORE");
            return;
        }
        required(errors, "security.trustStorePath", security.trustStorePath);
        requiredEnvName(errors, "security.trustStorePasswordEnv", security.trustStorePasswordEnv);
        if (requireSecrets) secret(errors, security.trustStorePasswordEnv, "TrustStore 密码");
        storeType(errors, "security.trustStoreType", security.trustStoreType);
        if (pem) {
            required(errors, "security.clientCertificatePath", security.clientCertificatePath);
            required(errors, "security.privateKeyPath", security.privateKeyPath);
            if (configured(security.keyStorePath) || configured(security.keyStorePasswordEnv)) {
                errors.add("security.clientAuthMode=PEM 时不能配置 KeyStore");
            }
        } else {
            required(errors, "security.keyStorePath", security.keyStorePath);
            requiredEnvName(errors, "security.keyStorePasswordEnv", security.keyStorePasswordEnv);
            if (requireSecrets) secret(errors, security.keyStorePasswordEnv, "KeyStore 密码");
            storeType(errors, "security.keyStoreType", security.keyStoreType);
            if (configured(security.clientCertificatePath) || configured(security.privateKeyPath)) {
                errors.add("security.clientAuthMode=KEYSTORE 时不能配置 PEM 证书或私钥");
            }
        }
    }

    /** 校验数据根目录、Outbox/状态/raw 相对路径和磁盘水位。 */
    private void validateStorage(CollectorConfig c, List<String> errors) {
        if (c.storage == null) {
            errors.add("storage 不能为空");
            return;
        }
        optionalPath(errors, "outputRoot", c.outputRoot);
        optionalPath(errors, "storage.dataRoot", c.storage.dataRoot);
        relativeDirectory(errors, "storage.outboxDir", c.storage.outboxDir);
        relativeDirectory(errors, "storage.stateDir", c.storage.stateDir);
        if (c.storage.rawPersistence == null) {
            errors.add("storage.rawPersistence 不能为空");
        } else {
            relativeDirectory(errors, "storage.rawPersistence.directory", c.storage.rawPersistence.directory);
            if ("PRODUCTION".equalsIgnoreCase(c.runtime == null ? null : c.runtime.deploymentMode)
                    && c.storage.rawPersistence.enabled) {
                errors.add("PRODUCTION 模式必须关闭 storage.rawPersistence.enabled");
            }
        }
        if (configured(c.outputRoot) && configured(c.storage.dataRoot)) {
            try {
                Path legacy = Path.of(c.outputRoot).toAbsolutePath().normalize();
                Path current = Path.of(c.storage.dataRoot).toAbsolutePath().normalize();
                if (!legacy.equals(current)) errors.add("outputRoot 与 storage.dataRoot 不能同时配置为不同目录");
            } catch (InvalidPathException ignored) {
                // 路径格式错误已由 optionalPath 报告。
            }
        }
        optionalPath(errors, "storage.effectiveDataRoot", c.effectiveDataRoot());
        if (c.storage.diskWarningWatermarkPercent < 1 || c.storage.diskWarningWatermarkPercent > 99) {
            errors.add("storage.diskWarningWatermarkPercent 必须在 1..99");
        }
        if (c.storage.diskCriticalWatermarkPercent < 1 || c.storage.diskCriticalWatermarkPercent > 99) {
            errors.add("storage.diskCriticalWatermarkPercent 必须在 1..99");
        }
        if (c.storage.diskResumeWatermarkPercent < 1 || c.storage.diskResumeWatermarkPercent > 99) {
            errors.add("storage.diskResumeWatermarkPercent 必须在 1..99");
        }
        if (!(c.storage.diskResumeWatermarkPercent < c.storage.diskWarningWatermarkPercent
                && c.storage.diskWarningWatermarkPercent < c.storage.diskCriticalWatermarkPercent)) {
            errors.add("storage 磁盘水位必须满足 diskResumeWatermarkPercent < diskWarningWatermarkPercent < diskCriticalWatermarkPercent");
        }
        if (c.storage.diskCheckIntervalSeconds < 1 || c.storage.diskCheckIntervalSeconds > 3600) {
            errors.add("storage.diskCheckIntervalSeconds 必须在 1..3600");
        }
        if (configured(c.storage.outboxDir) && configured(c.storage.stateDir)
                && pathsOverlap(c.storage.outboxDir, c.storage.stateDir)) {
            errors.add("storage.outboxDir 和 stateDir 不能相同或相互嵌套");
        }
        if (c.storage.rawPersistence != null && configured(c.storage.rawPersistence.directory)) {
            String raw = c.storage.rawPersistence.directory;
            if (pathsOverlap(raw, c.storage.outboxDir) || pathsOverlap(raw, c.storage.stateDir)) {
                errors.add("storage.rawPersistence.directory 不能与 outboxDir 或 stateDir 相同或相互嵌套");
            }
        }
    }

    /** 校验任务领取地址和控制轮询间隔。 */
    private void validateControl(CollectorConfig.ControlConfig control, CollectorConfig.SecurityConfig security,
                                 List<String> errors) {
        if (control == null) {
            errors.add("control 不能为空");
            return;
        }
        if (control.enabled) required(errors, "control.taskClaimUrl", control.taskClaimUrl);
        if (configured(control.taskClaimUrl)) httpUrl(errors, "control.taskClaimUrl", control.taskClaimUrl);
        if (control.heartbeatIntervalSeconds < 1 || control.heartbeatIntervalSeconds > 86400) {
            errors.add("control.heartbeatIntervalSeconds 必须在 1..86400");
        }
        if (control.taskPollIntervalSeconds < 1 || control.taskPollIntervalSeconds > 86400) {
            errors.add("control.taskPollIntervalSeconds 必须在 1..86400");
        }
        timeout(errors, "control.requestTimeoutSeconds", control.requestTimeoutSeconds);
        if (control.enabled && security != null && security.mtlsEnabled) {
            httpsUrl(errors, "control.taskClaimUrl", control.taskClaimUrl);
        }
    }

    /**
     * 校验 ARMS RPC 或 Dataset 分支。
     *
     * @param c 根配置，从中读取 arms 节点
     * @param errors 错误汇总列表
     * @param names 已使用的任务名称集合
     * @param secrets 是否检查凭据环境变量
     */
    private void validateArms(CollectorConfig c, List<String> errors, Set<String> names, boolean secrets) {
        // a 是 arms 配置的局部别名，减少后续重复访问 c.arms。
        var a = c.arms;
        boolean rpc = "RPC".equalsIgnoreCase(a.mode);
        boolean datasetMode = "DATASET".equalsIgnoreCase(a.mode);
        if (!rpc && !datasetMode) {
            errors.add("arms.mode 只能是 RPC 或 DATASET");
            return;
        }
        if (rpc) {
            required(errors, "arms.endpoint", a.endpoint);
            required(errors, "arms.regionId", a.regionId);
            required(errors, "arms.product", a.product);
            required(errors, "arms.version", a.version);
            required(errors, "arms.accessKeyIdEnv", a.accessKeyIdEnv);
            required(errors, "arms.accessKeySecretEnv", a.accessKeySecretEnv);
            if (a.pageSize < 1 || a.pageSize > 100) errors.add("arms.pageSize 必须在 1..100");
            if (secrets) secret(errors, a.accessKeyIdEnv, "ARMS AccessKey ID");
            if (secrets) secret(errors, a.accessKeySecretEnv, "ARMS AccessKey Secret");
            if (a.traceQueries == null) errors.add("arms.traceQueries 不能为空");
            else for (var q : a.traceQueries) {
                if (q == null) { errors.add("arms.traceQueries 不能包含 null"); continue; }
                task(errors, names, "arms.traceQueries", q.name, q.recordType);
                validateNormalization(errors, "arms.traceQueries." + q.name + ".normalization", q.normalization);
            }
            if (a.metricQueries == null) errors.add("arms.metricQueries 不能为空");
            else for (var q : a.metricQueries) {
                if (q == null) { errors.add("arms.metricQueries 不能包含 null"); continue; }
                task(errors, names, "arms.metricQueries", q.name, q.recordType);
                validateNormalization(errors, "arms.metricQueries." + q.name + ".normalization", q.normalization);
                if (!"QueryMetric".equals(q.action) && !"QueryMetricByPage".equals(q.action)) {
                    errors.add("指标任务 " + q.name + " 的 action 只能是 QueryMetric 或 QueryMetricByPage");
                }
                required(errors, "arms.metricQueries.metric", q.metric);
                if (q.measures == null || q.measures.isEmpty()) errors.add("指标任务 " + q.name + " 缺少 measures");
                if (q.limit < 1 || q.limit > 10000) errors.add("指标任务 " + q.name + " 的 limit 必须在 1..10000");
            }
        } else {
            if (a.dataset == null) {
                errors.add("arms.dataset 不能为空");
                return;
            }
            required(errors, "arms.dataset.url", a.dataset.url);
            if (secrets) secret(errors, a.dataset.usernameEnv, "ARMS Dataset 用户名");
            if (a.dataset.queries == null) {
                errors.add("arms.dataset.queries 不能为空");
                return;
            }
            for (var q : a.dataset.queries) {
                if (q == null) { errors.add("arms.dataset.queries 不能包含 null"); continue; }
                task(errors, names, "arms.dataset.queries", q.name, q.recordType);
                validateNormalization(errors, "arms.dataset.queries." + q.name + ".normalization", q.normalization);
                required(errors, "arms.dataset.queries.datasetId", q.datasetId);
                if (q.measures == null) errors.add("Dataset 任务 " + q.name + " 的 measures 不能为空");
                else if (q.measures.size() > 3) errors.add("Dataset 任务 " + q.name + " 的 measures 最多 3 个");
            }
        }
    }

    /**
     * 校验 SLS Endpoint、凭据变量名、Project 和 Logstore。
     *
     * @param c 根配置，从中读取 sls 节点
     * @param errors 错误汇总列表
     * @param names 已使用的任务名称集合
     * @param secrets 是否检查凭据环境变量
     */
    private void validateSls(CollectorConfig c, List<String> errors, Set<String> names, boolean secrets) {
        // s 是 sls 配置的局部别名。
        var s = c.sls;
        required(errors, "sls.endpoint", s.endpoint);
        required(errors, "sls.accessKeyIdEnv", s.accessKeyIdEnv);
        required(errors, "sls.accessKeySecretEnv", s.accessKeySecretEnv);
        if (secrets) secret(errors, s.accessKeyIdEnv, "SLS AccessKey ID");
        if (secrets) secret(errors, s.accessKeySecretEnv, "SLS AccessKey Secret");
        if (s.projects == null || s.projects.isEmpty()) {
            errors.add("sls.projects 不能为空");
            return;
        }
        for (var p : s.projects) {
            if (p == null) { errors.add("sls.projects 不能包含 null"); continue; }
            required(errors, "sls.projects.project", p.project);
            if (p.logstores == null || p.logstores.isEmpty()) {
                errors.add("SLS Project " + p.project + " 的 logstores 不能为空");
                continue;
            }
            for (var l : p.logstores) {
                if (l == null) { errors.add("sls.logstores 不能包含 null"); continue; }
                task(errors, names, "sls.logstores", p.project + "." + l.logstore, l.recordType);
                required(errors, "sls.logstores.logstore", l.logstore);
                if (l.pageSize < 1 || l.pageSize > 100) errors.add("SLS " + l.logstore + " 的 pageSize 必须在 1..100");
                validateNormalization(errors, "sls." + p.project + "." + l.logstore + ".normalization", l.normalization);
            }
        }
    }

    /** 校验标准化字段准入配置，防止空字段、重复字段和无界列表进入常驻任务。 */
    private void validateNormalization(List<String> errors, String path,
                                       CollectorConfig.NormalizationConfig normalization) {
        if (normalization == null) {
            errors.add(path + " 不能为空");
            return;
        }
        validateFieldNames(errors, path + ".payloadFields", normalization.payloadFields, 128);
        validateFieldNames(errors, path + ".identityFields", normalization.identityFields, 16);
    }

    /** 校验字段名列表；允许 SLS 的点号字段名，但不允许空白项和重复项。 */
    private void validateFieldNames(List<String> errors, String path, List<String> fields, int maximum) {
        if (fields == null) {
            errors.add(path + " 不能为空");
            return;
        }
        if (fields.size() > maximum) errors.add(path + " 最多配置 " + maximum + " 项");
        Set<String> names = new HashSet<>();
        for (String field : fields) {
            if (field == null || field.isBlank()) {
                errors.add(path + " 不能包含空字段名");
                continue;
            }
            if (field.length() > 256) errors.add(path + " 中字段名长度不能超过 256");
            String normalized = field.trim().toLowerCase(java.util.Locale.ROOT);
            if (!names.add(normalized)) errors.add(path + " 包含重复字段: " + field.trim());
        }
    }

    /**
     * 校验一个任务的名称和 recordType，并检查名称重复。
     *
     * @param errors 错误汇总列表
     * @param names 已出现任务名称集合
     * @param path YAML 中的任务路径，用于生成易定位的错误消息
     * @param name 任务名称
     * @param recordType 输出文件使用的记录类型
     */
    private void task(List<String> errors, Set<String> names, String path, String name, String recordType) {
        requiredToken(errors, path + ".name", name);
        requiredToken(errors, path + ".recordType", recordType);
        if (name != null && !names.add(path + ":" + name)) errors.add(path + " 中任务名重复: " + name);
    }

    /**
     * 检查普通必填字符串。
     *
     * @param errors 错误汇总列表
     * @param name 字段路径
     * @param value 字段值
     */
    private void required(List<String> errors, String name, String value) {
        if (value == null || value.isBlank() || value.startsWith("__REQUIRED")) errors.add(name + " 未配置");
    }

    /**
     * 检查必填字符串是否还能安全用作文件名字段。
     *
     * @param errors 错误汇总列表
     * @param name 字段路径
     * @param value 字段值
     */
    private void requiredToken(List<String> errors, String name, String value) {
        required(errors, name, value);
        if (value != null && !value.startsWith("__REQUIRED") && !TOKEN.matcher(value).matches()) {
            errors.add(name + " 只能包含字母、数字、点、下划线和连字符，且必须以字母或数字开头");
        }
    }

    /** 判断可选字符串是否配置了有效非空文本。 */
    private boolean configured(String value) {
        return value != null && !value.isBlank();
    }

    /** 校验一个可选路径的语法；不要求当前机器上已经挂载该文件。 */
    private void optionalPath(List<String> errors, String name, String value) {
        if (!configured(value)) return;
        try { Path.of(value); }
        catch (InvalidPathException e) { errors.add(name + " 路径无效"); }
    }

    /** 校验 dataRoot 下的非空相对目录，禁止绝对路径和 .. 越界。 */
    private void relativeDirectory(List<String> errors, String name, String value) {
        if (!configured(value)) {
            errors.add(name + " 未配置");
            return;
        }
        try {
            Path path = Path.of(value).normalize();
            if (path.isAbsolute() || path.startsWith("..") || path.toString().isBlank()) {
                errors.add(name + " 必须是 dataRoot 内的相对路径，且不能包含越界跳转");
            }
        } catch (InvalidPathException e) {
            errors.add(name + " 路径无效");
        }
    }

    /** 判断两个 dataRoot 内相对目录是否相同或存在父子嵌套。 */
    private boolean pathsOverlap(String first, String second) {
        if (!configured(first) || !configured(second)) return false;
        try {
            Path a = Path.of(first).normalize();
            Path b = Path.of(second).normalize();
            return a.startsWith(b) || b.startsWith(a);
        } catch (InvalidPathException e) {
            return false;
        }
    }

    /** 校验 HTTP(S) 绝对 URL。 */
    private void httpUrl(List<String> errors, String name, String value) {
        try {
            URI uri = URI.create(value);
            String scheme = uri.getScheme();
            if (!uri.isAbsolute() || uri.getHost() == null
                    || !("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))) {
                errors.add(name + " 必须是包含主机名的 HTTP 或 HTTPS 绝对 URL");
            }
        } catch (IllegalArgumentException e) {
            errors.add(name + " URL 无效");
        }
    }

    /** P0-01 固定批量上传路径，禁止容易破坏路由或幂等判断的查询参数和片段。 */
    private void ingestPath(List<String> errors, String value) {
        try {
            URI uri = URI.create(value);
            if (!"/collector/v1/events:batch".equals(uri.getPath())
                    || uri.getRawQuery() != null || uri.getFragment() != null) {
                errors.add("upload.ingestUrl 必须使用固定路径 /collector/v1/events:batch，且不能包含查询参数或片段");
            }
        } catch (IllegalArgumentException ignored) {
            // httpUrl 已经报告 URL 格式问题。
        }
    }

    /** mTLS 启用时要求控制面 URL 使用 HTTPS。 */
    private void httpsUrl(List<String> errors, String name, String value) {
        if (!configured(value)) return;
        try {
            if (!"https".equalsIgnoreCase(URI.create(value).getScheme())) errors.add(name + " 在启用 mTLS 时必须使用 HTTPS");
        } catch (IllegalArgumentException ignored) {
            // httpUrl 已经报告 URL 格式问题。
        }
    }

    /** 校验通用 HTTP 超时时间。 */
    private void timeout(List<String> errors, String name, int value) {
        if (value < 1 || value > 300) errors.add(name + " 必须在 1..300");
    }

    /** 校验 JKS/PKCS12 存储类型。 */
    private void storeType(List<String> errors, String name, String value) {
        if (!"JKS".equalsIgnoreCase(value) && !"PKCS12".equalsIgnoreCase(value)) {
            errors.add(name + " 只能是 JKS 或 PKCS12");
        }
    }

    /** 校验只保存变量名、不保存 Secret 内容的密码配置。 */
    private void requiredEnvName(List<String> errors, String name, String value) {
        required(errors, name, value);
        if (configured(value) && !value.startsWith("__REQUIRED") && !ENV_NAME.matcher(value).matches()) {
            errors.add(name + " 必须是合法环境变量名");
        }
    }

    /**
     * 检查凭据环境变量是否存在，仅判断存在性，不读取或记录凭据内容。
     *
     * @param errors 错误汇总列表
     * @param envName 环境变量名
     * @param label 面向用户显示的凭据名称
     */
    private void secret(List<String> errors, String envName, String label) {
        if (envName == null || envName.isBlank() || System.getenv(envName) == null || System.getenv(envName).isBlank()) {
            errors.add(label + " 对应环境变量未设置: " + envName);
        }
    }
}

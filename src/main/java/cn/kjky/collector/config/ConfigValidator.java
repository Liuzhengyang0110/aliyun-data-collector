package cn.kjky.collector.config;

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
        requiredToken(errors, "projectCode", c.projectCode);
        requiredToken(errors, "envCode", c.envCode);
        requiredToken(errors, "batchId", c.batchId);
        requiredToken(errors, "caseId", c.caseId);
        try { ZoneId.of(c.timezone); } catch (Exception e) { errors.add("timezone 无效: " + c.timezone); }
        try { Path.of(c.outputRoot); } catch (InvalidPathException | NullPointerException e) { errors.add("outputRoot 无效"); }
        if (c.runtime.windowSeconds <= 0) errors.add("runtime.windowSeconds 必须大于 0");
        if (c.runtime.maxRetries < 0) errors.add("runtime.maxRetries 不能小于 0");
        if (c.runtime.maxPollAttempts <= 0) errors.add("runtime.maxPollAttempts 必须大于 0");
        if (c.runtime.pollIntervalMillis < 0) errors.add("runtime.pollIntervalMillis 不能小于 0");
        if (!c.arms.enabled && !c.sls.enabled) errors.add("至少启用 arms 或 sls 之一");

        // taskNames 用于发现同一类任务中的重复名称，防止 checkpoint 键冲突。
        Set<String> taskNames = new HashSet<>();
        if (c.arms.enabled) validateArms(c, errors, taskNames, requireSecrets);
        if (c.sls.enabled) validateSls(c, errors, taskNames, requireSecrets);
        return errors;
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
        if (!"RPC".equalsIgnoreCase(a.mode) && !"DATASET".equalsIgnoreCase(a.mode)) {
            errors.add("arms.mode 只能是 RPC 或 DATASET");
        }
        if ("RPC".equalsIgnoreCase(a.mode)) {
            required(errors, "arms.endpoint", a.endpoint);
            required(errors, "arms.regionId", a.regionId);
            required(errors, "arms.product", a.product);
            required(errors, "arms.version", a.version);
            required(errors, "arms.accessKeyIdEnv", a.accessKeyIdEnv);
            required(errors, "arms.accessKeySecretEnv", a.accessKeySecretEnv);
            if (a.pageSize < 1 || a.pageSize > 100) errors.add("arms.pageSize 必须在 1..100");
            if (secrets) secret(errors, a.accessKeyIdEnv, "ARMS AccessKey ID");
            if (secrets) secret(errors, a.accessKeySecretEnv, "ARMS AccessKey Secret");
            for (var q : a.traceQueries) task(errors, names, "arms.traceQueries", q.name, q.recordType);
            for (var q : a.metricQueries) {
                task(errors, names, "arms.metricQueries", q.name, q.recordType);
                if (!"QueryMetric".equals(q.action) && !"QueryMetricByPage".equals(q.action)) {
                    errors.add("指标任务 " + q.name + " 的 action 只能是 QueryMetric 或 QueryMetricByPage");
                }
                required(errors, "arms.metricQueries.metric", q.metric);
                if (q.measures == null || q.measures.isEmpty()) errors.add("指标任务 " + q.name + " 缺少 measures");
                if (q.limit < 1 || q.limit > 10000) errors.add("指标任务 " + q.name + " 的 limit 必须在 1..10000");
            }
        } else {
            required(errors, "arms.dataset.url", a.dataset.url);
            if (secrets) secret(errors, a.dataset.usernameEnv, "ARMS Dataset 用户名");
            for (var q : a.dataset.queries) {
                task(errors, names, "arms.dataset.queries", q.name, q.recordType);
                required(errors, "arms.dataset.queries.datasetId", q.datasetId);
                if (q.measures.size() > 3) errors.add("Dataset 任务 " + q.name + " 的 measures 最多 3 个");
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
        if (s.projects == null || s.projects.isEmpty()) errors.add("sls.projects 不能为空");
        for (var p : s.projects) {
            required(errors, "sls.projects.project", p.project);
            for (var l : p.logstores) {
                task(errors, names, "sls.logstores", p.project + "." + l.logstore, l.recordType);
                required(errors, "sls.logstores.logstore", l.logstore);
                if (l.pageSize < 1 || l.pageSize > 100) errors.add("SLS " + l.logstore + " 的 pageSize 必须在 1..100");
            }
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

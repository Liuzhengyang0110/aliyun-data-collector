# syntax=docker/dockerfile:1

# 必须由构建命令传入现场内部 JRE 17 镜像的完整 digest 引用，例如：
# registry.intra.example/runtime/java17@sha256:<64位摘要>
# 不提供默认值，避免在离线/保密环境中意外回退到公网镜像。
ARG RUNTIME_IMAGE
FROM ${RUNTIME_IMAGE}

ARG APP_JAR=target/aliyun-data-collector-0.1.0-SNAPSHOT.jar
ARG APP_VERSION=0.1.0-SNAPSHOT

LABEL org.opencontainers.image.title="aliyun-data-collector" \
      org.opencontainers.image.version="${APP_VERSION}" \
      org.opencontainers.image.description="Apsara Stack ARMS/SLS collector"

WORKDIR /opt/aliyun-data-collector
COPY --chown=10001:10001 ${APP_JAR} application.jar

# JAVA_TOOL_OPTIONS 由 JVM 原生解析，无需 shell；Deployment 可整体覆盖该变量。
ENV JAVA_TOOL_OPTIONS="-XX:InitialRAMPercentage=25.0 -XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError -Djava.io.tmpdir=/tmp"

# 使用固定的非 root 数字身份，不依赖基础镜像中存在 useradd、shell 或用户名数据库。
USER 10001:10001

# exec 形式确保 SIGTERM 直接到达 JVM，并触发现有 CollectorDaemon shutdown hook。
ENTRYPOINT ["java", "-jar", "/opt/aliyun-data-collector/application.jar"]
CMD ["daemon", "--config", "/etc/aliyun-data-collector/site.yaml"]

FROM eclipse-temurin:17-jre
WORKDIR /opt/testbed
COPY target/mock-arms-0.1.0-SNAPSHOT-all.jar /opt/testbed/mock-arms.jar
USER 65532:65532
EXPOSE 9410
ENTRYPOINT ["java", "-jar", "/opt/testbed/mock-arms.jar", "--bind", "0.0.0.0", "--port", "9410"]

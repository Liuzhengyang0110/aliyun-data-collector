FROM eclipse-temurin:17-jre
WORKDIR /app
COPY target/mock-cc-ingest-0.1.0-SNAPSHOT-all.jar /app/mock-cc-ingest.jar
EXPOSE 9443
ENTRYPOINT ["java", "-jar", "/app/mock-cc-ingest.jar"]

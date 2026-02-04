# Build Stage
FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package

# Run Stage
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/raft-kv-store.jar .
EXPOSE 8001-8005
EXPOSE 9001-9005
ENTRYPOINT ["java", "-jar", "raft-kv-store.jar"]

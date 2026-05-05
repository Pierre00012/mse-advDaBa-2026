FROM maven:3.9.0-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src

RUN mvn clean package -DskipTests
FROM eclipse-temurin:17-jdk-focal
WORKDIR /app
COPY --from=build /app/target/*-uber.jar ./app.jar


CMD ["java", "-jar", "app.jar"]
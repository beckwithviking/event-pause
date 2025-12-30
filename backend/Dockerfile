FROM eclipse-temurin:21-jdk-alpine
WORKDIR /app
COPY . .
RUN chmod +x gradlew
RUN ./gradlew clean build -x test
ENTRYPOINT ["java", "-Djava.net.preferIPv4Stack=true", "-jar", "build/libs/pause-aware-app-0.0.1-SNAPSHOT.jar"]

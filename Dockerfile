FROM openjdk:17-jdk-slim AS build
WORKDIR /app
COPY . /app
RUN ./gradlew build --no-daemon -x test

#### Stage 2: A minimal docker image with command to run the app 
FROM openjdk:17-jdk-slim

EXPOSE 8080

RUN mkdir /app

COPY --from=build /app/demo/redis-sidecar-demo/build/libs/redis-sidecar-demo-*.jar /app/redis-sidecar-demo.jar

ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/app/redis-sidecar-demo.jar"]
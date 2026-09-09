FROM maven:3.9.15-eclipse-temurin-26 AS build
WORKDIR /build
COPY pom.xml .
COPY schemas schemas
COPY src src
RUN --mount=type=cache,target=/root/.m2 --mount=type=secret,id=maven_truststore \
    if [ -f /run/secrets/maven_truststore ]; then \
      export JAVA_TOOL_OPTIONS=-Djavax.net.ssl.trustStore=/run/secrets/maven_truststore; \
    fi; mvn -B -DskipTests package

FROM flink:2.2.1-scala_2.12-java21 AS runtime-base
USER root
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
 && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/ \
 && test -f /opt/flink/plugins/metrics-prometheus/flink-metrics-prometheus-2.2.1.jar \
 && mkdir -p /opt/flink/usrlib && chown -R flink:flink /opt/flink/usrlib
USER flink
COPY --chown=flink:flink infrastructure/docker/log4j-console.properties /opt/flink/conf/log4j-console.properties

FROM runtime-base AS local
COPY --chown=flink:flink target/risk-engine.jar /opt/flink/usrlib/risk-engine.jar

FROM runtime-base AS application
COPY --from=build --chown=flink:flink /build/target/risk-engine.jar /opt/flink/usrlib/risk-engine.jar

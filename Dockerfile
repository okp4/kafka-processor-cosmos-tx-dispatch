FROM openjdk:11-jdk-slim

COPY build/libs/kafka-processor-cosmos-tx-dispatch-*-standalone.jar /opt/kafka-processor-cosmos-tx-dispatch.jar

ENTRYPOINT [ "java", "-jar", "/opt/kafka-processor-cosmos-tx-dispatch.jar" ]

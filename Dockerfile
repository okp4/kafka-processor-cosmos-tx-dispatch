FROM openjdk:11-jdk-slim

COPY build/libs/template-kafka-processor-*-standalone.jar /opt/template-kafka-processor.jar

ENTRYPOINT [ "java", "-jar", "/opt/template-kafka-processor.jar" ]

FROM navikt/java:11
ENV JAVA_OPTS="-Dlogback.configurationFile=logback-remote.xml -Xms8g -Xmx16g"
COPY build/libs/app*.jar app.jar
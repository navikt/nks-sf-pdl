FROM navikt/java:11
ENV JAVA_OPTS="-Dlogback.configurationFile=logback-remote.xml -Xms4g -Xmx10g"
COPY build/libs/app*.jar app.jar
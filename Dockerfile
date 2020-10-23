FROM navikt/java:11
ENV JAVA_OPTS="-Dlogback.configurationFile=logback-remote.xml -Xms2g -Xmx8g"
COPY build/libs/app*.jar app.jar
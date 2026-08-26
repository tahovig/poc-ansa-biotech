FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml mule-artifact.json ./
COPY src ./src
RUN mvn -q clean package -DskipMunitTests

FROM eclipse-temurin:17-jre
ENV MULE_HOME=/opt/mule
RUN apt-get update && apt-get install -y --no-install-recommends unzip curl && rm -rf /var/lib/apt/lists/*
RUN curl -sL https://repository.mulesoft.org/nexus/content/repositories/releases/org/mule/distributions/mule-standalone/4.6.0/mule-standalone-4.6.0.tar.gz \
    | tar xz -C /opt && mv /opt/mule-standalone-4.6.0 /opt/mule
# The distribution's conf/wrapper.conf ships the JDK 11+ module flags
# (--module-path, --add-modules, --add-opens) as templated
# conf/java11-plus/wrapper.jvmDependant.conf entries with placeholder indices
# (wrapper.java.additional.<n>). Those only get merged into the resolved
# config by bin/launcher's mule-wrapper-additional-parameters-parser.jar --
# and, discovered while debugging Task 12, that merge silently drops the
# module flags whenever any -M-D... app property is also passed on the
# command line (as docker-entrypoint.sh always does here): confirmed via
# `bin/mule -M-Dfoo=bar wrapperDebug`, whose resolved Command[] list is
# missing --module-path/--add-modules entirely, so the JVM boot layer fails
# with "java.lang.module.FindException: Module org.mule.boot not found"
# before Mule's own code ever runs. Baking these same flags into
# wrapper.conf directly, as fixed (non-templated) indices appended after the
# highest one already in use (19), makes them survive that parser
# regardless of which -M-D args are passed -- verified this resolves the
# boot layer error with -M-D args present.
RUN printf '%s\n' \
    'wrapper.java.additional.20=--add-modules=java.se,org.mule.boot.tanuki,org.mule.runtime.jpms.utils,com.fasterxml.jackson.core' \
    'wrapper.java.additional.21=--module-path=%MULE_HOME%/lib/boot' \
    'wrapper.java.additional.22=--add-opens=java.base/java.lang=org.mule.runtime.jpms.utils' \
    'wrapper.java.additional.23=--add-opens=java.base/java.lang.reflect=org.mule.runtime.jpms.utils' \
    'wrapper.java.additional.24=-Dpolyglot.engine.WarnInterpreterOnly=false' \
    >> /opt/mule/conf/wrapper.conf
COPY --from=build /build/target/*.jar /opt/mule/apps/poc-ansa-biotech.jar
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh
EXPOSE 8081 8082 8083 8084
# bin/mule's wrapper resolves its "-classpath ./" entry (which must reach
# conf/) relative to the process's current working directory, not
# $MULE_HOME. Set WORKDIR so `exec` in the entrypoint always launches from
# $MULE_HOME regardless of how the container is invoked.
WORKDIR /opt/mule
ENTRYPOINT ["/docker-entrypoint.sh"]

# the idea of this dockerfile is to allow running artio build in multiple containers in order to increase the chances of a failing test locally
FROM alpine/java:21-jdk as artio-image
ENV GRADLE_OPTS="-Dorg.gradle.daemon=false -Dfix.core.debug=STATE_CLEANUP,FIX_MESSAGE,REPLAY,FIXP_SESSION,FIXP_BUSINESS -Dfix.core.ci=true"
ADD . artio-src
WORKDIR artio-src
ENTRYPOINT ./gradlew clean test

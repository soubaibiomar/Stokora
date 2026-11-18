# syntax=docker/dockerfile:1
ARG KAFKA_IMAGE=apache/kafka:4.2.2@sha256:1213eb3943d551e5ed1fca7a4e109001cee35770b66a02a0c37a8964efe09b69
FROM ${KAFKA_IMAGE} AS broker
USER root
RUN apk add --no-cache --upgrade brotli-libs=1.2.1-r0 libexpat=2.10.0-r0 \
    musl=1.2.6-r3 musl-utils=1.2.6-r3 zlib=1.4-r0
USER 1000:1000

FROM broker AS plugin
USER root
ADD --checksum=sha256:0b2ec2bb5b84dacf4501773aca754bba05c299ca2ae278985f1af260460af955 https://repo.maven.apache.org/maven2/io/debezium/debezium-connector-postgres/3.7.1.Final/debezium-connector-postgres-3.7.1.Final-plugin.tar.gz /tmp/postgres-plugin.tar.gz
RUN mkdir /plugins && tar -xzf /tmp/postgres-plugin.tar.gz -C /plugins \
    && find /plugins -type f -name 'debezium-connector-postgres-3.7.1.Final.jar' | grep -q .

FROM broker AS connect
USER root
RUN apk add --no-cache curl=8.23.0-r0 \
    && mkdir -p /kafka/connect && ln -s /opt/kafka/bin /kafka/bin
COPY --from=plugin /plugins/ /kafka/connect/
USER 1000:1000

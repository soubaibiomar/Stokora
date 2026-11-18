#!/bin/sh
set -eu
tool=$1
shift
credentials=/run/secrets/broker_admin
if [ "${STOKORA_KAFKA_ROLE:-admin}" = observer ]; then
    credentials=/run/secrets/broker_observer
fi
option=--command-config
case "$tool" in
    kafka-console-producer) option=--producer.config ;;
    kafka-console-consumer) credentials=/run/secrets/broker_observer; option=--consumer.config ;;
esac
umask 077
config=$(mktemp)
trap 'rm -f "$config"' EXIT
sed 's/^security.protocol=.*/security.protocol=SASL_SSL/' "$credentials" > "$config"
printf 'ssl.truststore.type=PEM\nssl.truststore.location=/tls/ca.pem\nssl.endpoint.identification.algorithm=https\n' >> "$config"
KAFKA_HEAP_OPTS=-Xmx192m "/opt/kafka/bin/$tool.sh" --bootstrap-server kafka:9092 "$option" "$config" "$@"

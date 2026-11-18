#!/bin/sh
set -eu
umask 077
worker=$(mktemp)
sed -e 's/SASL_PLAINTEXT/SASL_SSL/g' \
    -e 's@http://0.0.0.0:8083@http://127.0.0.1:8083@' \
    -e 's/rest.advertised.host.name=connect/rest.advertised.host.name=localhost/' \
    /opt/stokora/worker.properties > "$worker"
for prefix in '' producer. consumer. admin.; do
    printf '%ssecurity.protocol=SASL_SSL\n%ssasl.mechanism=PLAIN\n%sssl.truststore.type=PEM\n%sssl.truststore.location=/tls/ca.pem\n%sssl.endpoint.identification.algorithm=https\n' \
        "$prefix" "$prefix" "$prefix" "$prefix" "$prefix" >> "$worker"
done
exec /kafka/bin/connect-distributed.sh "$worker"

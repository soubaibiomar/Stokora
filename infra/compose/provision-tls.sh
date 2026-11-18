#!/bin/bash
set -euo pipefail
umask 077
test -z "$(ls -A /material)"
mkdir /material/postgres /material/kafka /material/trust
scratch=$(mktemp -d /scratch/ca.XXXXXX)
cd "$scratch"
openssl req -x509 -newkey rsa:3072 -nodes -days 90 -subj /CN=stokora-local-ca \
    -addext basicConstraints=critical,CA:TRUE -addext keyUsage=critical,keyCertSign,cRLSign \
    -keyout ca.key -out ca.pem
for name in postgres kafka; do
    openssl req -new -newkey rsa:2048 -nodes -subj "/CN=$name" -keyout "$name.key" -out "$name.csr"
    printf 'subjectAltName=DNS:%s\nextendedKeyUsage=serverAuth\n' "$name" > "$name.ext"
    openssl x509 -req -in "$name.csr" -CA ca.pem -CAkey ca.key -CAcreateserial \
        -days 90 -extfile "$name.ext" -out "$name.crt"
    openssl verify -CAfile ca.pem -verify_hostname "$name" "$name.crt"
done
install -o postgres -g postgres -m 600 postgres.key /material/postgres/server.key
install -o postgres -g postgres -m 644 postgres.crt /material/postgres/server.crt
cat > /material/postgres/pg_hba.conf <<'HBA'
local all all peer
hostnossl all all all reject
hostnossl replication all all reject
hostssl all all all scram-sha-256
hostssl replication stokora_cdc all scram-sha-256
HBA
chmod 644 /material/postgres/pg_hba.conf
cat kafka.key kafka.crt > /material/kafka/kafka.pem
chown 1000:1000 /material/kafka/kafka.pem
chmod 400 /material/kafka/kafka.pem
install -m 644 ca.pem /material/kafka/ca.pem
install -m 644 ca.pem /material/trust/ca.pem
chmod 755 /material/postgres /material/kafka /material/trust

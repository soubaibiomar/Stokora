# Secured local CDC profile

The intended secured deployment combines `compose.yaml`, `core.yml` and `core-tls.yml`. Spark uses `streaming.yml` followed by `streaming-tls.yml` and the project’s existing Kafka network and JAAS secret overrides. The TLS profiles require Docker volume subpath mounts. The non-TLS files alone remain the historical fixture configuration.

## Provision and start

Select a new named TLS volume. Use the same pinned PostgreSQL 16 image as the core profile to provision it. Run from the repository root in PowerShell, with the usual Compose credentials and image configuration already set.

```powershell
$env:STOKORA_TLS_VOLUME = 'stokora-local-tls'
docker volume create $env:STOKORA_TLS_VOLUME
$provision = (Resolve-Path infra/compose/provision-tls.sh).Path
docker run --rm --network none --tmpfs '/scratch:size=8m,mode=1777' -v "${env:STOKORA_TLS_VOLUME}:/material" -v "${provision}:/provision.sh:ro" --entrypoint bash postgres:16.16-bookworm /provision.sh
docker compose -p stokora --env-file config/.env.example -f infra/compose/compose.yaml -f infra/compose/core.yml -f infra/compose/core-tls.yml --profile core up -d --wait
python -m scripts.put_tls_connector --project stokora
```

PostgreSQL uses SCRAM for network sessions and peer authentication for its local Unix socket. Its network HBA rejects plaintext. Debezium is registered with `database.sslmode=verify-full` and the mounted CA. Kafka uses SASL/PLAIN inside TLS for both broker and controller listeners, and the clients verify the DNS identity `kafka`. Spark mounts only the public CA. The single-worker Connect management endpoint is loopback-only and accessed with `docker compose exec`. No listener is published on the host.

Initialize or verify the usual Kafka ACLs with `/opt/stokora/tls-client.sh` inside the broker. The migration exercise restored existing topics and ACLs. For a fresh isolated deployment, initialize topics and grants and then start/register Connect.

## Cutover and rollback

Quiesce writes, wait for Kafka and Spark offsets to agree, stop writers and verify checksummed backups of PostgreSQL, Kafka and Delta/checkpoints. Preserve the Kafka cluster/topic IDs and connector offset state. Test against a restored copy first. Keep the source volumes and the backup untouched until approval. In a failed rehearsal, stop the isolated profile and restore another copy of the checked backup. Preserve the previous Compose files, image references and connector configuration with the backup. Recover new writes from the tested secured state before considering any rollback after a real cutover.

The provisioner creates 90-day local certificates for the Compose DNS names `postgres` and `kafka`. Only the certificate and the service key enter each server subdirectory. The client subdirectory contains the public CA. The signing key is ephemeral in tmpfs, so rotation uses a new CA and keys. Plan a coordinated restart before expiry and rehearse replacing the volume and client trust together on a copy. If uninterrupted service is required, provision the new roots alongside the old root and test trust overlap before the cutover. Keep the private volume under Docker administrator custody. At-rest protection requires separate verified host/storage controls.

Security gate (archive de qualification locale)

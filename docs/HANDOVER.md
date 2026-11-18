# Developer handover

STOKORA forecasts daily shop demand in an academic synthetic environment. The operational path is PostgreSQL → Debezium → Kafka → Spark → Delta Bronze → Silver → Gold → features → seven-day baseline forecasts. A separate synthetic CSV import supplies batch Bronze. The canonical Compose project runs PostgreSQL 16, Kafka KRaft, Debezium Connect, and Spark 4.0 with Delta 4.0. Python and Great Expectations run in the development image.

[Agent policy](../AGENTS.md) · [Architecture](architecture-contract.md) · [Lifecycle guide](../reports/infrastructure/DOCKER_LIFECYCLE_GUIDE.md) · [Readiness report](../reports/audits/STOKORA_HANDOVER_READINESS.md)

## First run on another laptop

Install Git, PowerShell 7, and Docker Desktop or Docker Engine with Linux amd64 containers and Compose v2. Use a kernel with Landlock network ABI 4 or later. Give the Docker engine at least 8 GiB, use CPU execution, and allow Docker to reach Docker Hub and Maven Central during the first build. Use your organization’s approved proxy and certificate configuration where required. Runtime traffic uses a private Compose network. A new developer supplies no database credentials, existing volumes, real data, host Python, host Spark or JVM.

~~~powershell
git clone https://github.com/soubaibiomar/Stokora.git
Set-Location Stokora
docker compose -p stokora-local -f infra/compose/local.yml config --quiet
./scripts/stokora.ps1 build
./scripts/stokora.ps1 up
./scripts/stokora.ps1 status
./scripts/stokora.ps1 test
./scripts/stokora.ps1 generate
./scripts/stokora.ps1 pipeline
./scripts/stokora.ps1 forecast
~~~

Build only for setup or a reviewed change. Project name and container names are fixed. Keep the four normal containers under stokora-local. A name collision means the project may already exist on the engine. Inspect the labels and the checkout that owns it before starting. Additional qualification environments require isolated storage and explicit cleanup scope.

## Operate and inspect

| Task | Command or location |
|---|---|
| Service health | ./scripts/stokora.ps1 status |
| PostgreSQL read-only inspection | `docker compose -p stokora-local -f infra/compose/local.yml exec -T --user postgres postgres psql -X -d stokora -c "BEGIN READ ONLY; SELECT count(*) FROM operational.sales_events; COMMIT;"` |
| Logs | ./scripts/stokora.ps1 logs |
| Contract tests | ./scripts/stokora.ps1 test |
| CDC and batch reconciliation | ./scripts/stokora.ps1 generate followed by ./scripts/stokora.ps1 pipeline |
| Crash after Delta commit | ./scripts/stokora.ps1 recover |
| Forecast output | ./scripts/stokora.ps1 forecast. Output lives in the forecast volume under /home/jovyan/work. |
| Stop or planned restart | ./scripts/stokora.ps1 stop or ./scripts/stokora.ps1 restart |
| Restart verification | ./scripts/verify_local_compose.ps1 during a maintenance window |

Pipeline qualification expects eight receipts, seven mutations and one tombstone, followed by the calendar correction 6 → 4 → 6. The contract suite has 18 tests. Forecast output includes a scored population and coverage details. Fresh synthetic output is independently reproducible. Full parity with the private STEP 07 frozen data additionally requires that baseline’s approved Gold snapshot and reference files, plus matching checksums and population. The legacy Kafka outage rehearsal remains historical. The canonical recover command exercises commit-boundary crash and replay.

For a start timeout, inspect the first unhealthy dependency. Read only relevant logs and scrub secrets before sharing them. For TLS errors, verify container clocks and issuer and expiration status with the owner. Expired certificates require a planned rotation, with preservation of database and broker identity. For build registry or TLS failures on another network, resolve proxy and certificate trust on the Docker engine. For Landlock errors, use a supported kernel. For resource errors, check Docker memory and review the resource limits. Avoid disabling authentication or transport security.

## Recovery playbook

1. Establish which containers, image digests, named volumes, owner credentials and exact source identities belong to the incident. Use the reviewed Compose source. Do not publish Docker inspect output or private configuration.
2. With permission, quiesce writers and stop the services. Record the Kafka identity, connector offsets and Delta log hashes. Archive only the intended volumes and source configuration. Preserve ownership, permissions and the directory layout. Mount source volumes read-only when copying them. Record SHA-256 and a manifest of the runtime and archives.
3. Encrypt the archive before export using an approved recipient certificate and authenticated encryption. The local synthetic rehearsal uses OpenSSL CMS with AES-256-GCM. Store the recipient’s private key separately under owner-approved custody. Example Linux helper commands are below. They require a mounted stopped source, a dedicated scratch volume and authorization to use the named certificate and key. Test on synthetic data first.

~~~sh
umask 077
tar -C /source -czf state.tgz .
openssl cms -encrypt -binary -aes-256-gcm -in state.tgz -outform DER -out state.cms recipient.crt
sha256sum state.cms
openssl cms -decrypt -binary -inform DER -in state.cms -recip recipient.crt -inkey recipient.key -out restored.tgz
cmp state.tgz restored.tgz
~~~

4. Verify the encrypted artifact against the separately recorded digest. Inspect archive paths and ownership, then extract into confirmed empty isolated storage. Use the expected runtime images and test with private networking and unique names. Check PostgreSQL row state, Kafka cluster identity, consumer or connector offsets, source binding, historical Delta versions and replay results. Rehearse on a genuinely independent host before claiming off-host recovery. Obtain separate approval before restoring operational state or resetting any offsets.
5. The retired mono-runtime’s Windows DPAPI archive is bound to the original owner environment. The archive contains container configuration and recovery metadata. Data resides in the retained named volume. Use matching ciphertext and archive digest, the original decryption context, the exact retained image, and a copy of the volume. Never mount the protected source writable. A new developer’s standard workflow needs none of these private artifacts.

## Security, data and collaboration

Read [AGENTS.md](../AGENTS.md) before asking an agent to work. Use a scoped branch and follow the project’s reviewed commit and publication policy. Open a PR only after owner authorization. Keep secret values, raw scan evidence, host identifiers and runtime data outside Git. Use sanitized excerpts and scoped test results in PRs.

SEC-01 is PARTIAL, the supply-chain gate FAILS, and STEP 08 is blocked even with synthetic data. Production deployment and real M5 use require separate clearance. Current forecasts represent synthetic snapshots. A genuine point-in-time forecast needs version-level arrival and availability evidence. Look at the [issue register](../reports/audits/PRE_STEP_08_ISSUE_REGISTER.md) before planning work.

For Windows encryption evidence, identify the actual disk containing Docker Desktop’s VM disk in its settings. Confirm BitLocker or equivalent coverage for that disk, the checkout, and backup destinations. An elevated operator can run `Get-BitLockerVolume -MountPoint <drive>` and report only VolumeStatus, ProtectionStatus and EncryptionMethod. Encrypted WSL storage requires validating its backing host disk. A Linux `lsblk -f` view or runtime TLS alone cannot establish host at-rest protection. Store only sanitized attestation and keep recovery keys private.

[Handover acceptance checklist](HANDOVER_CHECKLIST.md)

## Bounded Delta disk-fault test

Run only when the synthetic development engine has spare memory. The test requires a dedicated tmpfs, writes only to that mount, and uses no persistent Docker volume. Run after the candidate image is built. Substitute the reviewed local image digest for the tag when recording evidence.

~~~powershell
Get-Content tests/integration/batch/test_disk_exhaustion.py -Raw |
  docker run --rm -i --network none --memory 2g --cpus 2 --read-only --tmpfs /tmp:rw,exec,size=512m --tmpfs /var/lib/postgresql/data:rw,size=8m --tmpfs /audit-tmpfs:rw,size=88m -w /tmp --entrypoint python stokora:local -
~~~

Expected results include an actual data-write ENOSPC, a metadata-only commit rejected at zero capacity, a readable previous version, and an idempotent append after space returns. Unreferenced files in a failed attempt can exist without a committed version.

## Certification and transfer scope

The [STEP 01–07 certification](../reports/audits/STOKORA_STEPS_01_07_CERTIFICATION.md) records where a fresh run, a retained result or an external blocker applies. A separate synthetic certification oracle proves the link from PostgreSQL-generated CDC through Gold to forecast calculations. The normal forecast command still produces the approved batch-derived benchmark. It does not automatically convert CDC into a training dataset. The [STEP 08 entry contract](STEP_08_ENTRY_CONTRACT.md) defines the gates and expected input contracts for a future owner-approved task.

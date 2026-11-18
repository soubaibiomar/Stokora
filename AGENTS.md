# AGENTS.md

## A. Project identity

STOKORA is an academic retail demand forecasting prototype. It reconciles batch sales and operational changes, then forecasts daily demand by tenant, store and product. The supported profile is local, CPU-only and synthetic. The implemented pipeline ends at statistical baselines. MLlib training, MLflow experiments, purchase decisions, approvals, public application APIs, dashboards and production operations are future work.

## B. Architecture

The fixture generator writes PostgreSQL sales. Debezium reads the logical slot and publishes Kafka records. Spark Structured Streaming binds the broker and topic identities, records durable receipts and merges unique mutations into Delta Bronze. Separately, the CSV importer atomically records batch rows, load markers and quarantine. Silver reconciles accepted data and tenant scope. Gold materializes validated daily aggregates and quality outcomes. Feature engineering and baselines consume approved Gold snapshots with chronological partitions. Read the [architecture contract](docs/architecture-contract.md), [CDC design](docs/cdc-streaming.md), [Lambda design](docs/lambda.md) and [feature dictionary](docs/ml/feature_dictionary.md).

## C. Docker invariant

The normal Compose project is **stokora-local** and has exactly four containers.

| Compose service | Container | Duty and storage | Limit |
|---|---|---|---|
| postgres | stokora-postgres | Operational rows, WAL and replication slot. Provisions scoped credential volumes. | 1 CPU, 768 MiB |
| kafka | stokora-kafka | KRaft log, cluster metadata, topics and offsets in the kafka volume. | 1 CPU, 1024 MiB |
| connect | stokora-connect | Debezium worker. Connector config and offsets reside in Kafka. | 1 CPU, 1024 MiB |
| spark | stokora-spark | On-demand CDC, batch, quality, Gold and forecasts. Named stream, batch, gold and forecast volumes. | 2 CPU, 3328 MiB |

Health dependencies order startup PostgreSQL, Kafka, Connect, Spark. Traffic stays on the internal Compose bridge. PostgreSQL uses TLS and SCRAM, Kafka uses SASL_SSL and ACLs, and the Connect management endpoint requires a client certificate. Credential mounts are scoped by role and share a trust root. Images use read-only root filesystems. A healthy unchanged Compose project is reused. Use the existing Spark container for regular jobs. A temporary qualification environment needs isolated names, fresh synthetic storage, resource bounds and an explicit cleanup scope.

## D. Development environment

Use Docker Desktop or Docker Engine with Linux amd64 containers, modern Compose v2, Git and PowerShell 7. Allocate at least 8 GiB to the engine. Offline Spark jobs require kernel Landlock network ABI 4 or later. Host Python, Java, Kafka, PostgreSQL and a GPU are unnecessary. From the repository root:

~~~powershell
./scripts/stokora.ps1 build
./scripts/stokora.ps1 up
./scripts/stokora.ps1 status
./scripts/stokora.ps1 test
./scripts/stokora.ps1 generate
./scripts/stokora.ps1 pipeline
./scripts/stokora.ps1 forecast
./scripts/stokora.ps1 logs
./scripts/stokora.ps1 stop
~~~

Build on first setup or after an approved dependency change. Read the [handover](docs/HANDOVER.md) and [lifecycle guide](reports/infrastructure/DOCKER_LIFECYCLE_GUIDE.md) before recovery, changes to dependencies or restarting existing state.

## E. Repository navigation

- infra/compose/local.yml is the canonical stack. infra/docker/Dockerfile pins the shared image and service.py dispatches services and synthetic jobs. Other Compose profiles are historical or specialized.
- scripts/stokora.ps1 wraps the canonical commands. scripts/verify_local_compose.ps1 deliberately stops and starts the stack to verify lifecycle and persistence.
- pipelines/generators, adapters, batch, streaming, storage, silver, gold, quality, features and ml contain the processing stages in dependency order.
- contracts/ holds the event contracts. tests/data has the 18 contract tests and explicitly synthetic CSVs. tests/integration and tests/ml hold storage, CDC and forecasting checks.
- docs/ contains decisions, contracts and the handover. reports/ records engineering decisions. Original acceptance evidence may be private and unavailable in a clone.
- data/ tracks documentation and safe manifests only. Runtime and restricted datasets remain local.

## F. Agent workflow

Read this file and the relevant design and issue documents. Inspect Git status and scope the task. Plan the smallest change that preserves established contracts, reuse existing entry points, then run targeted and relevant integration tests. Check security and data integrity, update documentation, record observed results and stop at approval boundaries. Keep historical evidence immutable.

## G. Git policy

Use codex/ branches for agent work unless the owner says otherwise. Keep commits focused on a reviewable milestone. Preserve other contributors’ changes and published ancestry. Preparation, local commits, merges and publication are distinct actions. Obtain authorization before committing or publishing. Avoid force-push, resets, unauthorized merges, remote changes and historical re-dating. Exclude credentials, private keys, environment files, real datasets, volumes, checkpoints, databases and private test evidence.

## H. Security policy

Retain TLS, SASL_SSL and scoped ACLs. Keep service ports private, validate input, enforce tenant constraints and preserve least privilege. Use Gitleaks for source and applicable history. Inventory and scan exact runtime digests and preserve SBOM evidence. The shared development image still carries 11 critical and 225 high scanner occurrences. SEC-01 is PARTIAL and its supply-chain threshold FAIL. Treat a fix as verified only after supported update, scan and relevant regression. Waivers, scanner suppression and production deployment require formal policy and owner decisions. An agent may never grant an exception on its own.

## I. Data governance

Synthetic data is the default. Real M5 is blocked until authorized acquisition and permitted-use evidence are documented. Preserve source identities, bytes, checkpoints and Delta versions. Wall-clock defaults or business dates cannot reconstruct what a forecast origin could know. Keep business date, arrival and genuinely observed availability distinct. Preserve tenant keys throughout joins. Test chronological splits, future-data rejection, visible revisions and replay on disposable data. Read the [handover issue register](reports/audits/PRE_STEP_08_ISSUE_REGISTER.md).

## J. Tests and acceptance

Run test, generate, pipeline and forecast as shown above. Run ./scripts/stokora.ps1 recover for post-commit crash and replay. The fixture expects 18 passing contract tests, eight Kafka receipts, seven mutations, one tombstone and Gold 6 → 4 → 6. Recheck tenant isolation, ACL denial, TLS, quarantine, approved storage and health after relevant changes. Run scripts/verify_local_compose.ps1 only during an authorized restart window. Use the [handover checklist](docs/HANDOVER_CHECKLIST.md) and [readiness report](reports/audits/STOKORA_HANDOVER_READINESS.md).

Frozen STEP 07 has 783 origins, 16,443 forecast rows and 800 scored rows per method. Naive MAE/WAPE 0.8613/21.23%, seasonal naive 0.0300/0.74%, moving average 0.7381/18.20%. Full parity requires all four reference checksums and matching scored keys. The private frozen Gold baseline may be unavailable to a new clone. Record whether evidence is fresh or retained. PASS means observed acceptance evidence, FAIL means a check ran and missed criteria, BLOCKED means a dependency prevents verification, and NOT RUN means no execution was performed. Accept only the scope actually demonstrated.

## K. Milestones

| Step | Scope | State |
|---|---|---|
| 01 | Architecture and repository baseline | Completed |
| 02 | Contracts, provenance and synthetic generator | Accepted synthetic scope |
| 03 | Operational database and secured CDC fixture | Accepted tested scope |
| 04 | Spark batch and Delta Bronze | Accepted synthetic scope |
| 05 | Streaming Bronze, identity, crash and replay | Accepted bounded fixture |
| 06 | Silver, Gold, quality and Lambda reconciliation | Accepted bounded fixture |
| 07 | Temporal features and forecast baselines | Synthetic snapshot acceptance, genuine historical availability unverified |
| 08 | MLlib and MLflow | BLOCKED, including synthetic mode |
| 09 | Decision and atomic approvals | Not started |
| 10 | Application, API and access control | Not started |
| 11 | Orchestration, BI and observability | Not started |
| 12 | Final controls, end-to-end audit and delivery | Not started |

## L. Prohibited operations

Preserve protected Docker volumes, Kafka identity, databases, Delta snapshots and original evidence. Avoid destructive actions, unauthorized real data, history rewrites, unapproved deployment and changes that bypass gates. Stop before STEP 08 until both the security gate and explicit owner approval are present. Do not claim tests, recovery or security results without evidence.

## M. Completion reporting

Report the objective, changes, files, tests and observed outcomes, security impact, remaining blockers, Git status and exact next action. State any skipped verification and material assumptions.

## Independent certification boundary

See the [STEP 01–07 certification](reports/audits/STOKORA_STEPS_01_07_CERTIFICATION.md) before extending the prototype. The documented forecast job reads the frozen-policy batch-derived Gold slice and an additional synthetic forecast fixture. A separate certification test exercised PostgreSQL-derived Gold through the same feature and baseline functions. Its operational test data does not change the approved baseline contract. Read the [STEP 08 entry contract](docs/STEP_08_ENTRY_CONTRACT.md) for prerequisites. The step remains unauthorized.

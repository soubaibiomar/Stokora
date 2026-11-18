# STOKORA handover readiness

**Scope** â€” Local synthetic development, based on the published b9d41ed unified Compose release. The running stack still uses the published image. Changes in this branch are reviewed separately from runtime promotion.

## Decisions

**READY FOR RESTRICTED DEVELOPMENT HANDOVER.** Remediation is partial. Functional work is verified in the synthetic scope. Supply-chain and external gates remain open.

| Gate | Decision |
|---|---|
| Academic synthetic demonstration | PASS on the published profile. |
| Supply-chain assurance | FAIL. Removing the unused privilege helper reduces findings and the threshold still fails. |
| SEC-01 | PARTIAL. |
| STEP 08 | BLOCKED in every mode. |
| Production | BLOCKED. Deployment authorization and security, availability and operational recovery evidence are outstanding. |
| Real M5 | BLOCKED until documented rights, provenance and owner approval. |
| Genuine historical availability | BLOCKED pending version-level source arrival and observation evidence. |

The [issue register](PRE_STEP_08_ISSUE_REGISTER.md) records priorities, proposed fixes and acceptance methods. Detailed command transcripts, SBOMs, advisory inventory, dataset-rights evidence and protected manifests remain in the separate engineering evidence store. This repository contains the operational commands and sanitized conclusions.

## Image and vulnerability findings

| Image | Result | Critical | High | Package occurrences, critical and high | Distinct IDs, critical and high |
|---|---|---:|---:|---:|---:|
| Published shared image b135920112d9 | Fresh Trivy and CycloneDX | 11 | 225 | 236 | 119 |
| Candidate removes unused gosu binary, 4b75b9e35458 | Trivy against the same advisory cache | 10 | 200 | 210 | 93 |
| Python 3.12-only experiment, ac623321cef4 | Trivy against the same advisory cache | 11 | 223 | 234 | 117 |

The live inventory found 115 critical/high occurrences carrying a scanner-listed fixed version and 121 without one. An available fix field does not establish a supported upgrade for this image. Reachability and exact-artifact applicability are unknown unless a separate test or vendor statement establishes them. No row was excluded. The candidate drops one critical and 25 high occurrences associated with the removed binary without updating shared libraries or changing service boundaries. The custom STOKORA entrypoint changed process UIDs before this patch. The inherited PostgreSQL image entrypoint was outside the supported workflow.

- **Harden the shared image.** Small, verified removals lower the installed attack surface while retaining reproducible startup and the same collation. The four services still share the imageâ€™s software footprint. A candidate image stays separate from the active runtime.
- **Split the services.** Role-specific images would reduce unrelated packages exposed to each role. Prior isolated Kafka and Connect candidates scanned at 0/0. Their results apply to those exact images. A split needs a deliberately designed credential bootstrap and coordinated persistence, resource, startup and CDC tests. PostgreSQL and Spark blockers would remain.
- **Use vendor replacements.** Apache Spark 4.0.5 is the maintenance release for the current 4.0 branch. A cross-minor Spark upgrade needs an explicitly compatible Delta and Kafka connector set, checked against frozen parity. The active Bookworm PostgreSQL collation contract and stored slot must survive any migration. [Spark release](https://spark.apache.org/releases/spark-release-4-0-5.html) Â· [Delta compatibility](https://docs.delta.io/releases/) Â· [Debian libxml2 advisory](https://security-tracker.debian.org/tracker/CVE-2026-6653)
- **Retain active pins pending clearance.** This preserves the accepted deployment. It leaves open Debian libxml2 and SQLite, Spark or bundled JAR findings, and the supported interpreter migration. Netty, Derby, Avro and Bouncy Castle scanner fixes need vendor-supported bundles and full regression. An arbitrary JAR swap does not establish compatibility.

The live Python probe found CPython 3.10.22 and reported Expat 2.8.5. pyexpat has no dynamic libexpat dependency in the checked image. The historical Jammy report concerned system Expat 2.4.7 and an older interpreter. The live pair meets Pythonâ€™s documented component version conditions for CVE-2026-7210. CPython 3.10 itself is end-of-life. GX 1.19 requires Python below 3.14 and the current wheel hashes include native wheels tied to the interpreter. [Python release and support](https://blog.python.org/2026/10/python-31022-31117/) Â· [Python changelog](https://docs.python.org/3.10/whatsnew/changelog.html) Â· [Ubuntu advisory](https://ubuntu.com/security/CVE-2026-7210)

The official Python 3.12 Bookworm candidate resolved all 38 pinned wheels for the different interpreter and built with wheel hash enforcement. It reported CPython 3.12.16 with Expat 2.9.0, imported GX, NumPy, pandas and PySpark 4.0.5, and produced a separate SBOM. The experiment changes only the Python base and lock. It is separate from the gosu removal and is outside the committed runtime update. A combined image requires its own pin review, exact-digest vulnerability scan and acceptance before owner-approved adoption.

## Verification

| Check | Status | Observation and scope |
|---|---|---|
| Published checkout and Docker portability | PASS | A clean public clone built the pinned runtime, started all four services on a private isolated qualification project with fresh synthetic storage, passed 18 contracts, generated and reconciled CDC, forecast, recovered after a crash and reused all four IDs across restart. The test used the same Compose file with temporary project, container names and image tag to avoid the live stack. Temporary containers and network were removed. |
| Shared candidate without gosu | PASS | Exact candidate passed four-service health, all 18 contracts, CDC and Gold, forecast, post-commit recovery, TLS/ACL negative checks, stable restart IDs and bounded ENOSPC. Frozen parity again matched all four hashes. The candidate reused only volumes created for this qualification. |
| Python 3.12-only experiment | PASS | Separate exact digest passed four-service readiness, 18 contracts, CDC/Gold, forecast and post-commit recovery. Exact frozen parity for this experiment remains NOT RUN. Its security threshold failed. |
| Published stack isolation | PASS | Fresh role health and TLS checks confirmed hostname validation and rejected PostgreSQL plaintext and a bad password. Kafka refused the observer access to quarantine. Current project uses no host port bindings. |
| Delta capacity failure | PASS | Read-only disposable container with an 88 MiB tmpfs. Parquet and metadata log writes failed at capacity, including a zero-byte availability check for the metadata commit. No new commit became visible. Previous version was still readable. Two unreferenced Parquet files were observed. A restart after restoring capacity committed once across replay, with the same five keys and readable version zero. |
| Archived runtime | PASS | Original owner DPAPI context decrypted the archive and its digest matched. The verified configuration and pinned archived image were used with a private copy of the stopped state volume. PostgreSQL, Kafka and Connect became healthy. The seed replay was idempotent. CDC receipts and three Gold snapshots matched the earlier run. The original volume was mounted read-only during copying. |
| Encrypted local backup | PASS | A bounded synthetic state backup encrypted with OpenSSL CMS AES-256-GCM, decrypted with the recipient key and restored into empty storage. Archive and restored data bytes matched the source. Runtime and source-identity checks ran on the restored copy. Temporary scratch and keys belonged to the isolated helper. |
| Actual off-host restore | NOT RUN | The original host ran the local exercise. An independent target, authorized storage and separate recipient key custody are required. |
| Temporal adapter hardening | PASS | Explicit schema and nonnegative integral revisions, with UTC, tenant, ambiguous revision, late arrival and invalid type regression tests. This synthetic adapter validates source-supplied metadata and does not reconstruct historical availability. A forward ingest contract must retain separately event time, genuine arrival time, availability time, revision/source identity, state and source clock provenance. Reject unverifiable availability for real point-in-time runs. |
| Frozen STEP 07 parity | PASS | Fresh protected-fixture copy and comparison, 783 origins and 16,443 forecasts. Four table hashes matched. 800 scored rows per method. Naive 0.86125/21.23%, seasonal 0.03/0.74%, moving average 0.73806/18.20%, MAE/WAPE. |
| Source volumes | PASS | All 74 protected historical hashes were verified read-only. |
| Host encryption | BLOCKED | Non-elevated BitLocker query returned access denied. The required evidence covers the Docker backing disk, workspace and backup targets. Provide sanitized elevated attestation. |
| M5 rights | BLOCKED | Public competition and organizer materials identify provenance. They did not establish a verified permission record for this projectâ€™s scope and copy. Obtain dated terms and an authorized acquisition record or written permission. No data access in this review. [Organizer materials](https://github.com/Mcompetitions/M5-methods) Â· [Competition rules](https://www.kaggle.com/competitions/m5-forecasting-accuracy/rules) |
| Older Kafka outage rehearsal | NOT RUN | The canonical crash recovery is newly tested. A sustained fault and outage campaign is a separate historical workflow. |
| Gitleaks | PASS | Staged diff, checkout and candidate ancestry scans found no leaks. |
| New agent and developer material | PASS | Agent guide, onboarding and checklist were reviewed against named services, executable entry points, tested commands and the roadmap. |

A point-in-time resource observation during the isolated candidate workload was approximately 46 MiB for PostgreSQL, 459 MiB for Kafka, 316 MiB for Connect and 1.06 GiB for Spark. This was a sample, not a matched startup or performance benchmark.

The milestone is prepared on codex/handover-readiness. Public main remains at b9d41ed. The raw audit logs, exploratory Python 3.12 build and generated SBOMs stay in the local evidence store. The milestone includes the qualified removal, synthetic availability guard, bounded tests and sanitized guides.

## Owner decisions

1. Decide when to qualify and approve a supported interpreter and replacement image for the active stack. Retain the current security threshold and obtain vendor-supported fixes for remaining findings.
2. Provide independent restore infrastructure and approved key custody, plus handover of authorized private evidence where applicable.
3. Provide sanitized host-encryption attestation and genuine historical source availability where required. Obtain verified M5 rights separately before any acquisition or use.
4. Review the local handover milestone before authorizing publication. Reopen STEP 08 only after its gate passes and the owner explicitly authorizes the step.

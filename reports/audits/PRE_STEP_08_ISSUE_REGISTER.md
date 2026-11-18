# Pre-STEP-08 issue register

The baseline is the published unified Compose milestone b9d41ed. Statuses apply to the named scope. The associated [handover readiness review](STOKORA_HANDOVER_READINESS.md) distinguishes new tests and retained results.

| Priority | Issue and evidence | Fix and acceptance method | State |
|---|---|---|---|
| P0 | Shared image scanned with 11 critical and 225 high occurrences. | Qualify signed replacements against exact runtime digests, SBOM, TLS and ACL checks, data continuity, resource limits and unchanged STEP 07 parity. Keep all unknown rows in the gate. | FAIL, SEC-01 PARTIAL |
| P0 | Official Bookworm libxml2 and SQLite findings, PostgreSQL collation constraints. | Use provider-supplied fixes or separately qualify a supported migration. Assert exact collation, slot and row identity. Keep the active image while blocked. | BLOCKED for promotion |
| P0 | Python 3.10.22 is end-of-life. The live parser reports Expat 2.8.5. The older Jammy Python/Expat finding is a different artifact. | Qualify a supported official interpreter, compatible Expat, new hashes for native wheels, GX and Spark/Delta regressions. The 3.12 candidate passed scoped runtime checks and failed supply-chain clearance. | OPEN for adoption |
| P0 | Real M5 source terms and authorized acquisition remain unresolved. | Obtain dated, scope-specific terms from the organizer or authorized rights holder and record provenance before handling data. | BLOCKED |
| P0 | Historical Gold has incomplete arrival and revision visibility. | Keep synthetic validation strict. Obtain genuine source arrival and version metadata before claiming real historical parity. | BLOCKED externally |
| P0 | Delta active write and log-commit disk exhaustion. | Use a capped tmpfs, force actual Parquet and metadata failures, verify table history, retry idempotently after capacity restoration. | PASS for bounded synthetic test |
| P1 | Retired archive and local backup restore. | Verify DPAPI archive digest and restore an encrypted backup of the stopped volume into isolated synthetic storage. Check readiness, Kafka and CDC/Gold replay. | See readiness review |
| P1 | Off-host recovery lacks an independent destination. | Obtain a separate host, backup destination, secure key custody and approval. Reconcile results after full restore. | NOT RUN |
| P1 | Host encryption query denied by Windows. | Confirm protection of the storage that backs Docker and backup targets using owner-provided elevated attestation. | BLOCKED |
| P1 | Legacy Kafka outage workflow is historical. | Keep the canonical post-commit test. Rehearse a distinct isolated outage when the requested scope includes it. | Retained evidence, fresh outage NOT RUN |
| P2 | Availability schema accepted coercible dates, malformed or negative revision keys. | Reject ambiguous types and negative revisions before point-in-time joining. Test UTC boundaries, tenant, late corrections and invalid inputs. | PASS on synthetic fixture |
| P2 | Fresh developer setup and prior private evidence boundary. | Build and run a fresh checkout with no borrowed volumes. Document private frozen artifacts and prerequisites. | See readiness review |
| P3 | Agent and developer onboarding lacked a root handover. | Add agent rules, entry-point commands, operational guide and checklist. Check links and commands. | Covered by this milestone |

## Independent STEP 01–07 certification

The independent review retained the SEC-01 PARTIAL and STEP 08 BLOCKED gates. It distinguished the supported benchmark workflow from a separate PostgreSQL-to-forecast synthetic oracle. The Gold synthetic product allowlist and published forecast input scope remain explicit. [Certification and requirements evidence](STOKORA_STEPS_01_07_CERTIFICATION.md) · [STEP 08 prerequisites](../../docs/STEP_08_ENTRY_CONTRACT.md)

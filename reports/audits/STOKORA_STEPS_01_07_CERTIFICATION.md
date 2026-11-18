# STOKORA independent certification of STEPS 01–07

Scope is the synthetic four-service development pipeline through statistical forecast baselines. Candidate source is 8cc99066 and published main is b9d41ed. Tests used an isolated four-service project with fresh named volumes and the existing reviewed handover candidate image. The running stokora-local project was inspected read-only. The audit built no replacement image and never used real M5.

## Source traceability

The original STEP_01_PROMPT.md through STEP_07_PROMPT.md, the requirements and design PDFs, six ADRs, seven original step reports and 70-control security reference were available. Each individual acceptance report maps the original §4 mandatory checks, implementation, expected and actual outcomes, supporting evidence and scope gaps. The original prompts and private benchmark snapshots are **MISSING from a public clone**. They were available from the local engineering archive for this review. The published repository and standardized synthetic fixtures are sufficient to run its supported demo with the documented Docker prerequisites.

| STEP | REQUIREMENTS | TESTS | EVIDENCE | DEFECTS AND GAPS | VERDICT |
|---|---|---|---|---|---|
| 01 | Repository baseline, ADRs, static checks, secret boundaries and bounded Compose | Original-pack verifier, preflight, fake-secret and Git scans | [STEP 01](STEP_01_ACCEPTANCE.md) | Future controls remain design-level | PASS |
| 02 | Provenance, canonical adapter, deterministic generator, event schemas | 18 contract tests and fresh seeded generation | [STEP 02](STEP_02_ACCEPTANCE.md) | Real acquisition remains blocked outside synthetic acceptance | PASS |
| 03 | PostgreSQL/Debezium/Kafka CRUD, snapshot, security and recovery | Real mutations, connector outage, broker recovery, health, auth and refusal checks | [STEP 03](STEP_03_ACCEPTANCE.md) | Non-empty initial snapshot case uses retained pinned evidence | CONDITIONAL PASS |
| 04 | Queryable Bronze, bounded performance, provenance, atomic inputs and replay | Importer regression, invalid/empty inputs, cold/warm 500/1,000 series, CLI limits | [STEP 04](STEP_04_ACCEPTANCE.md) | Bounded fixtures and local writer only. No real-volume claim | PASS |
| 05 | Streaming ledger, checkpoint, normalization, quarantine and incarnation protection | Crash/replay, malformed Kafka input, fail-closed outage and source mismatch | [STEP 05](STEP_05_ACCEPTANCE.md) | One partition and local storage. Retained non-empty snapshot evidence | CONDITIONAL PASS |
| 06 | GX, reconciliation, quarantine, tenant access and versioned Gold | Calendar 6 → 4 → 6, independent insert/correction/delete oracle, row-limit and denied tenant checks | [STEP 06](STEP_06_ACCEPTANCE.md) | Authorized overlap restricted to its qualified fixture. Operational product mapping is allowlisted | CONDITIONAL PASS |
| 07 | Pinned inputs, temporal windows, horizons, metrics and leakage | Fresh frozen checksum comparison, anti-leakage suite, second oracle and scoring keys | [STEP 07](STEP_07_ACCEPTANCE.md) | Genuine historic availability and production dimensions unverified. Official benchmark command selects its scoped inputs | CONDITIONAL PASS |

PASS means the declared synthetic acceptance criteria were observed. CONDITIONAL PASS means scope passed with disclosed retained evidence or nonblocking limitations. FAIL means an executed mandatory check missed its criteria. BLOCKED means a dependency or approval is absent. NOT VERIFIED means evidence is insufficient. These findings do not establish generalized engineering readiness for real-data or multi-writer workloads, security acceptance or production readiness.

## FULL_PIPELINE_ACCEPTANCE

**PASS for the isolated PostgreSQL-derived synthetic certification oracle.** The normal Compose job separately passed the named calendar and approved forecast fixture. It intentionally reads Gold version 2 for SIM_M5 plus the long synthetic forecasting fixture. This remains the published benchmark policy.

| Transition | Independent expected and observed |
|---|---|
| Synthetic source → PostgreSQL | Seeded standard workflow wrote committable PostgreSQL mutations. A separate SQL scenario created 27 accepted sales across February days 1–28 except day 22, plus one same-store/product observation for tenant B. During Kafka outage, the test updated day 21 from 21 to 30 and deleted day 19. |
| PostgreSQL → Debezium → Kafka | Real rows written while Connect was stopped were delivered after restart. Kafka was stopped separately, the consumer failed closed, and queued correction/delete were delivered after recovery. The full bounded broker fixture ended at offset 72, including the standard, rejected-product and positive cases. |
| Kafka → Spark CDC Bronze | Continuous offsets 0–71. Accepted oracle event classification and payload values, LSN, event/receipt identity and times matched. Three tombstones and two intentionally malformed payload receipts across the cases were accounted for. The fresh replay processed offsets 0–39 without a commit. The later replay reused its checkpoint and processed offsets 40–71 without another commit. The foreign-cluster test failed before checkpoint creation. |
| Bronze → Silver/Gold | The approved calendar fixture reproduced 6 → 4 → 6. The first independent product was outside the allowlist and quarantined. The positive case used the permitted product with a distinct month. Two source versions produced exact per-day current state, distinct Gold identities, GX quality results and the same identity on rebuild. A tenant-B request under the A grant failed. |
| Gold → features → forecasts | The oracle selected operational Gold under the tenant-A grant and ran the production feature and baseline functions. It produced three methods × seven horizons. Day 22 was missing, so six labels per method scored against 153 units. Absolute errors 27, 34 and 37 matched an independent calculation. The seasonal fallback used the last observation when the prior-week date was missing. Feb 22 test-origin windows remained bounded. |

The PostgreSQL-derived oracle is a certification test and does not widen the supported forecast command or authorize operational training. A harness mount error and the independent test’s unsupported-product assumption were corrected. The first batch attempt used the wrong temporary backing path. The first Gold scenario correctly exposed the hard-coded fixture product allowlist, and that rejection is now explicitly tested. No confirmed defect in production code was found. Tests and documentation were added with frozen expected values unchanged.

The isolated tests’ source setup and exact command outputs are retained in the engineering evidence directory docs/evidence/steps-01-07-certification. Raw transcripts and private reference data are outside the publication branch. The maintained independent assertion is tests/integration/lambda/certify_cdc_forecast.py. The preceding handover clean clone qualification was retained, and this certification freshly ran the reviewed image with no reused test volumes. Container startup, initial pipeline, stop/start and repeated run kept the same IDs in the isolation project. Temporary containers and the network were removed after validation. The named synthetic certification volumes were retained for evidence. The active stack IDs, image and health matched their opening inspection.

## Forecast and security

| Method | MAE | WAPE |
|---|---:|---:|
| Naive | 0.86125 | 21.2327% |
| Seasonal naive | 0.03 | 0.7396% |
| Moving average | 0.73806 | 18.1955% |

The fresh pinned benchmark reproduced 783 origins, 16,443 forecast rows and 800 scored rows per method. Feature, forecast, label and coverage SHA-256 checks all matched. A public clone can generate its synthetic benchmark; the private historical snapshot is a dependency for the exact frozen file hashes.

- Deployed b135920112d9 has 11 critical and 225 high findings. Candidate 4b75b9e35458 has 10 critical and 200 high findings. These are retained scan results tied to the exact images verified in this review. The candidate was not promoted. SEC-01 PARTIAL, supply-chain FAIL and production authorization BLOCKED.
- All 74 historical protected files matched in a fresh read-only hash check. Git history and scoped Gitleaks scans found no leaks. The scanner detected a redacted manufactured secret outside Git.
- Retained bounded disk exhaustion and local encrypted restore are PASS in their tested scopes. Actual off-host restore remains NOT RUN. Host encryption attestation, real M5 authorization and genuine historical observation availability remain outstanding.

## Handover and owner decisions

**READY FOR RESTRICTED SYNTHETIC DEVELOPMENT HANDOVER.** Agent instructions, repository navigation, the Docker command map and documented scope were checked. The public workflow is reproducible under the stated Docker and kernel prerequisites. Access to published and private benchmark data is a separate decision.

1. Acknowledge synthetic-only handover and give the next developer access to the reviewed branch. Authorize GitHub publication separately if needed.
2. Approve candidate adoption only after the exact proposed digests pass the applicable security and regression gates. Provide approved host attestation and independent restore infrastructure where required.
3. Obtain real M5 rights and provenance, or maintain synthetic-only scope. Obtain genuine version/availability data for any real point-in-time claim.
4. Approve STEP 08 only after applicable gates pass and the owner accepts the [entry contract](../../docs/STEP_08_ENTRY_CONTRACT.md). STEP 08 remains **BLOCKED and NOT AUTHORIZED**.

[Agent instructions](../../AGENTS.md) · [Handover](../../docs/HANDOVER.md) · [Issue register](PRE_STEP_08_ISSUE_REGISTER.md)

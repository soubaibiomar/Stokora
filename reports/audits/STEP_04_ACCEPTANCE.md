# STEP 04 independent acceptance

**Verdict — PASS for bounded synthetic batch.**

Source: original STEP_04_PROMPT.md §4 lines 30–38, STEP 04 report and remediation.

| Requirement | Code and check | Expected → observed |
|---|---|---|
| Queryable Delta and provenance | batch/import_m5.py, storage/bronze.py, integration/batch/verify_bronze.py | Bronze rows and load manifests queryable with checksums and source lineage. R07 preserved 31 rows, seven loads and versions 2, 1, 0. |
| Identical replay and duplicate prevention | verify_bronze.py R06–R08 | Identical inputs keep commit history. Malformed legacy success marker cannot bypass validation. |
| Error/atomicity controls | R01–R05, R09 plus earlier exact-image disk test | Invalid, empty, malformed and header-only files rejected. Failed inputs kept rows, provenance and history. Retained bounded Parquet and log ENOSPC evidence shows readable prior version and idempotent retry. |
| Fixture volume and limits | verify_bronze.py on Spark 4.0.5 | 500 and 1,000 synthetic series yielded 139,413 and 276,913 Bronze raw rows. Cold/warm wall times 15.882/6.371 s and 22.562/7.719 s. Benchmark CLI rejected 499 and 1,001 by design. These are benchmark sizes, not importer limits. |
| Licensed path | m5 adapter and batch preflight | Synthetic path works from committed files. Unauthorized real mode refuses before import. |

Evidence: batch.txt fresh. First isolated harness invocation lacked the /home/jovyan/work backing mount. The corrected temporary mount passed without application changes. Bounded performance does not establish real-data performance.

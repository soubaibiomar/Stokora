# STEP 02 independent acceptance

**Verdict — PASS for synthetic contracts and ingestion foundation.**

Source: original STEP_02_PROMPT.md §4 lines 30–37 and STEP 02 report.

| Requirement | Code and check | Expected → observed |
|---|---|---|
| Provenance and reproducible profiling | data/manifests/m5-fixture.json, adapters/m5.py, test_step02.py | File hashes and profiling support a known synthetic fixture. Contract suite passed. |
| Canonical grain, joins and counts | adapters/m5.py and fixture contract tests | Multiple series normalize to source/store/product/day without joining unrelated datasets. Malformed references and rows are rejected or quarantined. |
| Generator determinism and identities | generators/events.py, transactions.py, contract suite | Repeated seed reproduces output. The real PostgreSQL certification pipeline explicitly used that synthetic seed. Cross-tenant identities stay distinct. |
| Mutations and negative data controls | contracts/events/v1.schema.json and 18 contracts | Creation, correction, cancellation, invalid fields, simulation semantics and duplicate guards passed. The real M5 guard remains fail-closed. No commercial CSV is tracked or used. |

Evidence: workflow.txt for fresh contracts and generation, batch.txt for importer checks, and tracked-file review. Real acquisition, licensing and new-source qualification remain separate gates.

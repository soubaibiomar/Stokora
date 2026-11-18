# STEP 05 independent acceptance

**Verdict — CONDITIONAL PASS for one bounded source, one partition and a local Delta writer.** The non-empty initial snapshot case uses retained evidence from the earlier authorized fixture.

Source: original STEP_05_PROMPT.md §4 lines 30–38 and STEP 05 independent review.

| Requirement | Code and check | Expected → observed |
|---|---|---|
| Real CDC Bronze | consume_cdc.py, source.py, merge_bronze.py and isolated workflow | Eight receipts, seven mutations and one tombstone in the standard scenario. A separate real PostgreSQL case reached 72 durable receipts with continuous offsets. |
| Mutation and quarantine semantics | Kafka payload, Bronze receipt and normalized mutation | Create, update, delete and tombstone check fresh. Malformed JSON preserved provenance in quarantine. Snapshot operation r is supported and covered by retained non-empty snapshot evidence. |
| Crash and duplicate prevention | recover, original checkpoint, fresh offset replay | Injected exit 73 left durable commit. Original-checkpoint recovery and fresh empty-checkpoint replay preserved both commit hashes. The longer independent replay consumed the remaining 32 records from its persisted replay checkpoint with committed=false. |
| Source incarnation and monitoring | binding and progress metadata | A mismatched cluster identity failed before checkpoint creation. Progress and cgroup peak were observed. |
| Network failure | Stop isolated Kafka, run bounded consumer, resume | Consumer failed closed with cdc_source_lookup_failed. After restart the queued update/delete arrived and the corrected Bronze ledger remained replay-safe. |

Evidence: workflow.txt and scenario.txt and scenario-retest.txt. Local replay is bounded to the tested transaction, guard and storage model. Multi-writer and failover guarantees are outside it.

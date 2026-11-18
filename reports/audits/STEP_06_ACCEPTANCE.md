# STEP 06 independent acceptance

**Verdict — CONDITIONAL PASS for the synthetic reconciled lakehouse.**

Source: original STEP_06_PROMPT.md §4 lines 30–38 and STEP 06 review and remediation.

| Requirement | Code and check | Expected → observed |
|---|---|---|
| Silver uniqueness, quality and quarantine | silver/reconcile.py, quality/silver.py, gold/publish.py | The named calendar flow reproduced 6 → 4 → 6. The independent scenario built from CDC verified published Great Expectations outcomes and retained an invalid record in quarantine. |
| Idempotent reconciliation and corrections | Gold publishing and independent oracle | The independent insert/update/delete sequence matched per-day quantities before and after correction, preserved the prior snapshot, and reused identity on repeated rebuild. |
| Grain and simulation labels | Gold mart definitions and input contracts | Daily mart grain matches tenant/site/product/dataset/day. Stock coverage and thresholds use explicit synthetic assumptions. |
| Anti-leakage and duplicate overlap | gold_view denial and earlier mapping qualification | Fresh same store/product data for tenant B was excluded from A. Unauthorized tenant lookup failed. Authorized overlap replay was previously qualified against the approval-bound fixture. Arbitrary new overlaps remain denied. |
| Invalid inputs and bounded fixtures | Batch negatives and Gold read_source | Malformed rows do not become feature inputs. The isolated oracle accepted 10,000 Bronze rows and rejected 10,001. |

Evidence: workflow.txt, scenario.txt and the retained STEP 06 remediation review. The source-snapshot approval and overlap proofs concern synthetic keys. Application authentication and production-scale joins require separate qualification.

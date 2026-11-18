# STEP 07 independent acceptance

**Verdict — CONDITIONAL PASS for the synthetic snapshot and frozen baseline.**

Source: original STEP_07_PROMPT.md §4 lines 30–37, STEP 07 report, feature dictionary and approved frozen reference.

| Requirement | Code and check | Expected → observed |
|---|---|---|
| Partition and cutoffs | features/forecast.py and leakage suite | Keys include tenant, dataset, site, product and origin. Calendar lags use dates before origin. Targets crossing chronological split boundaries are excluded. Future perturbation did not alter past features. |
| Availability and provenance | approved_sales, run manifest, availability adapter | Read a pinned Gold commit and distinct second fixture with stable seed/hash. Synthetic revision availability, late corrections and tenant keys passed. Genuine historical availability and real category dimensions are unverified. |
| Horizons, fallback and missing labels | baseline functions and independent real-CDC oracle | Seven horizons × three methods. Exactly six scored labels per method with a missing day. Seasonal fallback correct. Independent absolute-error totals 27, 34 and 37 for naive, seasonal and moving average, over 153 actual units. WAPE and MAE matched. |
| Frozen files and comparable evaluation | protected reference comparator, coverage and scoring tests | 783 origins, 16,443 rows, 800 scored per method and matching four reference hashes. All methods share the scored key population. Baseline MAE/WAPE matches the accepted reference. |
| No synthetic inventory substituted for M5 | feature schema, run manifests and Gold policy | Forecast inputs are simulated sales. Missing genuine historical categories, prices and other business fields remain bounded by provenance. No STEP 08 fit or metric claim. |

Evidence: final-forecast.txt, test_leakage.txt, test_pre_step08.txt and scenario.txt. The future training gate remains closed.

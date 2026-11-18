# STEP 08 entry contract

**NOT AUTHORIZED.** This document specifies the inputs and acceptance rules for a future owner-approved implementation. SEC-01 remains PARTIAL and its supply-chain threshold FAILS. Acquiring real M5, deploying a candidate, starting training, and publishing to GitHub are separate gated actions.

## Accepted source and provenance

The candidate benchmark scope uses approved synthetic data only. Pin the Gold Delta path, numeric version and commit identity. Reconcile the Delta history userMetadata entry with the quality row for the same snapshot. Verify allowed tenant, dataset namespace, unit and simulated flag at the read boundary. Record both Bronze table identities and versions, source/stream identity, policy digest, snapshot SHA, source fixture hashes, seed, code revision and dependency/image digests in a run manifest. The published baseline adapter selects SIM_M5 and m5_fixture at Gold version 2 and adds independently identified simulator:step07-v1 rows. A standard clone generates its own Gold provenance. Full frozen reproduction requires access to the approved historical snapshot and reference files.

Gold sales_daily grain is (dataset, tenant, site, product, simulated, day) with observed unit sales quantity and publication/source provenance. Preserve tenant and dataset across every join. Calendar, price and category history require validated relationships and version-level availability. Synthetic inventory or supplier facts cannot be supplied as observed M5 inputs. A future operational CDC forecast needs an explicitly approved tenant and snapshot contract. An isolated synthetic oracle does not add a supported CDC training data product.

## Feature and label contract

- Feature row key is (tenant, dataset, site, product, origin). Origin is midnight UTC. One row per eligible series and date. Baseline rows include lags at 7, 14 and 28 days, prior_week, 7/28-day means, population standard deviation, trend, observed/zero/missing day counts, zero fraction, last observed value and historical price where availability is supported. Store simulated=true, unit=each and run_id.
- Label row uses the same identity plus split, a seven-element actuals array, and unit_sales_next_7d. A missing observation stays null and makes the weekly total null. Genuine observed zeros stay zero. Never include target fields, label availability or post-origin corrections as features.
- Forecast rows add method, horizon 1–7 and target_date = origin + horizon − 1. Horizon calendar fields can be derived deterministically. Publish coverage and fallback separately. All contenders must score the same eligible keys, and reports must distinguish issued, scored, after-history and missing-date cases.
- Baseline windows are fixed in 2024. Eligible training origins start Jan 6, validation starts Apr 1 and test starts May 1, with May 31 as the last target date. Drop origins with targets crossing split boundaries. An origin must be later than that series’ first observed date. Fit transformations only on the training partition for each fit or fold, tune on validation, then score the locked test once model selection is fixed.
- Snapshot-valid features use strictly earlier business days. Genuine point-in-time claims require immutable source event, arrival and availability times plus revision and identity. Use only the revision knowable by origin, account for later retractions after availability, and reject ambiguous, missing or reconstructed clocks. Historical availability of approved Gold remains unverified.

[Feature dictionary](ml/feature_dictionary.md)

## Frozen benchmark and entry checks

783 origins, 16,443 daily forecast rows and 800 scored rows per method. Naive MAE/WAPE 0.86125/21.2327%, seasonal naive 0.03/0.7396%, moving average 0.73806/18.1955%. Preserve the four reference checksums and the exact test keys. Compare MAE, WAPE, signed bias, negative rate and coverage overall and by series/store. WAPE is null when the actual-unit denominator is zero. Category reporting requires an approved dimension. A model only qualifies when it meets the owner-agreed comparison and resource goals on the same population, with fallback, latency and tradeoffs documented.

## Required controls before future implementation

1. Obtain an accepted STEP 01–07 handover for the scope used. Qualify reviewed dependencies and exact digests until supply-chain/security criteria pass. Retain isolation, least privilege, TLS, ACLs, source identity, protected hashes and secret scanning.
2. Obtain explicit owner authorization to begin STEP 08 and approve the synthetic datasets, model scope, time budget, tracking environment and metric thresholds. Request separate rights/provenance approval for real data if proposed.
3. Within the approved task, record deterministic seeds, code and package versions, feature allowlist, split manifest and pinned snapshot. Use chronological validation only. Fit preprocessing inside training. Preserve immutable held-out labels, avoid test tuning and evaluate identical eligible keys. Record errors, null handling, fallback and group coverage. Keep tests for future perturbation and tenant separation.
4. The future MLflow run must track parameters, stage, data/feature/run identity, dataset and code hashes, exact runtime digests, metrics, timing, resource usage, serialized pipeline/model, prediction/coverage manifests and reproducibility details. Use an approved authenticated backend and private artifact storage. Keep credentials and raw data out of logs and artifacts.
5. Require owner review of the head-to-head comparison, failure modes, operational limits and security before promotion. This contract creates no experiment, training code, or production authorization.

[Issue register](../reports/audits/PRE_STEP_08_ISSUE_REGISTER.md)

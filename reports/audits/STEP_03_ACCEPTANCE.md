# STEP 03 independent acceptance

**Verdict — CONDITIONAL PASS for the secured synthetic CDC stack.** A non-empty initial snapshot was directly observed in the previous pinned STEP 03 smoke test and retained for this review. The new fresh stack started with an empty sales table.

Source: original STEP_03_PROMPT.md §4 lines 30–38 and STEP 03 report.

| Requirement | Code and check | Expected → observed |
|---|---|---|
| Bounded Compose operational stack | infra/compose/local.yml, service.py and isolated workflow | PostgreSQL, Kafka and Connect became healthy. Clean stop/start reused IDs and preserved state. |
| Capture and metadata | migrations, Debezium connector, streaming receipts | True PostgreSQL inserts and subsequent update and delete were captured. The former smoke log observed operation r with an LSN at offset zero. The current Oracle reconciles partition, positions, event keys, LSN/time and tombstone. |
| Restart continuity | Stop Connect, write 28 rows, restart, then test broker interruption | Offset advanced to 36 only after connector recovery. A correction and delete made while Kafka was unavailable arrived by offset 39. |
| Least privilege and transport | PostgreSQL roles, dedicated TLS and ACL checks | Replication slot active. Role could read sales and lacked superuser, role/database admin, and insert privilege. TLS identity validation and invalid/plaintext login denials passed. Kafka rejected the observer on quarantine. Connect refused management without a client certificate. No host port bindings. |

Evidence: workflow.txt, scenario.txt, postgres-roles.txt, tls-acl.txt, connect-no-cert.txt, retained STEP 03 smoke.txt. This does not establish production WAL capacity or availability.

# Handover checklist

## New checkout

- [ ] Clone the intended revision. Compare Git HEAD and inspect AGENTS.md and the issue register.
- [ ] Verify Docker publishes Linux amd64, Compose v2, at least 8 GiB available and working Landlock support.
- [ ] Run the configuration check from HANDOVER.md. Confirm the project is stokora-local and names exactly four services.
- [ ] Build, start and check status. Confirm four healthy services and zero host port bindings. An unbound container port shown under PORTS is image metadata.
- [ ] Run test, generate, pipeline and forecast. Verify 18 contracts, eight receipts, seven mutations, one tombstone and 6 → 4 → 6.
- [ ] Run recover. Confirm the injected exit 73 and preservation of the two Delta log hashes after checkpoint and fresh replay.
- [ ] During an approved restart window run verify_local_compose.ps1. Confirm the same four IDs, same synthetic seed and replay results, and preserved volumes. Use stop for normal shutdown.
- [ ] Confirm forecast manifests and scored coverage. Mark private frozen parity unavailable if the reference snapshot is inaccessible.

## Safety and collaboration

- [ ] Use an isolated synthetic target for any destructive or capacity fault rehearsal. Preserve protected volumes, archive contents and broker identity.
- [ ] Scan the exact image digest and run Gitleaks on the intended source. Record what was scanned, test exits, evidence scope and blockers.
- [ ] Verify existing protected manifests on a host that owns the corresponding volumes. A clean clone has no historical volumes to verify.
- [ ] Confirm individual authorization before changes to security gates, existing environments, source data, Git history, publication or production systems.
- [ ] Keep real M5 restricted and STEP 08 blocked. Record genuine availability as unverified until authoritative version history exists.
- [ ] For off-host recovery, obtain an independent target and approved key custody, validate restoration, then report where the restore actually ran.
- [ ] Record handover decisions and use the normal owner-reviewed branch and PR workflow.

[Developer handover](HANDOVER.md) · [Agent rules](../AGENTS.md)

# VMsPopulation.sh Namespace Parameter

**Label:** `wayfinder:task`  
**Status:** Resolved  
**Blocked by:** (none)

## Question

`VMsPopulation.sh` auto-generates namespace names from its parameters using the pattern `${Rhel_Version}-oadp-${VMs}-1os-${TotalDataDisks}data[-cbt]-${scName}-ns${i}`. This creates a mismatch with `tests.yaml` scenarios where `namespaces_to_backup` is a fixed string that must match the actual namespace for the backup CR to target correctly.

Add a `-namespace <name>` flag to `VMsPopulation.sh` that overrides the auto-generated name when provided. When omitted, the script keeps its current auto-generation behavior for standalone use.

Implementation:

1. Add `-namespace` to the argument parser (`while [[ $# -gt 0 ]]` block)
2. When `-namespace` is provided, set `NamespaceToBackup` directly instead of constructing it from parameters
3. When `-namespace` is provided with `-ns > 1`, the script should append `-ns${i}` to the provided base name (matching the existing multi-NS loop pattern)
4. Update the `usage()` function to document the new flag
5. Validate: no shell metacharacters in the namespace value

Secondary concern: the script's `ClusterLogin()` function hardcodes `KUBECONFIG=/home/kni/clusterconfigs/auth/kubeconfig`. Benchmark-runner manages its own auth. Either add a `-skip-login` flag or make `ClusterLogin()` conditional on whether `KUBECONFIG` is already set.

**`[mpqe-MR]`** — This ticket modifies `VMsPopulation.sh` (and potentially `VMsEnableCBT.sh`, `VMsCBTstate.sh` for the same `ClusterLogin()` concern) in the `mpqe-scale-scripts` repository. Changes are ephemeral in `/tmp/mpqe-scale-scripts` — a commit and MR must be submitted on that repo for the changes to persist.

Resolve: script modified, tested with both auto-generated and explicit namespace names, backward-compatible, MR submitted on `mpqe-scale-scripts`.

## Answer

Implemented as specified:

1. Added `-namespace <name>` to `VMsPopulation.sh`'s argument parser. When provided, `NamespaceToBackup` is set directly from it instead of the auto-generated `${Rhel_Version}-oadp-${VMs}-1os-${TotalDataDisks}data[-cbt]-${scName}` pattern.
2. Multi-NS behavior preserved exactly: the `-ns${i}` suffix is still appended per namespace inside the existing `for i in seq 1 ${TotalNS}` loop, whether the base name is auto-generated or overridden (including the pre-existing compounding behavior across iterations — out of scope to fix here).
3. `usage()` updated to document `-namespace`.
4. Validation added: `-namespace` value must match `^[a-zA-Z0-9._-]+$` (rejects shell metacharacters), consistent with how `-vms`/`-disks` are validated.
5. `ClusterLogin()` in `VMsPopulation.sh` now short-circuits (no-op) when `KUBECONFIG` is already set in the environment — this covers benchmark-runner's own auth without touching the hardcoded kni fallback path used for standalone script runs. Applied the same fix to `VMsEnableCBT.sh`, `VMsCBTstate.sh`, and `VMsAddDataVerify.sh`, which all share the identical `ClusterLogin()` helper (the secondary concern named in this ticket, extended to all four scripts for consistency rather than just the two called out).

Verified via `bash -n` syntax checks on all four scripts, `-h` usage output, an invalid-namespace rejection test (`bad;name` → rejected), and an isolated logic replay of the namespace-construction branch confirming both the auto-generated and override paths produce identical `-ns<i>` suffixing behavior. Did not run the full script end-to-end (it provisions real VMs/namespaces and takes tens of minutes) — that happens naturally as part of the [Tier 1 Live Test](006-tier1-live-test.md).

MR submitted: [mpqe-scale-scripts!202](https://gitlab.cee.redhat.com/mlehrer/mpqe-scale-scripts/-/merge_requests/202) (branch `oadp-vmspopulation-namespace-param`).

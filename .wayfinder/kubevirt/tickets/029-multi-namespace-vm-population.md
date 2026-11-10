# Multi-Namespace VM Population and Backup (`ns` > 1)

**Label:** `wayfinder:task`
**Status:** Resolved
**Blocked by:** [Script Integration Dispatch](019-script-integration-dispatch.md), [CBT Kubevirt-Datamover Plugin + Resource Policy (Flow2)](028-cbt-kubevirt-datamover-resource-policy.md)

## Question

More TCs for OADP using `plugin: kubevirt`: when `vm_population_script_args.ns` > 1 (default 1), `VMsPopulation.sh` populates several sibling namespaces (`<base>-ns1`, `<base>-ns2`, ... `<base>-nsN`) with identical VM/disk/CBT config instead of just one. The framework needs to:

1. Populate all N namespaces (one `VMsPopulation.sh -ns N` invocation, not N separate calls).
2. Validate VM existence/data across all N namespaces, recreating them if missing.
3. Include all N namespaces in the backup CR's `includedNamespaces`:
   ```yaml
   spec:
     csiSnapshotTimeout: 30m0s
     includedNamespaces:
       - rhel9-oadp-2vms-1os-1data-rbd-ns1
       - rhel9-oadp-2vms-1os-1data-rbd-ns2
       - rhel9-oadp-2vms-1os-1data-rbd-ns3
   ```
4. Work for both Flow1 (no CBT) and Flow2 (CBT, ticket 028) — the CBT resource-policy setup is scenario-wide and orthogonal to namespace count.

No `tests.yaml` scenario uses this yet — this ticket is the framework-side prep so that scenario can be added later.

## Answer

Added `vm_population_script_args.ns` (positive int, default 1) to scenario validation. The scenario continues to name only the *first* sibling namespace via `namespaces_to_backup` (e.g. `...-ns1`, per the existing ticket-019 convention) — everything downstream reconstructs the other N-1 siblings from that one value plus `ns`.

**New shared helpers in `scenario.py`:**

- `get_script_ns_count(scenario)` — reads `vm_population_script_args.ns`, defaulting to 1 (and always 1 for `vm_population_method: native`, where `ns` is meaningless).
- `expand_multi_namespaces(namespace, ns_count)` — strips the `-ns1` suffix and returns `[base-ns1, ..., base-nsN]`; returns `[namespace]` unchanged when `ns_count <= 1` (today's behavior, byte-for-byte).

**Dataset loading (`scenario.py::load_datasets_for_scenario`):** the dict-form dataset branch now loops `expand_multi_namespaces(...)` and creates one `scenario_datasets` summary entry per sibling namespace (mirroring how the list-form branch already handles multiple namespaces). This is the load-bearing change — everything built on `ds_get_all_namespaces()` inherits correct multi-namespace behavior for free:

- **Backup CR** (`execution.py::oadp_execute_scenario`): already did `",".join(ds_get_all_namespaces())` before this ticket, so `--include-namespaces` (and therefore `spec.includedNamespaces`) automatically covers all N siblings. **No changes needed here** — this was the main risk area and it turned out to already be correct once dataset loading was fixed. Applies identically to Flow1 and Flow2 since `resource_policy_configmap` selection (ticket 028) is scenario-wide, not per-namespace.
- **Incremental data** (`vm_operations.py::apply_incremental_data_if_requested`): already looped `ds_get_all_namespaces()` — no changes needed.
- **Dataset utilization reporting** (`scenario.py::calc_total_dataset_utilization_per_namespace`): updated the dict-form branch to loop `ds_get_all_namespaces()` instead of the single `namespaces_to_backup` value, so per-namespace size totals cover every sibling.

**VM existence validation (`vm_validation.py::validate_expected_vm_datasets`):** this function builds its own dataset list directly off `scenario["dataset"]`/`scenario["args"]`, independent of `scenario_datasets` — so it needed its own `expand_multi_namespaces()` call in the dict-form branch (one synthetic `ds` per sibling namespace). Without this, only `<base>-ns1` would ever be checked for existence, and siblings 2..N would never be detected as missing (or created).

**VM population (`vm_operations.py::create_vm_dataset_via_script`):** `VMsPopulation.sh` has no way to create a single sibling namespace on its own — a single `-ns N` invocation always creates all N at once (`<base>-ns1`..`<base>-nsN`). Since `verify_vm_datasets_before_backups()`'s recreate loop calls this method once per *failing* namespace (up to N times when nothing exists yet), it now only actually runs the script when `ds["namespace"]` ends in `-ns1` (the group "leader") — passing `-ns {ns_count}` (previously hardcoded `-ns 1`) so the leader's single call creates every sibling. Calls for `-ns2`..`-nsN` are no-ops (log + skip) beyond checking/recording that sibling's own CBT state. `ns_count` defaults to 1, and a namespace always ends in `-ns1` when `ns_count == 1`, so single-namespace scenarios are byte-for-byte unaffected (leader check is always true, `-ns 1` unchanged).

**VMsPopulation.sh cumulative-namespace bug — already fixed upstream:** while implementing this, spotted what looked like a latent bug in the script's multi-NS loop (`NamespaceToBackup+="-ns${i}"` never reset between iterations, so it would concatenate cumulatively — `base-ns1-ns2` instead of `base-ns2` — for `-ns` > 1). Confirmed with the script owner that this is **already fixed in a newer version of `VMsPopulation.sh`** (which also has additional coverage beyond what the local `/tmp/mpqe-scale-scripts` checkout had). No action needed here; the local scratch-checkout edit made while investigating this was reverted rather than committed, since it's superseded by the newer version.

**Unit tests:** 26 new tests in `test_kubevirt.py` covering `get_script_ns_count`/`expand_multi_namespaces` truth tables, `ns` schema validation (positive-int rejection), `load_datasets_for_scenario` expansion into N summaries (each carrying the same dataset config), `validate_expected_vm_datasets` validating/failing every sibling independently, `create_vm_dataset_via_script` leader-vs-sibling dispatch (leader passes `-ns N`, siblings skip the script call but still record their own CBT state), and an end-to-end `oadp_execute_scenario` test confirming `--include-namespaces` covers all siblings for both Flow1 and Flow2. Full `oadp` suite: 356 passed (was 327 before this ticket).

## Follow-up (not addressed by this ticket)

- **`/tmp/mpqe-scale-scripts` git corruption** — unrelated to the (already-resolved) namespace bug above, but still worth noting: the local checkout's git object database has missing blobs/trees (`git fsck` errors), likely from `/tmp` cleanup, same root cause as the earlier `br-venv` incident. Worth re-cloning before the next ticket that needs to commit/push changes there.
- **cleanup.py / log_collection.py / reporting.py / pod_validation.py** still read `scenario["args"]["namespaces_to_backup"]` directly as a single namespace string (post-run dataset cleanup, VSC cleanup, log/artifact collection, reporting summaries). These weren't in scope for this ticket (which was specifically about population + backup CR) — they'll only actually run against `<base>-ns1` for `ns` > 1 scenarios until updated to loop `ds_get_all_namespaces()` like `execution.py` and `vm_operations.py` already do.
- **Restore-side multi-namespace** — not exercised; `exec_restore()` wasn't examined as part of this ticket. Given restores in this framework restore from the same backup (whose CR already lists all N namespaces), this is expected to work unchanged, but hasn't been verified.
- Not live-tested — no `tests.yaml` scenario exists yet exercising `ns` > 1 (per the request, the scenario will be authored separately, later).

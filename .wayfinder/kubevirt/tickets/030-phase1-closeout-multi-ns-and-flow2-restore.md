# Phase 1 Closeout: Multi-NS Outside Backup + Flow2 Restore ResourcePolicy

**Label:** `wayfinder:task`
**Status:** Resolved
**Blocked by:** [Multi-Namespace VM Population and Backup](029-multi-namespace-vm-population.md), [CBT Kubevirt-Datamover Plugin + Resource Policy (Flow2)](028-cbt-kubevirt-datamover-resource-policy.md)

## Question

Ticket 029 wired multi-NS (`ns` > 1) for population + backup CR `includedNamespaces`, but cleanup / log collection / reporting still keyed off the single `namespaces_to_backup` (ns1). Ticket 028 wired `--resource-policies-configmap` into `exec_backup()` only — Flow2 restores were left untouched. Also, with Phase 1 VM backup/restore supported, the `OADP_ENABLE_KUBEVIRT` default should flip from opt-in (`False`) to on (`True`).

Resolve: sibling namespaces are cleaned/logged/reported; Flow2 restores get the same resource-policy flag as backups; feature flag defaults to `True`.

## Answer

**Feature flag default:** [`environment_variables.py`](../../../../benchmark_runner/main/environment_variables.py) now reads `get_boolean_from_environment('OADP_ENABLE_KUBEVIRT', True)`. Explicit `False` still gates VM scenarios via `_reject_kubevirt_if_disabled()` (reverses ticket 003's safe-rollout default now that Phase 1 is live).

**Shared helper:** `scenario_backup_namespaces(scenario)` in `scenario.py` expands `args.namespaces_to_backup` via `get_script_ns_count` / `expand_multi_namespaces` so callers without loaded `scenario_datasets` still cover siblings.

**Multi-NS close-out:**
- `cleanup.py`: post-run dataset delete and ns-scoped VSC clean loop `ds_get_all_namespaces()` (fallback: `scenario_backup_namespaces`).
- `log_collection.py`: pod distribution + VM artifact collectors (`collect_vm_describe`, `collect_dv_status`, `collect_vm_events`, `collect_vm_pvc_status`) iterate all siblings; multi-NS artifact filenames are suffixed with the namespace (single-NS filenames unchanged).
- `reporting.py`: `enrich_summary_with_vm_metadata` sums VM/VMI/DV counts across siblings.
- `pod_validation.py`: dict-form dataset validation loops expanded namespaces.

**Flow2 restore:** `exec_restore(..., resource_policy_configmap=None)` appends `--resource-policies-configmap` on downstream restores when set. `oadp_execute_scenario()` restore branch uses the same CBT predicate as backup (`_scenario_needs_kubevirt_datamover_plugin`). Upstream restore unchanged (Velero 1.12).

**tests.yaml:** `ns` > 1 scenarios already present in mpqe-scale-scripts (TC 10.8 CBT 3ns, TC 10.9 Flow1 3ns); unit tests cover those shapes. New unit tests cover cleanup loops, reporting aggregation, log collectors, `exec_restore` flag, and restore wiring.

Full `test_kubevirt.py`: 266 passed.

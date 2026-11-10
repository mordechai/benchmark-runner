# CBT Kubevirt-Datamover Plugin + Resource Policy (Flow2)

**Label:** `wayfinder:task`
**Status:** Resolved
**Blocked by:** [CBT Script Integration](021-cbt-script-integration.md), [Tier 1 Live Test](006-tier1-live-test.md)

## Question

`plugin: kubevirt` VM backups actually split into two distinct flows depending on whether the scenario's `vm_population_script_args.cbt` is set, and Flow2's setup was missing entirely:

- **Flow1** (no `cbt` / `cbt: false`, e.g. TC 10.4): plain `kubevirt` plugin in the DPA's `defaultPlugins` is sufficient. `exec_backup()` already runs a kubevirt-datamover backup (`--data-mover 'velero' --snapshot-move-data=true`) per ticket 006.
- **Flow2** (`cbt: true`, e.g. TC 10.5): additionally requires:
  1. `kubevirt-datamover` present in the DPA's `spec.configuration.velero.defaultPlugins` (not just `kubevirt`) — verify and add if missing.
  2. A `kubevirt-volume-policy` ConfigMap in the `openshift-adp` namespace — verify and add if missing:
     ```yaml
     apiVersion: v1
     kind: ConfigMap
     metadata:
       name: kubevirt-volume-policy
       namespace: openshift-adp
     data:
       policy.yaml: |
         version: v1
         volumePolicies:
           - conditions: {}
             action:
               type: skip
     ```
  3. The backup CR referencing that ConfigMap via `spec.resourcePolicy: {kind: ConfigMap, name: kubevirt-volume-policy}`.

Without this, the CSI snapshot policy conflicts with the kubevirt-datamover plugin's own snapshot handling of CBT-labeled volumes — this is exactly what surfaced live during the TC 10.5 backup run in ticket 006's follow-up testing: 12 warnings of the form `PVC ... has snapshot policy which conflicts with kubevirt datamover`. The backup still completed (CSI snapshot fallback), but the setup was incomplete relative to what Flow2 actually needs.

Resolve: `is_dpa_change_needed`/DPA setup distinguishes Flow1 vs Flow2, `kubevirt-datamover` gets registered and the ConfigMap applied automatically for CBT scenarios, and `exec_backup()` references the resource-policy ConfigMap on CBT backups.

## Answer

Added a new helper, `OadpConfigurationMixin._scenario_needs_kubevirt_datamover_plugin(scenario)`, that is `True` only when the scenario both needs the `kubevirt` plugin (`_scenario_needs_kubevirt_plugin`) **and** has `vm_population_script_args.cbt` truthy — i.e. exactly the Flow2 case (TC 10.5-style).

DPA setup (`configuration.py`):

- `is_dpa_change_needed()` deliberately does **not** gate on `kubevirt-datamover` — that plugin isn't known to `modify-dpa.yaml` (the ansible playbook this function's return value triggers), so reporting a change there would just cause a no-op ansible run.
- New `ensure_kubevirt_datamover_plugin(scenario, oadp_namespace)`: no-op unless Flow2; otherwise reads the DPA, and if `kubevirt-datamover` is missing from `defaultPlugins`, appends it via a direct `oc patch --type=json` (mirrors the JSON-patch style already used by `set_velero_log_level()`, since this bypasses the ansible-managed path entirely). Returns `True` iff it actually patched, so the caller knows whether to wait for the DPA reconcile.
- New `ensure_kubevirt_volume_policy_configmap(scenario, oadp_namespace)`: no-op unless Flow2; otherwise applies the ConfigMap manifest above via `oc apply -f -` (piped through a heredoc over the existing `ssh.run()`), idempotent so it's safe to call unconditionally every run.
- Both are called from `oadp_workloads.run_workload()` right after the existing Flow1 DPA block, gated only by `this_is_downstream()` (matches every other DPA-touching call in that block); if `ensure_kubevirt_datamover_plugin` reports a patch was applied, `wait_for_dpa_changes()`/`verify_bsl_status()` are re-run so Velero has restarted with the new plugin before the backup CR is created.

Backup execution (`execution.py`):

- `exec_backup()` gained an optional `resource_policy_configmap` parameter. For the downstream `PLUGIN_KUBEVIRT` branch, when set, it appends `--resource-policies-configmap <name>` to the `velero backup create` command (confirmed via Velero upstream docs — this maps 1:1 to the backup CR's `spec.resourcePolicy: {kind: ConfigMap, name: <name>}`).
- `oadp_execute_scenario()` computes this value as `KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME` when `_scenario_needs_kubevirt_datamover_plugin(test_scenario)` is `True`, else `None`, and threads it through to `exec_backup()`.
- Upstream Velero's `PLUGIN_KUBEVIRT`/`PLUGIN_VBD` branch is intentionally left unchanged: the pinned upstream Velero (1.12, per ticket 009) predates the `--resource-policies-configmap` flag (introduced 1.14+), and upstream `kubevirt-datamover` plugin registration is already a known open Phase 2 gap (ticket 009) — Flow2 is downstream-only for now.

New constants in `constants.py`: `PLUGIN_KUBEVIRT_DATAMOVER = "kubevirt-datamover"` (a DPA `defaultPlugins` entry, never a valid `args.plugin` value — deliberately not added to `KNOWN_PLUGINS`) and `KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME = "kubevirt-volume-policy"`.

29 new unit tests added to `test_kubevirt.py`: `_scenario_needs_kubevirt_datamover_plugin` truth table (cbt true/false/absent, non-kubevirt plugin with cbt true); `ensure_kubevirt_datamover_plugin` no-op/already-present/patches-when-missing/fails-when-DPA-missing (asserts existing plugins are preserved, not clobbered); `ensure_kubevirt_volume_policy_configmap` no-op/applies-manifest/fails-on-apply-error; `exec_backup` resource-policy flag present only for kubevirt+CBT and never leaking to other plugins; and an `oadp_execute_scenario` wiring test confirming the CBT scenario passes `KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME` through while the non-CBT scenario passes `None`. Full `oadp` suite: 327 passed (was 298 before this ticket).

### Live verification (cluster access restored same day)

Re-ran TC 10.5 (`backup-kubevirt-script-2vm-1data-cbt-cephrbd`) end-to-end from scratch (source namespace had been cleaned up, so this exercised full VM re-population with `-cbt` plus the backup):

- `ensure_kubevirt_datamover_plugin`/`ensure_kubevirt_volume_policy_configmap` were no-ops on this cluster — both the `kubevirt-datamover` DPA plugin and the `kubevirt-volume-policy` ConfigMap were already present (pre-existing on this shared cluster, age 37d), which happens to validate the idempotent-detection logic itself (no spurious patch attempted).
- Backup CR `backup-kubevirt-script-2vm-1data-cbt`: `phase: Completed`, `itemsBackedUp: 407/407`, **`warnings: None`, `errors: None`** — the 12 "PVC ... has snapshot policy which conflicts with kubevirt datamover" warnings seen in ticket 006's pre-fix run are gone. `spec.snapshotMoveData: true`, `spec.datamover: velero`, `spec.resourcePolicy: {kind: configmap, name: kubevirt-volume-policy}` — exactly the shape specified in this ticket.
- 2 `DataUpload` CRs, both `Completed` — one per VM (not per disk/PVC as in the non-CBT Flow1 case), confirming CBT-enabled kubevirt-datamover backs up the whole VM as a single data-mover operation.
- Noted in passing (unrelated to this fix): VM re-population with `-cbt` hit a transient `SyncFailed`/"No space left on device" on the QCOW2 CBT overlay during the post-label VM restart for both VMs; self-resolved on retry within the script's own retry loop, total population time ~35 min (vs. the usual ~15 min for non-CBT).

Restore-side `resourcePolicy` (noted as an open question in `map.md`) remains unexercised — this verification only covered backup.

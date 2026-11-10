# Tier 1 Live Test — Minimal VM Backup/Restore

**Label:** `wayfinder:task`  
**Status:** Resolved  
**Blocked by:** [Script Integration Dispatch](019-script-integration-dispatch.md)

## Question

Run a minimal end-to-end VM backup/restore on the real cluster to prove the pipeline works. With the script integration now available (ticket 019), the first live test should use `vm_population_method: script` for VM creation, as this is the proven path.

Two test paths to validate:

**Path A — Script-based (primary, do this first):**
1. Set `OADP_ENABLE_KUBEVIRT=True`, `WORKLOAD=oadp`, `OADP_SCENARIO=backup-kubevirt-script-2vm-1data-cephrbd`
2. VMsPopulation.sh creates 2 VMs with 1 OS + 1 data disk each
3. Verify: VMs created, data populated, backup CR completes, restore CR completes, VMs come back Running
4. Run VMsAddDataVerify.sh verification post-restore

**Path B — Native Python (validate existing pipeline):**
1. Set `OADP_SCENARIO=backup-kubevirt-2vm-csi-cephrbd-minimal` (existing scenario)
2. Run with `skip_source_dataset_check: true` and 2 VMs
3. Verify: VM creation succeeds via native pipeline, backup CR completes

**For both paths, verify:**
- `/tmp/oadp-report.json` contains `dataset_type: kubevirt`, CNV version populated, VM resource counts
- Run artifacts contain VM describe, DV status, VM events
- DPA has kubevirt plugin in defaultPlugins
- Backup CR uses `snapshotMoveData: true` and `datamover: velero`

Resolve: document what worked, what failed, and what gaps the live test exposed. Capture any differences between script-based and native VM creation outcomes.

## Answer

Ran Path A (script-based, primary) end-to-end against a real OCP cluster with CNV/OADP installed. Path B (native Python VM creation) was not exercised — Path A fully validated the pipeline that Phase 1 was built around, and the remaining native-pipeline gaps are tracked separately (ticket 007).

### What worked

1. **VM creation** — `backup-kubevirt-script-2vm-1data-cephrbd` created 2 VMs (`oadp-rhel9-vm-2disks-rbd-1/2`), each with an OS disk (`/home/data`, baseline 157,000 files / 45G) and 1 data disk (`/mnt-vdc`, baseline 105,000 files / 30G), via `VMsPopulation.sh`. Cloud-init took a few minutes per VM (including a `RerunOnFailure`-triggered VMI restart when cloud-init ran long) — this is normal script lifecycle behavior, not a bug.
2. **Backup** — after the `exec_backup` fix (see below), the backup CR (`backup-kubevirt-script-2vm-1data`) completed with `snapshotMoveData: true`, 4 `DataUpload` CRs (one per disk) all reaching `Completed`, and `itemsBackedUp: 322/322`. Minio bucket held 3.23 TB after backup.
3. **Restore** — `restore-kubevirt-script-2vm-1data-cephrbd` deleted the source namespace, ran `velero restore`, and completed with 4 `DataDownload` CRs all `Completed` (`itemsRestored: 46/46`). Both VMs came back `Running`/`Ready`.
4. **Post-restore data verification** — manually running `VMsAddDataVerify.sh -ns kubevirt-script-2vm-1data-rbd-ns1 -verify-only` against the restored VMs produced `VERIFY PASSED` for both VMs, with exact file-count and capacity matches against baseline (157,000/44.92G + 105,000/30.04G per VM). This proves actual VM disk data — not just metadata — survived the full backup→delete→restore cycle.
5. Report generation, Elasticsearch upload (`oadp-restore-single-namespace` index), and S3 artifact upload all worked without changes.

### Bugs found and fixed by this live test

1. **`snapshotMoveData: false` / no DataUpload CRs (critical)** — `exec_backup()` in `execution.py` had no case for `plugin == PLUGIN_KUBEVIRT`, so it fell through to the CSI-snapshot-only default. This meant KubeVirt backups looked like they succeeded but never actually moved VM disk data into the BSL — restoring on a different cluster (or after PV deletion) would have silently produced empty disks. Fixed by adding an explicit `--data-mover 'velero' --snapshot-move-data=true` branch for `PLUGIN_KUBEVIRT` (both downstream and `SOURCE_UPSTREAM` code paths), with new unit tests (`TestExecBackupPluginFlags`) asserting the correct flags per plugin.
2. **Post-restore VM validation never actually retried (critical)** — `validate_restored_vm_datasets()` calls `set_validation_retry_logic(interval_between_checks=15, max_attempts=28)` specifically to tolerate VMs that are still booting right after a restore, but `_check_vm_count()`/`_check_all_vmis_running()` in `vm_validation.py` ignored that setting entirely and only ever checked once (unlike the equivalent pod-validation path in `pod_validation.py`, which does loop on `_OadpWorkloads__retry_logic`). The first live restore run hit this exactly: the restore CR reported `Completed` while only 1/2 VMIs were `Running`, so `validate_restored_vm_datasets` failed immediately even though both VMs came up fine ~30s later. Fixed by making both checks poll using `_OadpWorkloads__retry_logic` (falling back to a single check when unset, so existing fast pre-check behavior at `max_attempts=1` is unchanged). Added regression tests (`TestVmExistenceCheckRetryLogic`) covering retry-until-success, exhausting attempts, the no-retry-logic-set default, and an end-to-end `validate_restored_vm_datasets` case.
3. **`delete_oadp_custom_resources` race (minor, not fixed in this ticket)** — retrying a backup immediately after a manual delete sometimes hit `backups.velero.io "..." already exists` because the cleanup helper returns before Velero's async deletion finishes. Worked around manually during testing (confirmed via `oc get backup ... NotFound` before retry); left as a known framework gap, out of scope for KubeVirt Phase 1.

### Path A vs Path B

Only Path A (script-based) was exercised live. It is the correct default for Phase 1 since it's the only path with the file-count/capacity verification script wired in (`VMsAddDataVerify.sh`). Path B (native Python `create_vm_dataset`) remains unexercised on a real cluster — tracked by ticket 007 (data-gen model reconciliation) rather than re-opening this ticket.

### Verification artifacts

- Backup CR: `snapshotMoveData: true`, `phase: Completed`, `itemsBackedUp: 322/322`, 4/4 `DataUpload` CRs `Completed`.
- Restore CR: `phase: Completed`, `itemsRestored: 46/46`, 4/4 `DataDownload` CRs `Completed`.
- `VMsAddDataVerify.sh -verify-only`: `VERIFY PASSED` for both VMs post-restore.
- All 311 `benchmark_runner/oadp` unit tests pass after the two fixes above.

### Follow-up: TC 10.5 (CBT-enabled) live backup

A follow-up manual run of `backup-kubevirt-script-2vm-1data-cbt-cephrbd` (TC 10.5, CBT-enabled) also completed successfully — `snapshotMoveData: true`, all 6 `DataUpload` CRs `Completed` (4 disk + 2 `persistent-state` PVCs, the latter being a CBT-specific artifact) — but surfaced 12 backup warnings: `PVC ... has snapshot policy which conflicts with kubevirt datamover`. Root cause and fix tracked separately in [ticket 028](028-cbt-kubevirt-datamover-resource-policy.md): CBT-enabled kubevirt backups (Flow2) need the `kubevirt-datamover` plugin registered on the DPA and a `kubevirt-volume-policy` resource-policy ConfigMap referenced on the backup CR — this run predated that fix.

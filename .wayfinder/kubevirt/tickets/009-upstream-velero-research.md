# Upstream Velero Compatibility Research

**Label:** `wayfinder:research`  
**Status:** Resolved  
**Blocked by:** (none)  
**Note:** Upstream Velero support is Phase 2 scope (not required for Phase 1 script-integration completion). This research ticket ran opportunistically ahead of Phase 2 since it has no technical dependency; the implementation it informs still waits on Phase 1 completion, which is now done (see [Tier 1 Live Test](006-tier1-live-test.md)).

## Question

Research and document how KubeVirt VM backup/restore works with upstream Velero (without OADP operator):

1. **Plugin installation** — how is the Velero kubevirt plugin installed? `velero install --plugins`? Manual binary?
2. **Version matrix** — which Velero versions support the kubevirt plugin? Which kubevirt plugin versions? Which CNV versions?
3. **Configuration differences** — what's different in the Velero config vs OADP DPA? Does the backup location, snapshot location, or plugin config differ?
4. **Code path implications** — what code paths in the OADP workload need `if upstream:` branching? The `config_dpa_for_plugin()` path doesn't apply to upstream. What replaces it?
5. **Test scenario** — what would an upstream kubevirt scenario look like in tests.yaml? Different args?

Deliverable: a markdown summary documenting the compatibility matrix and implementation path for upstream support.

## Answer

Full research deliverable: [research/upstream-velero-compatibility.md](../research/upstream-velero-compatibility.md).

Key findings, summarized:

1. **Plugin install** — upstream has no DPA/`defaultPlugins` concept; the kubevirt plugin is installed via `velero plugin add quay.io/kubevirt/kubevirt-velero-plugin:<tag>` (patches an initContainer onto the Velero Deployment). Nothing else is needed at backup time — the plugin's `BackupItemAction`/`RestoreItemAction` handlers activate automatically once registered.
2. **Version matrix** — this repo's upstream mode is pinned to Velero 1.12 (`VELERO_CLI_PATH = "/tmp/velero-1-12"`), which maps to kubevirt-velero-plugin **v0.6.x**, KubeVirt ≥ v1.0.0, CDI ≥ v1.57.0. CSI was still a separate plugin at that Velero version (merged into core only from v1.14+).
3. **Config differences** — DPA's `backupLocations`/`snapshotLocations`/`nodeAgent` blocks are thin passthroughs of native `BackupStorageLocation`/`VolumeSnapshotLocation`/node-agent DaemonSet objects upstream; `--snapshot-move-data` is a per-backup CLI flag upstream (matching what `exec_backup()` already does for `PLUGIN_VBD`/`PLUGIN_KUBEVIRT`).
4. **Code path implications** — most upstream/downstream branching already exists (`this_is_downstream()`, `exec_backup`/`exec_restore` CLI branching). The one real gap: no upstream equivalent of `config_dpa_for_plugin()` to actually get the kubevirt plugin registered on the Velero deployment — `ensure_kubevirt_plugin_installed()` (sketched in the research doc, §4) needs to be added and wired into `oadp_workloads.py`'s existing `if self.this_is_downstream(): ... else: ...` branch. No changes needed to `exec_backup`/`exec_restore`/`vm_operations.py`/`vm_validation.py` themselves.
5. **Test scenario** — identical `dataset`/`args` shape to the downstream scenario (`plugin: kubevirt`, current unified schema); no new scenario-level fields needed since stream detection happens from live-cluster inspection, not the YAML.

Recommended Phase 2 implementation order (detailed in the research doc): (1) fix schema drift in the 5 existing `tests.yaml` kubevirt scenarios — **already done** as part of [ticket 023](023-script-based-scenarios-and-tests.md), ahead of this research landing; (2) add `ensure_kubevirt_plugin_installed()`; (3) add an `EnableCSI` feature-flag pre-check for upstream; (4) add a named `KUBEVIRT_PLUGIN_IMAGE_UPSTREAM` constant; (5) add the upstream scenario and run a Tier-1 live test against an upstream cluster.

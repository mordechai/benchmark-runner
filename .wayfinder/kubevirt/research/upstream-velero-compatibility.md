# Upstream Velero Compatibility Research

**Ticket:** [009-upstream-velero-research.md](../tickets/009-upstream-velero-research.md)
**Status:** Research complete

## TL;DR compatibility matrix

| Question | Downstream (OADP operator) | Plain upstream Velero |
|---|---|---|
| Plugin install | Automatic — DPA `defaultPlugins: [kubevirt, ...]` causes the OADP operator to inject an init container | Manual: `velero plugin add quay.io/kubevirt/kubevirt-velero-plugin:<tag>` (or an `initContainers` entry in `velero install`/Helm) |
| Config surface | One CRD: `DataProtectionApplication` (DPA) | Multiple native CRs: `BackupStorageLocation`, `VolumeSnapshotLocation` (rarely needed with CSI), node-agent DaemonSet, `velero install` flags |
| Volume backup mechanism for VM PVCs/DataVolumes | CSI plugin is bundled into the Velero image itself in OADP ≥ 1.3 (matches upstream ≥ v1.14); enabled via `defaultPlugins: [csi]` | Same — CSI support has been built into Velero core since v1.14 (no separate `velero-plugin-for-csi` install); enable with `--features=EnableCSI` |
| Data Mover / snapshotMoveData | DPA `spec.features.dataMover` (< 1.3) or plain `--snapshot-move-data` backup flag (≥ 1.3, "VBD"/kopia path) | Native Velero `DataUpload`/`DataDownload` CRs via `velero backup create --snapshot-move-data`, requires node-agent (`--use-node-agent`) |
| kubevirt plugin's own snapshot method | Not a Volume Snapshotter — piggybacks on whatever the backup already uses (CSI or CSI+DataMover) | Same — plugin is BackupItemAction/RestoreItemAction only |
| This repo's DPA-mutation code (`config_dpa_for_plugin`) | Used | **Not applicable** — no DPA object exists upstream |

---

## 1. Plugin installation

Upstream Velero installs the kubevirt plugin exactly like any other Velero plugin — there is **no special manifest, DaemonSet, or CRD** for it. Per the plugin's own README:

> "To install the plugin check current velero documentation https://velero.io/docs/main/overview-plugins/. Below example for kubevirt-velero-plugin version v0.9.0 on Velero 1.18.0:
> ```
> velero plugin add quay.io/kubevirt/kubevirt-velero-plugin:v0.9.0
> ```"
— [kubevirt/kubevirt-velero-plugin README, "Install" section](https://github.com/kubevirt/kubevirt-velero-plugin/blob/main/README.md#install)

Mechanically, `velero plugin add <image>` (or the equivalent `--plugins <image>` flag on `velero install` for a fresh install) patches the Velero server `Deployment` to add an `initContainer` running that image, which copies the plugin binary into a shared `emptyDir` (`/target`) that the Velero server process reads on startup — this is the generic Velero plugin architecture, not anything kubevirt-specific ([Velero docs, "Velero plugin system"](https://velero.io/docs/main/overview-plugins/)). Equivalently you can hand-edit the Deployment/Helm values to add the init container directly (this is what several downstream consumers, e.g. Alauda and Cozystack, do — see their manifests referenced in the README's linked ecosystem examples).

Per the README's "Backup/Restore Virtual Machines Using the Plugin" section: **"When the plugin is deployed it is already effective. There is nothing special required to make it work."** No extra CLI flags, ConfigMaps, or feature gates are needed at backup time — the plugin registers `BackupItemAction`/`RestoreItemAction` handlers that Velero invokes automatically for `VirtualMachine`, `VirtualMachineInstance`, `DataVolume`, `PersistentVolumeClaim`, and `Pod` objects it encounters during a normal `velero backup create` / `velero restore create`.

Steps end-to-end for upstream:
1. `velero install --provider <bsl-provider> --plugins <bsl-plugin-image> --features=EnableCSI --use-node-agent ...` (standard Velero install, CSI is now built into Velero core so no separate CSI plugin image is needed — see §3)
2. `velero plugin add quay.io/kubevirt/kubevirt-velero-plugin:<version>` — pick the version from the compatibility table in §2
3. `velero backup create <name> --include-namespaces <ns>` — same command whether the namespace has pods or VMs; the plugin activates automatically

## 2. Version matrix

Straight from the plugin repo's own compatibility table ([README, "Compatibility" section](https://github.com/kubevirt/kubevirt-velero-plugin/blob/main/README.md#compatibility)):

| kubevirt-velero-plugin | Velero | KubeVirt | CDI |
|---|---|---|---|
| v0.9.x | v1.18.x | ≥ v1.1.0 | ≥ v1.57.0 |
| v0.8.x | v1.16.x | ≥ v1.1.0 | ≥ v1.57.0 |
| v0.7.x | v1.14.x | ≥ v1.1.0 | ≥ v1.57.0 |
| v0.6.x | v1.12.x | ≥ v1.0.0 | ≥ v1.57.0 |

Additional per-release detail from [GitHub Releases](https://github.com/kubevirt/kubevirt-velero-plugin/releases):
- **v0.9.0** (2026-03-27): bumped to Velero v1.16.2, KubeVirt v1.6.0.
- **v0.8.0** (2025-04-29): bumped Velero 1.14.0 → 1.16.0.
- **v0.7.0** (2024-06-20): bumped to Velero v1.14 (the release where the CSI plugin was merged into Velero core — see §3).
- **v0.6.2** (2024-02-14): pinned back to Velero 1.12.z + matching `velero-plugin-for-csi`.
- **v0.6.1** (2023-11-07): Velero 1.9.4.

**Relevance to this repo:** `benchmark_runner/oadp/constants.py` hardcodes `VELERO_CLI_PATH = "/tmp/velero-1-12"`, i.e. this repo's existing upstream-Velero mode already targets **Velero 1.12**. Per the table above, that maps to **kubevirt-velero-plugin v0.6.x**, KubeVirt ≥ v1.0.0, CDI ≥ v1.57.0. If/when the repo's upstream Velero build is bumped, the plugin tag must be bumped in lockstep or the plugin will be running against an untested Velero API surface (the table only lists *tested* combos; nothing says newer/older combos are guaranteed to fail, but they're unsupported).

The Backup Methods sub-section of the README is also directly relevant: *"This plugin has been tested with... CSI volume snapshots... CSI volume snapshots with DataMover... Other backup methods, such as file system backup (Kopia/Restic) or native cloud provider snapshots, have not been tested with this plugin and may not work correctly with KubeVirt volumes."* This confirms VM/DataVolume PVCs must go through the CSI (or CSI+DataMover) path, not the FSB/kopia/restic path — see §3.

## 3. Configuration differences (DPA vs. native Velero resources)

### 3a. Plugin registration
- **OADP/DPA:** `spec.configuration.velero.defaultPlugins: [openshift, kubevirt, csi, <bsl-provider>]`. The operator reconciles this list into init containers on the Velero Deployment it manages. Confirmed from official OpenShift/OKD docs:
  > "Install the Data Protection Application (DPA) with the `kubevirt` and `openshift` OADP plug-ins." — [Configuring OADP with OpenShift Virtualization](https://docs.okd.io/latest/backup_and_restore/application_backup_and_restore/installing/installing-oadp-kubevirt.html)
- **Upstream:** No `defaultPlugins` concept. Each plugin is a separate `velero plugin add <image>` call (or `initContainers` list at `velero install` time). `openshift`-plugin equivalent doesn't exist/isn't needed upstream since it exists to translate OpenShift-specific SCC/route objects; `csi` is no longer a separate plugin at all (see below).

### 3b. CSI support — built into Velero core since v1.14
This is the single biggest "gotcha" divergence and it collapses a distinction that used to exist:
> "From release-1.14, the `github.com/velero-io/velero-plugin-for-csi` repository, which is the Velero CSI plugin, is merged into the `github.com/velero-io/velero` repository... As a result, no need to install Velero CSI plugin anymore." — [Velero docs, CSI Snapshot Support](https://velero.io/docs/main/csi/)

So:
- **OADP (≥1.3, tracking Velero ≥1.14):** `csi` in `defaultPlugins` is effectively a no-op flag that just enables the feature (the code is already in the Velero image); OADP still lists it for explicitness/back-compat.
- **Upstream (≥1.14):** enable via feature flag at install time, no plugin image: `velero install --features=EnableCSI --plugins=<bsl-plugin> ...` ([Velero docs, "Installing Velero with CSI support"](https://velero.io/docs/main/csi/)).
- **Upstream (<1.14, e.g. this repo's pinned 1.12):** CSI *was* a separate plugin (`velero-plugin-for-csi`) that had to be installed the same way as kubevirt's (`velero plugin add velero/velero-plugin-for-csi:<version>`), in addition to `--features=EnableCSI`. This matters for this repo today since it's pinned to Velero 1.12.

### 3c. BackupStorageLocation / VolumeSnapshotLocation
- **OADP:** DPA `spec.backupLocations[]` / `spec.snapshotLocations[]` are translated 1:1 by the operator into native `BackupStorageLocation`/`VolumeSnapshotLocation` objects in the same namespace.
- **Upstream:** You create the `BackupStorageLocation` (and, only for legacy non-CSI block-storage snapshotting, `VolumeSnapshotLocation`) objects directly, or let `velero install --bucket ... --backup-location-config ...` generate the default one. With CSI-based snapshotting (which is what VM PVCs need — see §3e) a `VolumeSnapshotLocation` isn't required at all; the CSI `VolumeSnapshotClass` mechanism replaces it ([Velero CSI docs](https://velero.io/docs/main/csi/)). `benchmark_runner/oadp/configuration.py::config_dpa_for_plugin` reads `dpa_data["spec"]["backupLocations"][0]["velero"]["objectStorage"]["bucket"|"config"]["profile"]|"credential"]["name"]` — upstream there is simply a `BackupStorageLocation` CR with the same `spec.objectStorage`/`spec.config`/`spec.credential` fields (DPA's schema is a thin passthrough of the native Velero BSL/VSL spec), so translating this read is straightforward.

### 3d. Data Mover / `nodeAgent` / uploaderType
- **OADP:** `spec.configuration.nodeAgent.enable: true` + `uploaderType: kopia|restic` (OADP ≥1.3) deploys the node-agent DaemonSet; pre-1.3 used `spec.configuration.restic.enable` and a separate `spec.features.dataMover` block for CSI Data Mover.
- **Upstream:** `velero install --use-node-agent [--default-volumes-to-fs-backup]` deploys the same node-agent DaemonSet directly; no `uploaderType` install flag exists — Kopia is the only uploader for the *built-in* Velero data mover as of v1.14+ (Restic is legacy/FSB-only path, selected via `--uploader-type=restic|kopia` for File System Backup, a different feature from CSI Data Mover). Data Mover ("VBD"/`snapshotMoveData` in this repo's terms) is a per-backup flag, not an install-time toggle: `velero backup create NAME --snapshot-move-data` ([Velero docs, CSI Snapshot Data Movement, "To back up"](https://velero.io/docs/main/csi-snapshot-data-movement/#to-back-up)). This repo's `execution.py::exec_backup` already implements exactly this for `PLUGIN_VBD` upstream: `--data-mover 'velero' --snapshot-move-data=true`.

### 3e. Does upstream Velero need restic/kopia (FSB) or CSI for kubevirt PVCs specifically?
**CSI (or CSI+DataMover), not FSB/restic/kopia.** The kubevirt-velero-plugin README is explicit that only CSI-based methods have been validated (§2 above), and mechanically this makes sense: KubeVirt VM disks are backed by `DataVolume`→`PVC` objects, and the plugin's `DVBackupItemAction`/`VMBackupItemAction` only add CDI annotations and object-graph completeness (instancetypes, access credentials, launcher pod, etc.) — the actual disk bytes still move through whatever Velero's normal PVC backup path is. If that path is FSB (kopia/restic mounting the pod volume), the plugin authors haven't validated it and don't recommend it; if it's CSI snapshot (optionally +DataMover for off-cluster data), that's the tested/recommended path — same conclusion as OADP's own defaults (`csi` is always in the recommended `defaultPlugins` list alongside `kubevirt` in every official OADP/OKD kubevirt doc found, e.g. [Configuring OADP with OpenShift Virtualization](https://docs.okd.io/latest/backup_and_restore/application_backup_and_restore/installing/installing-oadp-kubevirt.html), [Using OADP VMFR](https://docs.okd.io/4.22/backup_and_restore/application_backup_and_restore/oadp-vmfr/oadp-using-vmfr.html)). This repo's existing scenarios already follow this: every kubevirt scenario in `tests.yaml` uses CSI-family backing (`sc: ocs-storagecluster-ceph-rbd-virtualization`).

A `VolumeSnapshotClass` with the `velero.io/csi-volumesnapshot-class: "true"` label (or the `snapshot.storage.kubernetes.io/is-default-class` annotation) is required for the CSI driver backing the VM's storage class, matching what `configuration.py::set_volume_snapshot_class` already applies downstream (`vsc-cephRBD.yaml`/`vsc-cephFS.yaml`) — this requirement is identical upstream and downstream since it's a Kubernetes/CSI-level object, not an OADP-managed one.

## 4. Code path implications (grounded in this repo)

Reviewed: `benchmark_runner/oadp/{configuration,environment,execution,scenario,vm_operations}.py`, `benchmark_runner/oadp/constants.py`, `tests/unittest/benchmark_runner/oadp/test_kubevirt.py`, and the external `mpqe-scale-scripts` repo's `dpa.yaml.j2` / `modify-dpa.yaml` / `tests.yaml`.

**Good news: most of the upstream/downstream branching infrastructure already exists** for non-kubevirt scenarios and needs no new architecture, only a kubevirt-specific gap to close:

- `environment.py::this_is_downstream()` / `set_velero_stream_source()` already detect upstream vs. downstream by checking for the `openshift-adp-controller-manager` pod, and set `test_env["source"]` to `SOURCE_UPSTREAM`/`SOURCE_DOWNSTREAM`.
- `oadp_workloads.py` already gates the entire DPA-management block behind `if self.this_is_downstream():` (lines 254–264): `set_default_storage_class`, `set_volume_snapshot_class`, `is_dpa_change_needed`, `config_dpa_for_plugin`, `wait_for_dpa_changes` are **all skipped upstream today**. This means upstream runs currently assume the cluster's Velero install/plugins/node-agent/VolumeSnapshotClass are pre-provisioned out-of-band (by whatever installs `/tmp/velero-1-12`), not managed by benchmark-runner.
- `execution.py::exec_backup`/`exec_restore` already branch on `test_env["source"] == SOURCE_UPSTREAM` and call the CLI binary at `test_env["velero_cli_path"]` directly via SSH instead of `oc exec deployment/velero`. Neither function has a `PLUGIN_KUBEVIRT`-specific branch — both fall through to the same "plain backup, no extra flags" path used for `PLUGIN_CSI`, which is *correct* per §1/§3e (the plugin activates automatically, no CLI flag needed) — **no code change needed here** for kubevirt specifically, on either stream.
- `configuration.py::set_velero_log_level` already has a working upstream branch that JSON-patches the `velero` Deployment args directly instead of the DPA.

**The actual gap:** `config_dpa_for_plugin()` / `is_dpa_change_needed()` are the *only* code paths that add the `kubevirt` plugin to the running Velero instance, and they are DPA-only (downstream-only) by construction — they read/write `dpa_data["spec"]["configuration"]["velero"]["defaultPlugins"]` via `oc get dpa`/ansible `modify-dpa.yaml`. There is currently **no upstream equivalent that ensures the kubevirt plugin is actually installed on the Velero deployment** before a `role: kubevirt` scenario runs upstream. Today this is silently assumed to be pre-provisioned; per §1, the correct replacement is:

```python
@logger_time_stamp
def ensure_kubevirt_plugin_installed(self, scenario, oadp_namespace):
    """Upstream equivalent of config_dpa_for_plugin(): ensure the kubevirt
    Velero plugin image is present as an initContainer on the velero Deployment."""
    if not self._scenario_needs_kubevirt_plugin(scenario):
        return
    velero_deployment = self.get_oc_resource_to_json(
        resource_type="deployment", resource_name="velero", namespace=oadp_namespace,
    )
    init_containers = velero_deployment["spec"]["template"]["spec"].get("initContainers", [])
    if not any("kubevirt-velero-plugin" in c["image"] for c in init_containers):
        ssh.run(cmd=(
            f"cd {test_env['velero_cli_path']}/velero/cmd/velero; "
            f"./velero plugin add quay.io/kubevirt/kubevirt-velero-plugin:{plugin_tag} "
            f"-n {oadp_namespace}"
        ))
```
— i.e. a thin wrapper around `velero plugin add`, called from the same call site in `oadp_workloads.py` but under an `else:` branch (or unconditionally, since `velero plugin add` is idempotent-ish — it will error if already present, so an existence check first is warranted as sketched above). This is a **much smaller** surface than the downstream DPA-mutation path: no ansible playbook, no bucket/profile/credential plumbing, just one `oc get deployment` read + one conditional `velero plugin add` exec.

**Second-order implication — CSI enablement upstream:** because `--features=EnableCSI` is a Velero-server-wide **install-time** flag (not per-backup, not per-plugin), if the repo's shared upstream Velero install (at `test_env['velero_cli_path']`) wasn't installed with `--features=EnableCSI`, kubevirt scenarios cannot work upstream regardless of the plugin. This should be a fail-fast pre-check (`velero server --features` or inspecting the Deployment's `--features` arg for `EnableCSI`) analogous to `is_dpa_change_needed()`'s CSI check downstream (`configuration.py:48-57`).

**Third-order implication — plugin ↔ Velero version pinning:** since `VELERO_CLI_PATH` hardcodes `velero-1-12`, the plugin tag to install is effectively fixed at `v0.6.x` per §2's table; this should become a named constant (e.g. `KUBEVIRT_PLUGIN_IMAGE_UPSTREAM = "quay.io/kubevirt/kubevirt-velero-plugin:v0.6.2"`) in `constants.py` rather than hardcoded inline, so bumping `VELERO_CLI_PATH` and the plugin tag stay visibly coupled.

**Scenario-schema discrepancy found (not upstream-specific, but affects §5):** the in-repo, currently-tested contract (`scenario.py:128-135`, `test_kubevirt.py`) requires **`plugin: kubevirt`** literally in `args`, per commit `65547f80` ("unify plugin field — use `plugin: kubevirt` instead of separate `kubevirt_plugin` flag"). The external `mpqe-scale-scripts/oadp-helpers/templates/internal_data/tests.yaml` scenarios still use the **older** schema (`plugin: csi` + a separate `kubevirt_plugin: true` arg), which `65547f80` postdates and no longer matches. Any new tests.yaml scenario (upstream or downstream) must use the current `plugin: kubevirt` form or `validate_scenario()` will reject it — this is a pre-existing drift between the two repos that's orthogonal to upstream-Velero support but must be fixed in the same pass as adding an upstream scenario, or the upstream scenario will fail validation for an unrelated reason.

## 5. Test scenario sketch

Existing downstream scenario for reference (`mpqe-scale-scripts/oadp-helpers/templates/internal_data/tests.yaml`, lines 4967–4986 — note this predates the `65547f80` schema unification, see §4):

```yaml
- name: backup-kubevirt-2vm-csi-cephrbd-minimal
  testcase: 10.1
  testtype: backup
  result_dir_base_path: /tmp/results/
  dataset:
    role: kubevirt
    vm_profile: rhel9-hammerdb-mariadb
    vms_per_namespace: 2
    sc: ocs-storagecluster-ceph-rbd-virtualization
    pv_size: 60Gi
  args:
    plugin: csi              # PRE-UNIFICATION SCHEMA — current code requires plugin: kubevirt
    kubevirt_plugin: true
    use_cli: true
    OADP_CR_TYPE: backup
    OADP_CR_NAME: backup-kubevirt-2vm-csi-minimal
    backup_name: backup-kubevirt-2vm-csi-minimal
    testcase_timeout: 79200
    namespaces_to_backup: kubevirt-minimal-ns
    skip_source_dataset_check: true
```

Upstream-Velero equivalent, using the **current** schema (`plugin: kubevirt`, per `scenario.py`/`test_kubevirt.py`):

```yaml
- name: backup-kubevirt-2vm-upstream-cephrbd-minimal
  testcase: 10.4
  testtype: backup
  result_dir_base_path: /tmp/results/
  dataset:
    role: kubevirt
    vm_profile: rhel9-hammerdb-mariadb
    vms_per_namespace: 2
    sc: ocs-storagecluster-ceph-rbd-virtualization
    pv_size: 60Gi
  args:
    plugin: kubevirt                 # current unified schema; drives CSI-family backup path in exec_backup
    use_cli: true
    OADP_CR_TYPE: backup
    OADP_CR_NAME: backup-kubevirt-2vm-upstream-minimal
    backup_name: backup-kubevirt-2vm-upstream-minimal
    testcase_timeout: 79200
    namespaces_to_backup: kubevirt-upstream-minimal-ns
    skip_source_dataset_check: true
```

The `dataset`/`args` shape is **identical** to the downstream scenario — no `plugin_image`/`velero_binary`/upstream-specific arg is needed in the scenario YAML itself, because:
- Stream detection (`this_is_downstream()`) already happens from live cluster inspection, not from the scenario file (`environment.py::set_velero_stream_source`).
- `exec_backup`/`exec_restore` already branch on stream internally and need no new scenario args (§4).
- The only *new* work is on the setup side, not the scenario schema: `oadp_workloads.py`'s current `if self.this_is_downstream(): ... config_dpa_for_plugin(...)` block needs a parallel `else: ensure_kubevirt_plugin_installed(...)` (per §4's sketch) so an upstream run actually installs the plugin instead of silently assuming it. If that setup call is added, this scenario runs unmodified against either stream — the differentiator lives in Python, not YAML.

One thing that *would* need to differ is which cluster/test-run declares itself upstream — that's a `test_env`-level / CLI-invocation-level concern (`velero_cli_path`, `velero_ns`) already handled by existing environment plumbing, not something `tests.yaml` scenarios currently encode per-scenario.

## Implementation path

Recommended concrete next steps, roughly in dependency order:

1. **Fix the schema drift first** (small, unblocks everything else): update `mpqe-scale-scripts/oadp-helpers/templates/internal_data/tests.yaml`'s 5 existing kubevirt scenarios from `plugin: csi` + `kubevirt_plugin: true` to `plugin: kubevirt` to match the `65547f80` unification already merged in this repo. Requires an `[mpqe-MR]` per this repo's map.md convention.
2. **Add `ensure_kubevirt_plugin_installed()`** to `configuration.py` (upstream analog of `config_dpa_for_plugin`/`is_dpa_change_needed`, sketched in §4) and wire it into `oadp_workloads.py`'s existing `if self.this_is_downstream(): ... else: ...` structure at the same call site (currently line ~261).
3. **Add an `EnableCSI` feature-flag pre-check** for the upstream Velero Deployment, mirroring the downstream `is_dpa_change_needed()` CSI check, so a misconfigured upstream cluster fails fast with a clear error instead of a confusing plugin-registration failure.
4. **Add `KUBEVIRT_PLUGIN_IMAGE_UPSTREAM` (and ideally a small version-matrix comment/table, mirroring §2) to `constants.py`**, keyed to whatever Velero version `VELERO_CLI_PATH` currently points at (today: `v0.6.2` for Velero 1.12).
5. **Add the upstream scenario from §5** to `tests.yaml` (paired backup+restore, same pattern as the existing 5 downstream ones) once steps 1–3 land, and run a Tier-1 live test against an upstream Velero cluster (this repo doesn't appear to have one provisioned yet — check with whoever owns `/tmp/velero-1-12`'s installation for whether `--features=EnableCSI --use-node-agent` were used).
6. **No changes needed** to `exec_backup`/`exec_restore`/`vm_operations.py`/`vm_validation.py` — their upstream branches are already correct for kubevirt's "just works once registered" behavior; scope here is deliberately narrow (plugin *registration*, not plugin *behavior*).

## Sources

- [kubevirt/kubevirt-velero-plugin README](https://github.com/kubevirt/kubevirt-velero-plugin/blob/main/README.md) — install command, compatibility table, backup-methods caveat
- [kubevirt/kubevirt-velero-plugin Releases](https://github.com/kubevirt/kubevirt-velero-plugin/releases) — per-version Velero/KubeVirt bump history
- [Velero docs — Velero plugin system](https://velero.io/docs/main/overview-plugins/) — `velero plugin add`/`velero install --plugins`
- [Velero docs — Container Storage Interface Snapshot Support](https://velero.io/docs/main/csi/) — CSI plugin merged into core since v1.14, `--features=EnableCSI`
- [Velero docs — CSI Snapshot Data Movement](https://velero.io/docs/main/csi-snapshot-data-movement/) — `--use-node-agent`, `velero backup create --snapshot-move-data`, DataUpload/DataDownload CRs
- [Velero docs — File System Backup](https://velero.io/docs/main/file-system-backup/) — `--use-node-agent`, uploader-type distinction from CSI Data Mover
- [Velero docs — Basic Install](https://velero.io/docs/main/basic-install/) — `velero install` overview
- [OKD docs — Configuring OADP with OpenShift Virtualization](https://docs.okd.io/latest/backup_and_restore/application_backup_and_restore/installing/installing-oadp-kubevirt.html) — DPA `defaultPlugins: [kubevirt, csi, openshift, ...]`
- [OKD docs — Using OADP VMFR](https://docs.okd.io/4.22/backup_and_restore/application_backup_and_restore/oadp-vmfr/oadp-using-vmfr.html) — DPA kubevirt+csi+openshift example
- [openshift/oadp-operator — docs/config/vm_file_restore.md](https://github.com/openshift/oadp-operator/blob/oadp-dev/docs/config/vm_file_restore.md) — required-plugins statement
- In-repo: `benchmark_runner/oadp/{configuration,environment,execution,scenario,vm_operations}.py`, `constants.py`, `tests/unittest/benchmark_runner/oadp/test_kubevirt.py`, `git log -- benchmark_runner/oadp/scenario.py` (commit `65547f80`)
- External (ephemeral) repo: `/tmp/mpqe-scale-scripts/oadp-helpers/templates/{dpa.yaml.j2,internal_data/tests.yaml}`, `modify-dpa.yaml`

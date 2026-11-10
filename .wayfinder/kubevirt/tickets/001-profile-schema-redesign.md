# Profile Schema Redesign for Golden Images and Trigger Modes

**Label:** `wayfinder:grilling`  
**Status:** Closed  
**Blocked by:** (none)

## Question

How should the VM profile YAML schema be revised to support:

1. **Golden image management** — expressing where the golden DV lives (shared namespace + name), the import URL (qcow2), and the pull method
2. **Data generation trigger modes** — `auto` (image self-drives HammerDB on boot), `ssh` (OADP triggers FIO/custom via SSH), `none` (image arrives pre-populated)
3. **Completion signaling** — process name for polling + optional sentinel file path
4. **FIO replacing dd** — FIO configuration (dirs, files per dir, file size, timeout) instead of dd parameters

Current profile schema has `data_generation.method: hammerdb` with `config.database/tpcc_warehouses/tpcc_virtual_users`. This needs to expand to cover the trigger mode distinction, FIO config, and the golden image source pattern (check shared namespace, import from URL if absent, clone per VM).

The answer should be a concrete YAML schema that works for: (a) Linux HammerDB/MariaDB auto-start, (b) Linux HammerDB/PostgreSQL auto-start, (c) Linux FIO-only SSH-triggered, (d) Windows MSSQL+HammerDB+FIO auto-start, (e) a "none" profile for pre-populated VMs.

## Resolution

The revised profile schema splits image management from data generation, makes filesystem/mount_point optional (only for SSH-triggered formatting), and uses dual access methods (cloud-init for Linux, accessCredentials for Windows).

### Approved Schema

```yaml
name: rhel9-hammerdb-mariadb

os:
  type: linux          # linux | windows
  distribution: rhel9

image:
  url: "http://host/rhel9-hammerdb-mariadb.qcow2"
  golden_namespace: openshift-virtualization-os-images
  golden_name: rhel9-hammerdb-mariadb-golden
  storage_class: ocs-storagecluster-ceph-rbd-virtualization
  volume_mode: Filesystem    # Filesystem (Linux) | Block (Windows)
  root_size: 30Gi

resources:
  cpu_cores: 4
  memory: 8Gi

disks:
  - name: datadisk
    size: 60Gi
    source: blank
    # Optional — only when trigger=ssh and OADP must format:
    # filesystem: xfs
    # mount_point: /var/lib/mysql
    expected_capacity: "45G"
    validation_path: /var/lib/mysql

network:
  interfaces:
    - name: default
      model: virtio
      type: masquerade

access:
  method: ssh                    # ssh (Linux cloud-init) | accessCredentials (Windows qemu-ga)
  user: cloud-user
  ssh_authorized_keys: []

data_generation:
  trigger: auto                  # auto | ssh | none
  method: hammerdb               # hammerdb | fio | custom | none
  process_name: hammerdb         # pgrep pattern for completion polling
  timeout_minutes: 60
  includes_fio: false            # true = image handles FIO too; false = OADP triggers FIO if scenario requests
  config:                        # method-specific config
    database: mariadb
    tpcc_warehouses: 10
    tpcc_virtual_users: 4
```

### Key Design Decisions

1. **`image:` section** replaces `os.image_source` and `os.pull_method`. Adds `golden_namespace`, `golden_name`, `url`, `storage_class`, `volume_mode`, `root_size`.
2. **Golden image logic**: OADP checks for DV `golden_name` in `golden_namespace`. If absent, imports from `url`. Each VM's DataVolumeTemplate clones from the golden DV.
3. **`data_generation.trigger`**: `auto` = image self-drives, OADP polls `process_name` until exit. `ssh` = OADP invokes command, then polls. `none` = skip data gen entirely.
4. **`data_generation.method`**: explicit for dispatch logic. Values: `hammerdb`, `fio`, `custom`, `none`.
5. **`includes_fio`**: if `false` and scenario args contain `fio_fill_extra_disks: true`, OADP triggers FIO via SSH after primary data gen completes.
6. **FIO config lives in scenario args** (tests.yaml), not the profile — because FIO parameters are test-specific (how much data for THIS run), not image-specific.
7. **`access.method`**: `ssh` = Linux cloud-init key injection. `accessCredentials` = Windows qemu-guest-agent SSH key propagation.
8. **Disk `filesystem`/`mount_point`**: optional, only needed when `trigger: ssh` and OADP must format blank disks via cloud-init runcmd.
9. **Completion signaling**: process polling (`pgrep`) is the universal mechanism. Sentinel file is deferred as future enhancement.

### Scenario Args for FIO (in tests.yaml)

```yaml
args:
  fio_fill_extra_disks: true
  fio_dir_count: 5
  fio_files_per_dir: 10
  fio_file_size: "1G"
  fio_timeout_minutes: 30
```

### OADP FIO Trigger Logic

```
if scenario.args.fio_fill_extra_disks == true
   AND profile.data_generation.includes_fio == false:
    # Wait for primary data gen to complete first
    wait_for_process_exit(profile.data_generation.process_name)
    # Then trigger FIO on extra disks via SSH
    trigger_fio(scenario.args.fio_*)
    wait_for_process_exit("fio")
```

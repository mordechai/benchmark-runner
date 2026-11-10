# Multi-Disk VM Profiles and Volume Mode Alignment

**Label:** `wayfinder:task`  
**Status:** Open  
**Blocked by:** [Cloud-Init Lifecycle Management](025-cloud-init-lifecycle-management.md)

## Question

Implement profiles with 2+ data disks and align volume mode / access mode with what the shell scripts use. Two related concerns:

**Multi-disk profiles:**

The VM template already supports multi-disk via loop; `VMsPopulation.sh` demonstrates the pattern with `-disks N` (N data disks, OS disk always 1). Validation must iterate all disks with independent `expected_capacity`, `validation_path`, and `expected_file_count`.

Key decisions:
1. How does each disk's data gen work? VMsPopulation.sh fills all data disks identically (105k files each). HammerDB only writes to disk 1 (its mount point). Should profiles express per-disk data gen, or is "all data disks get identical fill" the default?
2. `disk_overrides` in scenario — does it override per-disk `expected_capacity` only, or also data gen parameters?
3. Validation: per-disk du/df + file count checks via SSH, reported individually in the JSON summary

**Volume mode alignment (plan 2.5):**

Update `vm_template.yaml` and `dv_template.yaml` to support both modes:
- `volumeMode: Block` with `ReadWriteMany` (used by VMsPopulation.sh — the operational standard)
- `volumeMode: Filesystem` with `ReadWriteOnce` (current Python pipeline default)

Make these configurable via the profile YAML. The profile schema from ticket 001 has `image.volume_mode` — extend to per-disk support:

```yaml
disks:
  - name: datadisk
    size: 150Gi
    volume_mode: Block        # Block | Filesystem (default)
    access_mode: ReadWriteMany # RWX | RWO (default)
```

Resolve: at least one multi-disk profile (2+ data disks) created, volume mode configurable, template renders both modes, unit tests cover multi-disk validation and volume mode variants.

# Hot-Plug Disk Backup/Restore Scenarios

**Label:** `wayfinder:research`  
**Status:** Open  
**Blocked by:** [Tier 1 Live Test](006-tier1-live-test.md)

## Question

Investigate how the Velero kubevirt plugin handles hot-plugged disks (added via `virtctl addvolume` after VM creation):

1. Are hot-plugged disks included in the backup? (They're not in the VM spec's `dataVolumeTemplates`)
2. After restore, are hot-plugged disks restored and re-attached?
3. Does the plugin distinguish between spec-defined disks and hot-plugged disks?
4. What validation is needed post-restore for hot-plugged disks?
5. Does the OADP code need to track which disks were hot-plugged vs. original?

This research determines whether hot-plug scenarios are feasible with the current plugin or require plugin-level changes.

Deliverable: a summary of findings with clear go/no-go for hot-plug disk scenario support.

# Cloud-Init Lifecycle Management

**Label:** `wayfinder:task`  
**Status:** Open  
**Blocked by:** [Data Generation Model Reconciliation](007-data-gen-model-reconciliation.md), [SSH Key Secret Management](024-ssh-key-secret-management.md)

## Question

Implement cloud-init removal and VM spec post-update in the native Python pipeline, matching the post-population lifecycle in `VMsPopulation.sh`. This is critical for backup/restore correctness — cloud-init data should not be part of the backup, and VM resources should be reduced after data population completes.

**Cloud-init removal (plan 2.3):**

Add `_remove_cloud_init_from_vms(namespace)` to `vm_operations.py` matching `RemoveCloudInit()` in the script:

1. Power off all VMs in namespace via `virtctl stop`
2. For each VM:
   - Patch to remove `kubectl.kubernetes.io/last-applied-configuration` annotation
   - Patch DV source from `pvc` clone to `blank: {}` (prevent re-import from golden)
   - Find and remove cloud-init disk from `spec.template.spec.domain.devices.disks` (by name `cloudinitdisk`)
   - Find and remove cloud-init volume from `spec.template.spec.volumes` (by name `cloudinitdisk`)
3. Power on all VMs
4. Wait for all VMIs to reach Running state

**VM spec post-update (plan 2.4):**

Add `_update_vm_resources_post_population(namespace, profile)` to `vm_operations.py` matching `UpdateVMspec()`:

1. After cloud-init removal, patch each VM to reduce CPU and memory
2. Default: 4 cores -> 2 cores, 4Gi -> 2Gi (matching script behavior)
3. Make target values configurable via profile (`resources.post_population.cpu_cores`, `resources.post_population.memory`)
4. This reduces resource consumption during the (potentially long) backup/restore phase

**Integration into `create_vm_dataset()` flow:**

After data population completes and is verified:
```
create_vm_dataset() -> _populate_vms_with_data() -> _remove_cloud_init_from_vms() -> _update_vm_resources_post_population()
```

Resolve: both methods implemented, VM lifecycle matches script behavior, VMs boot cleanly after cloud-init removal, unit tests cover the patch operations.

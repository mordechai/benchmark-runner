# CBT Native Python Implementation

**Label:** `wayfinder:task`  
**Status:** Open  
**Blocked by:** [CBT Script Integration](021-cbt-script-integration.md)

## Question

Implement the CBT (Changed Block Tracking) lifecycle natively in Python, replacing the dependency on `VMsEnableCBT.sh`. The script's CBT flow is: power off -> label -> power on -> expand persistent-state PVCs -> restart.

Implementation:

1. `_enable_cbt_labels(namespace)` — for each VM: `oc label vm <name> -n <namespace> changedBlockTracking=true --overwrite`

2. `_expand_cbt_pvcs(namespace)` — for each PVC matching `persistent-state`: `oc patch pvc <name> -n <namespace> --type=merge -p '{"spec":{"resources":{"requests":{"storage":"500Mi"}}}}'`

3. `_cbt_lifecycle_cycle(namespace)`:
   ```
   virtctl stop <vm> for all VMs
   wait for all VMIs to terminate
   _enable_cbt_labels(namespace)
   virtctl start <vm> for all VMs
   wait for all VMIs Running
   _expand_cbt_pvcs(namespace)
   virtctl restart <vm> for all VMs
   wait for all VMIs Running
   ```

4. `_verify_cbt_state(namespace)` — for each VM: check `status.changedBlockTracking.state` and `metadata.labels.changedBlockTracking` via jsonpath, return structured pass/fail per VM

5. Add `cbt: true/false` to scenario args schema and profile schema

6. Wire into `create_vm_dataset()` native flow: after data population + cloud-init removal, if `cbt: true`, run `_cbt_lifecycle_cycle()` then `_verify_cbt_state()`

Resolve: native CBT implementation matches script behavior, unit tests cover the lifecycle phases, CBT state verification passes.

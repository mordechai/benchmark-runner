# Wire Virtctl Helpers into VM Lifecycle (G-07)

**Label:** `wayfinder:task`  
**Status:** Open  
**Blocked by:** [Tier 1 Live Test](006-tier1-live-test.md)

## Question

Migrate `vm_operations.py` and `cleanup.py` from raw `OC.run(cmd=...)` string commands to `Virtctl` high-level helpers: `create_vm_sync`, `stop_vm_sync`, `delete_all_vms`, `wait_for_dv_status`, etc. The `_get_virtctl()` lazy initializer already exists but is never called — wire it in.

Benefits: shared timeout/retry logic, consistent error handling, less string-command surface area.

Resolve: refactored code, all unit tests pass, confirmed no behavioral change via Tier 1 live test comparison.

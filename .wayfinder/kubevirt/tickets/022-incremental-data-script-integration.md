# Incremental Data Script Integration

**Label:** `wayfinder:task`  
**Status:** Resolved  
**Blocked by:** [Script Integration Dispatch](019-script-integration-dispatch.md)

## Question

Integrate `VMsAddDataVerify.sh`'s incremental data generation capability into the benchmark-runner flow, enabling incremental backup test scenarios.

The script supports 3 increment levels on top of the baseline:

| Increment | OS disk `/home/data` | Per data disk `/mnt-vd*/data` |
|-----------|---------------------|-------------------------------|
| baseline  | 157,000 files (~45G) | 105,000 files (~30G) |
| inc1      | +5,000 files (~1.45G) | +10,500 files (~3G) |
| inc2      | +3,000 files (~0.85G) | +52,500 files (~15G) |
| inc3      | +15,000 files (~4.3G) | +70,000 files (~20G) |

Implementation:

1. Add `add_incremental_data(namespace, increment_level)` to `vm_operations.py`:
   - Invokes `{MPQE_VM_SCRIPTS_PATH}/VMsAddDataVerify.sh -ns <namespace> -inc <inc1|inc2|inc3>`
   - Script generates the incremental files via `virtctl ssh` + `sshpass`
   - Validates exit status

2. Add `increment_level` to scenario args:
   - `increment_level: inc1` (optional, only for incremental backup scenarios)
   - Validate against allowed values: `inc1`, `inc2`, `inc3`

3. Wire into the workload flow:
   - If `increment_level` is present in scenario args, call `add_incremental_data()` before backup execution
   - This enables a test pattern: run baseline backup, add inc1 data, run incremental backup

4. Post-increment verification:
   - After adding incremental data, invoke `VMsAddDataVerify.sh -ns <namespace> -verify-only` (from ticket 020) to confirm the increment was applied

Resolve: method implemented, increment levels validated, wired into pre-backup flow, unit tests cover argument construction.

## Answer

Added `add_incremental_data(namespace, increment_level)` to `vm_operations.py`: invokes `VMsAddDataVerify.sh -ns <namespace> -inc <level>` via `_run_vm_script()` (fatal on non-zero exit). Note: the script itself already generates *and* self-verifies in one invocation whenever `-inc` is given without `-verify-only` (confirmed by reading its `main()`), so the exit-code check alone already catches generation failures.

Schema: `increment_level` (optional) validated in `scenario.py`'s new `_validate_increment_level()` — must be one of `inc1`/`inc2`/`inc3` (`KNOWN_INCREMENT_LEVELS`, added in ticket 019's constants pass), and requires `vm_population_method: script` (VMsAddDataVerify.sh's increment folders assume the disk layout only `VMsPopulation.sh` creates — a native-populated VM has no matching baseline to increment against).

Workflow wiring: added `apply_incremental_data_if_requested(scenario)` to `vm_operations.py`, called from `oadp_workloads.py`'s `run_workload()` immediately after `verify_vm_datasets_before_backups()` and before `oadp_execute_scenario()` — i.e. before backup execution, and only for VM-dataset backup scenarios (mirrors where `verify_vm_datasets_before_backups` itself is gated). It's a no-op when `increment_level` is absent (the normal path). When present, it iterates every namespace in the scenario (`ds_get_all_namespaces()`) and, per namespace, calls `add_incremental_data()` then an explicit **post-increment verification** via ticket 020's `verify_vm_data_via_script()` (plan item 4) — a deliberate second, explicit boolean confirmation beyond the script's own embedded verify, calling `fail_test_run()` with a clear message if it disagrees.

Added 15 unit tests: scenario validation (absent is valid, all three levels valid with `vm_population_method: script`, unknown level rejected, rejected when method isn't `script`), `add_incremental_data` command construction and fail-on-nonzero-exit, and `apply_incremental_data_if_requested` no-op/dispatch/multi-namespace/failure-propagation behavior (new `TestableIncrementalData` fixture combining the vm-ops, vm-validation, and scenario mixins, since this method spans all three). Full repo suite: 291 passed (13 new since ticket 021's 278).

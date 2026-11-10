# CBT Script Integration

**Label:** `wayfinder:task`  
**Status:** Resolved  
**Blocked by:** [Script Integration Dispatch](019-script-integration-dispatch.md)

## Question

Integrate Changed Block Tracking (CBT) support by wrapping `VMsEnableCBT.sh` and `VMsCBTstate.sh` as callable methods from the benchmark-runner flow.

CBT is a KubeVirt feature that tracks block-level changes between backups, enabling incremental backup efficiency. The scripts implement a specific lifecycle: power off VMs, add CBT label, power on, expand persistent-state PVCs to 500Mi, restart VMs.

Implementation:

1. Add `enable_cbt_on_vms(namespace)` to `vm_operations.py`:
   - Invokes `{MPQE_VM_SCRIPTS_PATH}/VMsEnableCBT.sh -ns <namespace>`
   - Script handles the full cycle: power off, label, power on, expand PVC, restart
   - Validate exit status

2. Add `check_cbt_state(namespace)` to `vm_operations.py`:
   - Invokes `{MPQE_VM_SCRIPTS_PATH}/VMsCBTstate.sh -ns <namespace>`
   - Parses output for per-VM CBT state and label status
   - Returns structured result for reporting/validation

3. Wire into VM dataset creation flow:
   - When `cbt: true` in `vm_population_script_args`, call `enable_cbt_on_vms()` after `create_vm_dataset_via_script()` completes (if VMsPopulation.sh was called without `-cbt`), or pass `-cbt` to VMsPopulation.sh directly
   - Add CBT state verification as a post-creation check
   - Record CBT state in run_metadata for reporting

4. Add `cbt` field to scenario args validation in `scenario.py`

Note: `VMsEnableCBT.sh` also has a hardcoded `ClusterLogin()` — same concern as ticket 018.

Resolve: both methods implemented, CBT can be enabled on script-populated VMs, state verified, unit tests cover CBT flag propagation.

## Answer

Added both methods to `vm_operations.py`:

- `enable_cbt_on_vms(namespace)` — invokes `VMsEnableCBT.sh -ns <namespace>` via the same `_run_vm_script()` helper used for `VMsPopulation.sh` (fatal on non-zero exit). For enabling CBT on VMs that already exist outside the creation flow — the primary path (`create_vm_dataset_via_script`, ticket 019) already passes `-cbt` directly to `VMsPopulation.sh`, so this is the "or" alternative named in the ticket, not a second call layered on top of it.
- `check_cbt_state(namespace)` — invokes `VMsCBTstate.sh -ns <namespace>` and parses its per-VM block output (`Verify CBT state and label for VM: <name> , NameSpace: ...` / `CBT State: ...` / `CBT label: ...`) into `{vm_name: {"state": ..., "label": ...}}` via three line-anchored regexes.

Wired into the creation flow: `create_vm_dataset_via_script()` now calls `check_cbt_state()` immediately after `VMsPopulation.sh` completes, but only when `cbt: true` was requested (skipped entirely otherwise — no wasted `oc` calls for the common non-CBT case). The result is recorded via a new `_record_cbt_state()` helper into `run_metadata["summary"]["validations"]["cbt_state"][namespace]`, matching the existing `run_metadata["summary"]["validations"][...]` convention used elsewhere (e.g. `vm_dataset_already_present` in `vm_validation.py`).

The `cbt` field in `vm_population_script_args` was already validated (as a required-boolean, SEC-002-checked field) in ticket 019's `_validate_vm_population_args()` — no separate schema change was needed here.

`VMsEnableCBT.sh` and `VMsCBTstate.sh`'s hardcoded `ClusterLogin()` was already fixed as part of ticket 018 (applied to all four scripts sharing that helper, not just the two named in ticket 018's own scope).

Added 12 unit tests: `enable_cbt_on_vms` command construction and fail-on-nonzero-exit; `check_cbt_state` multi-VM parsing, exact command, empty-output, and missing-label-line fallback; `_record_cbt_state` single/multi-namespace accumulation; and two `create_vm_dataset_via_script` wiring tests (state checked+recorded when `cbt: true`, skipped when `cbt: false`). Full repo suite: 278 passed (10 new since ticket 020's 268).

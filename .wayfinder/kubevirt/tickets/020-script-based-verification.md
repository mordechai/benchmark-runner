# Script-Based VM Data Verification

**Label:** `wayfinder:task`  
**Status:** Resolved  
**Blocked by:** [Script Integration Dispatch](019-script-integration-dispatch.md)

## Question

Add `verify_vm_data_via_script()` to `vm_validation.py` that invokes `VMsAddDataVerify.sh` in verify-only mode as an alternative to the native df-based capacity check.

Implementation:

1. Add `verify_vm_data_via_script(namespace)` method:
   - Invokes `{MPQE_VM_SCRIPTS_PATH}/VMsAddDataVerify.sh -ns <namespace> -verify-only`
   - Parses stdout for "VERIFY PASSED" or "VERIFY FAILED" outcome
   - Returns boolean for validation flow integration
   - Captures full script output to run artifacts for debugging

2. Wire into `validate_expected_vm_datasets()`:
   - When `vm_population_method: script`, use `verify_vm_data_via_script()` instead of `_validate_vm_data_sample()` / `_validate_vm_data_all()`
   - VM count and running state checks (existence checks) still use the existing Python logic regardless of population method
   - The script handles per-disk file count + capacity verification internally

3. Wire into `verify_vm_datasets_before_backups()`:
   - Script-based verification replaces df-based checks for script-populated VMs
   - If verification fails, the existing recreate-and-revalidate flow still applies

The script verifies baseline data counts: 157,000 files on OS disk `/home/data`, 105,000 files per data disk `/mnt-vd*/data`, plus capacity checks with `du`.

Resolve: method implemented, stdout parsing tested, wired into validation flow, unit test covers VERIFY PASSED and VERIFY FAILED parsing.

## Answer

Added `verify_vm_data_via_script(namespace)` to `vm_validation.py`: runs `VMsAddDataVerify.sh -ns <namespace> -verify-only` via `self.__ssh.run()`, parses the output for the `VERIFY PASSED`/`VERIFY FAILED` markers the script always prints (treating neither-marker output — e.g. a script crash — as failed rather than raising), and unconditionally saves the raw output to `{run_artifacts_path}/vm_script_verify_<namespace>.log` for debugging regardless of outcome.

Wired into `_validate_vm_dataset()` (the single call site both `validate_expected_vm_datasets()` and `verify_vm_datasets_before_backups()` funnel through): existence checks (VM count, VMI running) still run unconditionally; `validation_mode: none` still short-circuits before any data check; when `vm_population_method: script`, the light/full df-based checks (`_validate_vm_data_sample`/`_validate_vm_data_all`) are replaced by `verify_vm_data_via_script()` instead. No changes were needed to `verify_vm_datasets_before_backups()` itself — its recreate-and-revalidate flow already calls `validate_expected_vm_datasets()` on both passes, so a script-verification failure flows through the same recreate path a native-verification failure would.

Added 9 unit tests: `VERIFY PASSED`/`VERIFY FAILED`/neither-marker parsing, exact command construction, artifact-file writing (and safe no-op when `run_artifacts_path` is unset), plus three `_validate_vm_dataset` dispatch tests confirming script-populated datasets call `verify_vm_data_via_script()` (not the native sample check), native datasets still use the native check, and `validation_mode: none` skips both. Full repo suite: 268 passed (9 new since ticket 019's 259).

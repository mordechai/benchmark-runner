# Script-Based Kubevirt Scenarios and Unit Tests

**Label:** `wayfinder:task`  
**Status:** Resolved  
**Blocked by:** [Script-Based VM Verification](020-script-based-verification.md), [CBT Script Integration](021-cbt-script-integration.md), [Incremental Data Script Integration](022-incremental-data-script-integration.md)

## Question

Add concrete `tests.yaml` scenarios that exercise the script-based VM population path, and extend `test_kubevirt.py` unit tests to cover all Phase 1 integration points.

**Scenarios to add to tests.yaml:**

1. `backup-kubevirt-script-2vm-1data-cephrbd` — minimal script-based backup (2 VMs, 1 data disk each)
2. `restore-kubevirt-script-2vm-1data-cephrbd` — matching restore
3. `backup-kubevirt-script-2vm-1data-cbt-cephrbd` — with CBT enabled
4. `backup-kubevirt-script-10vm-1data-cephrbd` — scale scenario (10 VMs, matching script defaults)
5. `backup-kubevirt-script-2vm-2data-cephrbd` — multi data disk (2 VMs, 2 data disks each)

Each scenario uses `vm_population_method: script` and `vm_population_script_args` with `data_disks` (data disks per VM; OS disk always 1, implicit).

**Unit tests to add/extend in test_kubevirt.py:**

1. Test `vm_population_method: script` dispatch routes to `create_vm_dataset_via_script()`
2. Test `vm_population_method: native` (or absent) routes to `create_vm_dataset()`
3. Test script argument construction: `-vms`, `-disks` (from `data_disks`), `-namespace`, `-cbt` flag mapping
4. Test SEC-002 validation on `vm_population_script_args` values
5. Test `data_disks` validation (non-negative integer, 0 = OS only)
6. Test CBT flag propagation through scenario args to `enable_cbt_on_vms()`
7. Test verification script output parsing ("VERIFY PASSED" / "VERIFY FAILED")
8. Test incremental data level validation (`inc1`/`inc2`/`inc3` only)
9. Test scenario validation rejects unknown `vm_population_method` values
10. Test script-based scenarios pass `validate_scenario()`

**`[mpqe-MR]`** — The new `tests.yaml` scenarios live in `/tmp/mpqe-scale-scripts/oadp-helpers/templates/internal_data/tests.yaml`, which is a separate repository. A commit and MR must be submitted on `mpqe-scale-scripts` for the scenario additions to persist. The unit test additions in `test_kubevirt.py` are in the `benchmark-runner` repo and follow the normal commit flow.

Resolve: at least 5 new scenarios in tests.yaml (mpqe-MR submitted), at least 10 new unit tests passing in benchmark-runner, existing tests unbroken.

## Answer

Added the 5 requested scenarios to `tests.yaml` under a new "Script-based KubeVirt scenarios" section, all using `sc: ocs-storagecluster-ceph-rbd` (the storage class `VMsPopulation.sh` actually hardcodes — not the `-virtualization` variant the native path uses) and namespaces ending in `-ns1` (required by ticket 019's schema). `vm_profile` is set to an existing profile name for schema compliance only; it's never read on the script-population path.

**Pre-existing bug found and fixed along the way:** while validating the new scenarios I discovered all 5 *existing* kubevirt scenarios in `tests.yaml` were already failing `validate_scenario()` — they still use the retired `plugin: csi` + `kubevirt_plugin: true` pair, but benchmark-runner commit `65547f80` (made outside this ticket chain, same day) migrated the check to require `plugin: kubevirt` directly and dropped the separate flag. This wasn't tracked by any wayfinder ticket and would have silently blocked every kubevirt scenario — script-based or native — from passing validation, including ticket 006's live test. Fixed all 5 in the same MR.

`[mpqe-MR]`: [!203](https://gitlab.cee.redhat.com/mlehrer/mpqe-scale-scripts/-/merge_requests/203) (same branch as ticket 018, one commit added on top). Verified all 10 kubevirt scenarios (5 existing + 5 new) pass `validate_scenario()` by loading the real `tests.yaml` file directly.

Unit tests — cross-referencing the 10 requested items against what tickets 019-022 already covered, then filling the 2 gaps:

1-5, 7-9 were already covered by `TestScriptDispatchRouting`, `TestCreateVmDatasetViaScript`, `TestVmPopulationMethodValidation`, `TestVerifyVmDataViaScript`, `TestIncrementLevelValidation` (tickets 019-022).
6. Added `TestCbtFlagPropagationFromScenario` (2 tests). Note: this implementation propagates `cbt: true` via the `-cbt` flag passed directly to `VMsPopulation.sh` (the alternative wiring option from ticket 021), rather than a separate `enable_cbt_on_vms()` call — `check_cbt_state()` confirms enablement instead. `enable_cbt_on_vms()` remains available standalone and is tested on its own in `TestEnableCbtOnVms`.
10. Added `TestScriptBasedScenariosFromTestsYaml` (6 tests) — mirrors the 5 new `tests.yaml` scenarios as literal scenario dicts and asserts they pass `validate_scenario()`, so a regression in either the scenario shape or the validator is caught without needing the external repo checked out.

8 new tests added (191 kubevirt / 299 total repo-wide, all passing, 0 lint errors). Committed to `benchmark-runner` on branch `OADP-5339-kubevirt-support-dv` (commit `31caa734`, bundled with tickets 019-022 which hadn't been committed yet).

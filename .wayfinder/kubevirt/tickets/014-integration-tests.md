# Integration Tests for KubeVirt Scenarios

**Label:** `wayfinder:task`  
**Status:** Open  
**Blocked by:** [Script-Based Scenarios and Unit Tests](023-script-based-scenarios-and-tests.md)

## Question

Add integration tests that exercise the full scenario-load -> profile-resolve -> feature-gate -> dataset-dispatch path. Current `test_kubevirt.py` covers unit-level but nothing exercises the integration seams. With Phase 1 script integration complete (ticket 023), integration tests should cover both population methods.

Scope:

1. Test scenario loading with `role: kubevirt` against mock OC responses — both `vm_population_method: script` and `vm_population_method: native`
2. Test script dispatch: scenario with `vm_population_method: script` calls `create_vm_dataset_via_script()` with correct argument construction (`-vms`, `-disks` from `data_disks`, `-namespace`, `-cbt`)
3. Test native dispatch: scenario without `vm_population_method` (or `native`) calls `create_vm_dataset()` with correct profile resolution
4. Test profile resolution (including golden image check logic once implemented)
5. Test feature flag gating end-to-end (scenario rejected when flag off)
6. Test DPA configuration path for kubevirt plugin
7. Test verification dispatch (script-based vs native df-based)
8. Test CBT flag propagation through the full flow
9. Determine: can these run without a real cluster (mocked), or do they need a real cluster?

Resolve: test scaffolding created, at least 8 integration tests passing in CI.

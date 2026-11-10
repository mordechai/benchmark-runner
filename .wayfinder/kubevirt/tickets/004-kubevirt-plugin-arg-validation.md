# Add kubevirt_plugin Arg Validation (G-09)

**Label:** `wayfinder:task`  
**Status:** Closed  
**Blocked by:** (none)

## Question

Add scenario validation in `scenario.py` that when any dataset has `role: kubevirt`, the scenario's `args` must contain `kubevirt_plugin: true`. Fail fast with a descriptive error message if missing — prevents silent incomplete backups where Velero runs without the kubevirt plugin.

Resolve: implement validation, add unit test coverage, confirm existing tests pass.

## Resolution

Already implemented in commit `6c8379e5` ("fix(kubevirt): safe-rollout defaults and plugin arg validation (G-08/G-09)").

Verified at `scenario.py:127-134`: `scenario_includes_kubevirt_dataset(scenario)` check followed by `args.get("kubevirt_plugin", False)` validation. Returns `False` (fails validation) with descriptive error: "Scenario with dataset role 'kubevirt' requires 'kubevirt_plugin: true' in args." Unit test `test_kubevirt_missing_kubevirt_plugin_arg_fails` confirms behavior.

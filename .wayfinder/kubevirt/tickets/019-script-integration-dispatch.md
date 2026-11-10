# Script Integration Dispatch in vm_operations.py

**Label:** `wayfinder:task`  
**Status:** Resolved  
**Blocked by:** [VMsPopulation.sh Namespace Parameter](018-vmspopulation-namespace-parameter.md)

## Question

Wire the shell scripts into the benchmark-runner OADP execution flow. This is the core integration ticket — schema, constants, invocation method, and dispatch routing.

Three changes in one session:

**1. Scenario schema extension (plan 1.2)**

Add `vm_population_method` and `vm_population_script_args` to scenario args:

```yaml
args:
  vm_population_method: script    # "script" | "native" (default: "native")
  vm_population_script_args:
    vms: 2
    data_disks: 1                 # data disks only; OS disk always 1
    cbt: false
```

Update `scenario.py` `validate_scenario()` to:
- Accept `vm_population_method` as valid arg
- When `vm_population_method: script`, require `vm_population_script_args` with `vms` and `data_disks`
- Validate `data_disks` is a non-negative integer (0 = OS disk only)
- Validate `vm_population_script_args` values for shell metacharacters (SEC-002)

**2. Constants and invocation (plan 1.3)**

Add `MPQE_VM_SCRIPTS_PATH` to `constants.py`:
```python
MPQE_VM_SCRIPTS_PATH = "/tmp/mpqe-scale-scripts/oadp-helpers/Scripts"
```

Add `create_vm_dataset_via_script()` to `vm_operations.py`:
- Build command: `{MPQE_VM_SCRIPTS_PATH}/VMsPopulation.sh -vms N -disks N -ns 1 -namespace <ns>` + optional `-cbt`
- Map `data_disks` in scenario to `-disks` flag (data disks per VM; OS disk implicit)
- Invoke via `self.__ssh.run()` (long-running, script blocks until VMs are ready)
- Check exit status; on failure call `self.fail_test_run()`

**3. Dispatch routing (plan 1.4)**

In `create_source_dataset()` (`dataset.py`) or `create_vm_dataset()` (`vm_operations.py`), route based on `vm_population_method`:
```python
if scenario["args"].get("vm_population_method") == "script":
    self.create_vm_dataset_via_script(scenario, ds)
else:
    self.create_vm_dataset(scenario, ds)  # existing native flow
```

Resolve: schema validated, constants added, invocation method works with mock scenario, dispatch routes correctly, unit tests cover argument construction and SEC-002 validation.

## Answer

Implemented all three pieces:

**1. Schema** (`scenario.py`): `_validate_vm_population_args()` validates `vm_population_method` (defaults to `native`, rejects unknown values) and, when `script`, requires `vm_population_script_args` with `vms` (positive int) and `data_disks` (non-negative int, 0 = OS disk only), validates optional `cbt` (bool), and rejects shell metacharacters in any string values (SEC-002). A second validator, `_validate_script_population_namespace()`, enforces a namespace-naming contract needed to make `-namespace` round-trip correctly (see below): the dataset's effective namespace must end with `-ns1`.

**2. Constants** (`constants.py`): added `MPQE_VM_SCRIPTS_PATH`, `VM_POPULATION_METHOD_NATIVE`/`_SCRIPT`, `KNOWN_VM_POPULATION_METHODS`, and `MPQE_VM_SCRIPT_NS_SUFFIX`.

**3. Invocation + dispatch** (`vm_operations.py`, `dataset.py`, `vm_validation.py`): added `create_vm_dataset_via_script()`, which builds `VMsPopulation.sh -vms N -disks N -ns 1 -namespace <base>` (+ `-cbt` when requested) and runs it via `self.__ssh.run()` (the same raw-shell client used everywhere else in the OADP module for external script invocation — `_oc` is reserved for structured `oc`/`virtctl` operations). `create_source_dataset()` in `dataset.py` and the dataset-recreation path in `vm_validation.py`'s `verify_vm_datasets_before_backups()` both now route to `create_vm_dataset_via_script()` vs. the existing `create_vm_dataset()` based on `vm_population_method`.

**Namespace round-trip note:** `VMsPopulation.sh`'s multi-NS loop always appends `-ns<i>` to whatever base name it's given — even at `-ns 1` (see [VMsPopulation.sh Namespace Parameter](018-vmspopulation-namespace-parameter.md)'s resolution). So the scenario's dataset namespace is required (validated) to already end in `-ns1`, and `_mpqe_namespace_base()` strips that suffix before passing `-namespace <base>` to the script — the two round-trip exactly.

**Exit-status handling:** `SSH.run()`'s default path returns merged stdout/stderr with no return code, and its `is_check=True` path imposes a short subprocess timeout unsuitable for a script that can run 20+ minutes. `_run_vm_script()` works around both by appending `; echo MPQE_SCRIPT_EXIT_CODE:$?` to the command and parsing the marker from output, calling `fail_test_run()` on non-zero (or missing/unparseable) exit codes.

Added 36 new unit tests (`test_kubevirt.py`, all passing, 259 total in the module / full repo suite green) covering: method/args schema validation (defaults, unknown method, missing/malformed `vms`/`data_disks`/`cbt`, metacharacter rejection, namespace-suffix contract for both dict and list dataset forms), `_mpqe_namespace_base()` suffix stripping, `create_vm_dataset_via_script()` command construction (vms/disks/cbt flags, defaults) and exit-code handling (success, non-zero, missing marker), and dispatch routing in both `create_source_dataset()` and the validation recreate-path for both `native` and `script` methods.

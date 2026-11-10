# Add Test Scenario and Update DPA Playbook (E-01 + E-02)

**Label:** `wayfinder:task`  
**Status:** Closed  
**Blocked by:** (none)

## Question

Two external changes in `/tmp/mpqe-scale-scripts`:

1. **E-01**: Add at least one `role: kubevirt` scenario to `oadp-helpers/templates/internal_data/tests.yaml` with all required fields (`vm_profile`, `vms_per_namespace`, `kubevirt_plugin: true`, `result_dir_base_path`, etc.)
2. **E-02**: Update `oadp-helpers/modify-dpa.yaml` ansible playbook to handle the `kubevirt_plugin=true` extra-var — add `kubevirt` to `defaultPlugins` list in the DPA Jinja2 template when this var is true.

Resolve: both changes made and validated syntactically.

## Resolution

**E-01 — Already done.** tests.yaml contains 5 kubevirt scenarios (lines 4962-5079):
- `backup-kubevirt-2vm-csi-cephrbd-minimal` (minimal, skip_source_dataset_check)
- `restore-kubevirt-2vm-csi-cephrbd-minimal` (matching restore)
- `backup-kubevirt-hammerdb-mariadb-5vm-cephrbd` (5 VMs with data validation)
- `restore-kubevirt-hammerdb-mariadb-5vm-cephrbd`
- `backup-kubevirt-hammerdb-postgres-5vm-cephrbd`

**E-02 — Fixed this session.** Added Jinja2 conditional to `dpa.yaml.j2`:

```jinja2
{%+ if kubevirt_plugin | default(false) | bool +%}
     - kubevirt
{%+ endif +%}
```

The full chain:
1. `configuration.py:132-133` — builds `kubevirt_plugin=true` when `_scenario_needs_kubevirt_plugin()` is True
2. `modify-dpa.yaml:22` — declares `kubevirt_plugin: false` default (overridden by extra-var)
3. `dpa.yaml.j2:30-32` — conditionally renders `- kubevirt` in `spec.configuration.velero.defaultPlugins`

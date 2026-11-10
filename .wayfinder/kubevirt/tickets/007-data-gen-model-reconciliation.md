# Data Generation Model Reconciliation

**Label:** `wayfinder:grilling`  
**Status:** Open  
**Blocked by:** [Script-Based Scenarios and Unit Tests](023-script-based-scenarios-and-tests.md)

## Question

The current `_populate_vms_with_data()` and `_build_data_gen_command()` in `vm_operations.py` assume SSH-triggered data generation via `virtctl ssh`. However, `VMsPopulation.sh` demonstrates a different, proven model: **cloud-init `runcmd` data generation** where dd commands run as part of the VM's first boot and the script waits for `cloud-init status --wait` to signal completion.

Now that Phase 1 script integration validates the cloud-init approach end-to-end, this ticket resolves how to align the native Python pipeline with that model:

1. **Cloud-init data gen in vm_template.yaml** — Add dd-based file generation to the cloud-init `runcmd` block, matching VMsPopulation.sh:
   - OS disk: `for x in {1..157000}; do dd if=/dev/urandom of=/home/data/file$x bs=1k count=300; done`
   - Per data disk: `for x in {1..105000}; do dd if=/dev/urandom of=${diskname}/data/file$x bs=1k count=300; done`
   - Data disk partitioning via `parted`/`mkfs.ext4` already partially exists in template

2. **Trigger model revision** — The profile schema (ticket 001) defined `data_generation.trigger: auto|ssh|none`. Cloud-init `runcmd` is a third category: `cloud_init` — data gen is embedded in the VM spec at creation time, not auto-started by the image nor SSH-triggered after boot. Should `trigger: cloud_init` be added, or does `auto` cover this case (since the VM self-drives data gen on boot)?

3. **Completion signaling** — VMsPopulation.sh uses `cloud-init status --wait` via `virtctl ssh` to detect completion. The Python pipeline should adopt this instead of `pgrep`-based process polling for the `runcmd` case.

4. **File count and size configurability** — VMsPopulation.sh hardcodes 157k/105k file counts and 300KB file size. Should these be configurable via profile `data_generation.config` or scenario args?

5. **What happens to HammerDB auto-start?** — HammerDB golden images auto-start on boot (trigger: `auto`). This is a different use case from cloud-init runcmd dd fill. Both models should coexist — the dispatch in `_populate_vms_with_data()` should route based on trigger type.

Resolve: decision on trigger model taxonomy, vm_template.yaml updated with cloud-init data gen blocks, completion signaling via `cloud-init status --wait`, unit tests updated.

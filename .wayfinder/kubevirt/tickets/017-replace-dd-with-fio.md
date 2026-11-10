# Replace dd with FIO as Lightweight Fill Method

**Label:** `wayfinder:task`  
**Status:** Open  
**Blocked by:** [Incremental Data Script Integration](022-incremental-data-script-integration.md)

## Question

Replace the `dd` data generation method in `vm_operations.py` with FIO. This is a Phase 2 task that comes **after** dd parity is established with the shell scripts (tickets 019-023 establish script integration, ticket 007 aligns the native pipeline with cloud-init dd-based fill).

The migration order is:
1. Phase 1: scripts use dd via cloud-init (already working)
2. Phase 2 early: native Python pipeline uses dd via cloud-init (ticket 007)
3. Phase 2 later: **this ticket** — replace dd with FIO for both native and optionally script paths

The current `_build_data_gen_command()` has a `dd` branch that generates `dd if=/dev/urandom of=<mount>/fill.dat bs=1M`. Replace with FIO-based fill that:

1. Generates high-entropy data (defeats storage dedup/compression)
2. Supports configurable directory count, files per directory, file size (matching cnv-scenarios pattern)
3. Reports progress/completion via sentinel file
4. Works on both Linux (likely pre-installed in golden image) and Windows (MSI install or pre-installed)

The profile's `data_generation.config` section needs FIO-specific parameters replacing dd parameters. FIO config lives in scenario args (per ticket 001 decision), not the profile.

Additionally, this ticket should address native incremental data generation (plan 2.7):
- Define increment levels in scenario args (matching `VMsAddDataVerify.sh` inc1/inc2/inc3 pattern)
- Use `virtctl ssh` to run FIO commands for each increment
- Implement file-count verification via `find | wc -l` per disk

Resolve: FIO method implemented, profile schema supports FIO config, incremental FIO support, unit tests updated, existing dd tests preserved for backward compatibility.

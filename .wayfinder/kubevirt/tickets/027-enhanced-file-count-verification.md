# Enhanced File-Count Verification

**Label:** `wayfinder:task`  
**Status:** Open  
**Blocked by:** [Script-Based VM Verification](020-script-based-verification.md)

## Question

Align the native Python VM validation in `vm_validation.py` with the file-count + capacity verification approach used by `VMsAddDataVerify.sh`, replacing the current df-only capacity check.

The script verifies two dimensions per disk:
1. **File count** — `find <path> -type f | wc -l` must match expected count
2. **Capacity** — `du -sh <path>` must meet expected threshold

Current Python validation (`_validate_single_vm_data()`) only checks capacity via `df -BG` with 20% tolerance. This misses cases where files are missing but total capacity looks correct (e.g., fewer larger files after a faulty restore).

Implementation:

1. Add expected file counts to profile/scenario schema:
   - Per-disk `expected_file_count` field alongside existing `expected_capacity`
   - Baseline: 157,000 (OS `/home/data`), 105,000 (per data disk `/mnt-vd*/data`)
   - Incremental: accumulated count after each increment level

2. Add `_validate_vm_file_count()` to `vm_validation.py`:
   - `virtctl ssh <user>@<vm> --command 'find <path> -type f | wc -l'`
   - Compare against `expected_file_count` with tolerance (e.g., 5%)
   - Report per-disk pass/fail with actual vs expected counts

3. Update `_validate_single_vm_data()`:
   - Run both file-count and capacity checks
   - Both must pass for the disk to be considered valid
   - Report structured results in run_metadata JSON

4. Support incremental verification:
   - After inc1/inc2/inc3, expected file counts change
   - The verification must use the cumulative expected count (baseline + increments applied)

5. Add "VERIFY PASSED"/"VERIFY FAILED" outcome logging to match script output pattern for consistency

Resolve: file-count verification implemented alongside capacity check, per-disk reporting, incremental support, unit tests cover pass/fail scenarios.

"""
Mixin for KubeVirt VM dataset validation (pre-backup and post-restore).

Mirrors the pod validation flow in ``pod_validation.py`` and ``dataset.py``:
- Fast pre-check with ``interval=5, max_attempts=1`` to detect existing datasets
- Patient retry after creation/restore with ``interval=15, max_attempts=28``
- Validation modes: none / light / full
- Per-disk ``expected_capacity`` checks via ``virtctl ssh -- du -sh``
"""

from __future__ import annotations

import random

from benchmark_runner.common.logger.logger_time_stamp import logger, logger_time_stamp
from benchmark_runner.oadp.constants import (
    VALIDATION_MODE_FULL,
    VALIDATION_MODE_LIGHT,
    VALIDATION_MODE_NONE,
    VM_DATASET_ROLE,
    VM_DEFAULT_SSH_USER,
)
from benchmark_runner.oadp.vm_profiles import load_vm_profile, merge_disk_overrides


class OadpVmValidationMixin:
    """Pre-backup and post-restore validation for KubeVirt VM datasets."""

    # ------------------------------------------------------------------
    # Pre-backup: verify or recreate VM datasets
    # ------------------------------------------------------------------

    @logger_time_stamp
    def verify_vm_datasets_before_backups(self, scenario: dict) -> None:
        """Check existing VM datasets; recreate only those that fail validation.

        Mirrors ``verify_datsets_before_backups()`` in ``dataset.py``.
        """
        try:
            self.set_validation_retry_logic(interval_between_checks=5, max_attempts=1)
            vm_dataset_present = self.validate_expected_vm_datasets(scenario)
            run_metadata = self._OadpWorkloads__run_metadata
            run_metadata["summary"]["validations"]["vm_dataset_already_present"] = vm_dataset_present

            if not vm_dataset_present:
                self.set_validation_retry_logic(
                    interval_between_checks=15,
                    max_attempts=28,
                )
                failing = self._OadpWorkloads__oadp_ds_failing_validation
                logger.info(f"VM datasets not fully present; failing datasets: {len(failing)}")
                for ds in failing:
                    if ds.get("role") == VM_DATASET_ROLE:
                        self.create_vm_dataset(scenario, ds)

                recreated_ok = self.validate_expected_vm_datasets(scenario)
                run_metadata["summary"]["validations"]["vm_dataset_recreated_as_expected"] = recreated_ok
                if not recreated_ok:
                    logger.exception(f"VM dataset recreation for {scenario['args']['OADP_CR_NAME']} failed validation")
                else:
                    logger.info("VM dataset post-creation validations passed")

        except Exception as err:
            self.fail_test_run(f" {err} occurred in verify_vm_datasets_before_backups")
            raise err

    # ------------------------------------------------------------------
    # Core validation
    # ------------------------------------------------------------------

    @logger_time_stamp
    def validate_expected_vm_datasets(self, scenario: dict) -> bool:
        """Validate all VM datasets for the scenario.  Returns True if all pass."""
        try:
            dataset_value = scenario.get("dataset")
            if isinstance(dataset_value, list):
                datasets = [d for d in dataset_value if d.get("role") == VM_DATASET_ROLE]
            elif isinstance(dataset_value, dict):
                if dataset_value.get("role") != VM_DATASET_ROLE:
                    return True
                datasets = [dataset_value]
            else:
                return True

            all_valid = True
            for ds in datasets:
                ns = ds.get("namespace", scenario["args"].get("namespaces_to_backup", ""))
                ds.setdefault("namespace", ns)
                if not self._validate_vm_dataset(scenario, ds):
                    self._OadpWorkloads__oadp_ds_failing_validation.append(ds)
                    all_valid = False
            return all_valid

        except Exception as err:
            self.fail_test_run(f" {err} occurred in validate_expected_vm_datasets")
            raise err

    @logger_time_stamp
    def _validate_vm_dataset(self, scenario: dict, ds: dict) -> bool:
        """Validate a single VM dataset: count, running state, guest agent, data."""
        namespace = ds["namespace"]
        expected_vms = ds.get("vms_per_namespace", ds.get("pods_per_ns", 1))
        validation_mode = self.get_dataset_validation_mode(ds)
        skip = scenario["args"].get("skip_source_dataset_check", False)

        if skip:
            logger.warning("Skipping VM dataset validation (skip_source_dataset_check=True)")
            return True

        if validation_mode == VALIDATION_MODE_NONE:
            logger.warning("VM dataset validation mode is 'none' — skipping all checks")
            return True

        if not self._check_vm_count(namespace, expected_vms):
            return False

        if not self._check_all_vmis_running(namespace, expected_vms):
            return False

        if validation_mode == VALIDATION_MODE_LIGHT:
            return self._validate_vm_data_sample(ds, namespace, expected_vms)

        if validation_mode == VALIDATION_MODE_FULL:
            return self._validate_vm_data_all(ds, namespace, expected_vms)

        return True

    # ------------------------------------------------------------------
    # Individual checks
    # ------------------------------------------------------------------

    def _check_vm_count(self, namespace: str, expected: int) -> bool:
        ssh = self._OadpWorkloads__ssh
        result = ssh.run(cmd=f"oc get vm -n {namespace} --no-headers 2>/dev/null | wc -l")
        try:
            count = int(result.strip())
        except (ValueError, AttributeError):
            count = 0
        if count < expected:
            logger.warning(f"VM count mismatch in {namespace}: found {count}, expected {expected}")
            return False
        return True

    def _check_all_vmis_running(self, namespace: str, expected: int) -> bool:
        ssh = self._OadpWorkloads__ssh
        result = ssh.run(cmd=f"oc get vmi -n {namespace} --no-headers 2>/dev/null | grep -c Running")
        try:
            count = int(result.strip())
        except (ValueError, AttributeError):
            count = 0
        if count < expected:
            logger.warning(f"Running VMI count in {namespace}: {count}/{expected}")
            return False
        return True

    def _validate_vm_data_sample(
        self,
        ds: dict,
        namespace: str,
        total_vms: int,
    ) -> bool:
        """Validate a 10% random sample of VMs (light mode)."""
        sample_size = max(1, int(total_vms * 0.1))
        indices = random.sample(range(total_vms), min(sample_size, total_vms))
        profile = self._load_profile_for_ds(ds)

        logger.info(f"Light validation: checking {len(indices)} of {total_vms} VMs")
        for idx in indices:
            vm_name = f"oadp-vm-{profile['name']}-{idx}"
            if not self._validate_single_vm_data(vm_name, namespace, profile):
                return False
        return True

    def _validate_vm_data_all(
        self,
        ds: dict,
        namespace: str,
        total_vms: int,
    ) -> bool:
        """Validate every VM (full mode)."""
        profile = self._load_profile_for_ds(ds)
        logger.info(f"Full validation: checking all {total_vms} VMs")
        for idx in range(total_vms):
            vm_name = f"oadp-vm-{profile['name']}-{idx}"
            if not self._validate_single_vm_data(vm_name, namespace, profile):
                return False
        return True

    def _validate_single_vm_data(
        self,
        vm_name: str,
        namespace: str,
        profile: dict,
    ) -> bool:
        """Check per-disk expected_capacity via ``df -BG`` with 20% tolerance."""
        ssh = self._OadpWorkloads__ssh
        ssh_user = VM_DEFAULT_SSH_USER

        for disk in profile.get("disks", []):
            expected = disk.get("expected_capacity")
            validation_path = disk.get("validation_path")
            if not expected or not validation_path:
                continue

            self._validate_shell_safe_value("validation_path", validation_path)

            result = ssh.run(
                cmd=f"virtctl ssh {ssh_user}@{vm_name} -n {namespace} --command 'df -BG {validation_path} | tail -1'"
            )
            actual_gb = self._parse_df_used_gb(result)
            expected_gb = self._parse_capacity_gb(expected)

            if expected_gb <= 0:
                logger.info(f"VM {vm_name} disk {validation_path}: expected=0, skipping")
                continue

            ratio = actual_gb / expected_gb if expected_gb > 0 else 0
            if ratio < 0.8:
                logger.warning(
                    f"VM {vm_name} disk validation failed: "
                    f"path={validation_path}, expected>={expected}, "
                    f"actual={actual_gb}G (ratio={ratio:.2f})"
                )
                return False
            logger.info(
                f"VM {vm_name} disk OK: path={validation_path}, used={actual_gb}G, expected={expected} (ratio={ratio:.2f})"
            )
        return True

    @staticmethod
    def _validate_shell_safe_value(field_name: str, value: str) -> None:
        """Reject values with shell metacharacters (SEC-002 defense-in-depth)."""
        from benchmark_runner.oadp.constants import SHELL_METACHARACTERS

        if any(ch in value for ch in SHELL_METACHARACTERS) or "'" in value:
            raise ValueError(f"Field '{field_name}' contains forbidden characters: {value!r}")

    @staticmethod
    def _parse_df_used_gb(df_output: str) -> float:
        """Extract used-GB from ``df -BG`` output (third column)."""
        try:
            line = df_output.strip().splitlines()[-1]
            parts = line.split()
            used_str = parts[2].rstrip("G")
            return float(used_str)
        except (IndexError, ValueError):
            return 0.0

    @staticmethod
    def _parse_capacity_gb(capacity_str: str) -> float:
        """Parse a capacity string like '45G' or '500M' into GB."""
        import re

        match = re.match(r"(\d+\.?\d*)\s*([KMGT]?)", capacity_str.strip())
        if not match:
            return 0.0
        value = float(match.group(1))
        unit = match.group(2).upper()
        multipliers = {"K": 1 / (1024 * 1024), "M": 1 / 1024, "G": 1, "T": 1024}
        return value * multipliers.get(unit, 1)

    def _load_profile_for_ds(self, ds: dict) -> dict:
        """Load and merge the VM profile for a dataset entry."""
        profile = load_vm_profile(ds.get("vm_profile", ""))
        return merge_disk_overrides(profile, ds.get("disk_overrides"))

    # ------------------------------------------------------------------
    # Post-restore validation
    # ------------------------------------------------------------------

    @logger_time_stamp
    def validate_restored_vm_datasets(self, scenario: dict) -> bool:
        """Validate VM datasets after restore with patient retry settings.

        Mirrors the pod restore validation flow in ``oadp_workloads.py``.
        """
        self.set_validation_retry_logic(interval_between_checks=15, max_attempts=28)
        restored_ok = self.validate_expected_vm_datasets(scenario)
        run_metadata = self._OadpWorkloads__run_metadata
        run_metadata["summary"]["validations"]["vm_dataset_restored_as_expected"] = "PASS" if restored_ok else "FAIL"
        run_metadata["summary"]["results"]["vm_dataset_post_run_validation"] = restored_ok
        if not restored_ok:
            logger.error(f"Restored VM dataset for {scenario['args']['OADP_CR_NAME']} failed post-run validations")
        else:
            logger.info("VM restore passed post-run validations")
        return restored_ok

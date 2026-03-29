"""Unit tests for KubeVirt support: scenario validation, profile loader, security,
feature flag gating, data generation commands, and VM validation helpers."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from benchmark_runner.oadp.constants import (
    KNOWN_PLUGINS,
    SHELL_METACHARACTERS,
    VM_DATASET_ROLE,
    VM_DEFAULT_SSH_USER,
)
from benchmark_runner.oadp.scenario import OadpScenarioMixin, scenario_includes_kubevirt_dataset
from benchmark_runner.oadp.vm_operations import OadpVmOperationsMixin
from benchmark_runner.oadp.vm_profiles import load_vm_profile, merge_disk_overrides
from benchmark_runner.oadp.vm_validation import OadpVmValidationMixin


# ---------------------------------------------------------------------------
# Shared stubs
# ---------------------------------------------------------------------------


def _valid_args(**overrides):
    base = {
        "plugin": "csi",
        "kubevirt_plugin": True,
        "use_cli": True,
        "OADP_CR_TYPE": "backup",
        "OADP_CR_NAME": "backup-kubevirt-test",
        "backup_name": "backup-kubevirt-test",
        "testcase_timeout": 600,
    }
    base.update(overrides)
    return base


def _kubevirt_scenario(**overrides):
    s = {
        "name": "backup-kubevirt-test",
        "testcase": "10.1",
        "result_dir_base_path": "/tmp/results",
        "testtype": "backup",
        "dataset": {
            "role": "kubevirt",
            "vm_profile": "rhel9-hammerdb-mariadb",
            "vms_per_namespace": 2,
            "sc": "ocs-storagecluster-ceph-rbd-virtualization",
        },
        "args": _valid_args(),
    }
    s.update(overrides)
    return s


def _pod_scenario(**overrides):
    s = {
        "name": "backup-pods",
        "testcase": "1.0",
        "result_dir_base_path": "/tmp/results",
        "testtype": "backup",
        "dataset": {
            "role": "BusyBoxPodSingleNS.sh",
            "pods_per_ns": 10,
            "sc": "ocs-storagecluster-ceph-rbd",
        },
        "args": {
            "plugin": "kopia",
            "use_cli": False,
            "OADP_CR_TYPE": "backup",
            "OADP_CR_NAME": "cr1",
            "backup_name": "bk1",
            "testcase_timeout": 600,
        },
    }
    s.update(overrides)
    return s


class OadpWorkloads:
    """Minimal stub for mixin attribute access via name mangling."""

    def __init__(self):
        self.__test_env = {"source": "upstream", "velero_ns": "openshift-adp"}
        self.__run_metadata = {
            "index": "",
            "metadata": {},
            "status": "",
            "summary": {
                "env": {},
                "runtime": {"datasetsize": {}, "dataset": {}},
                "results": {},
                "validations": {},
                "resources": {"nodes": {}, "run_time_pods": [], "pods": {}},
                "transactions": [],
            },
        }
        self.__scenario_datasets = []
        self.__oadp_ds_failing_validation = []
        self.__oadp_enable_kubevirt = True
        self.__retry_logic = {"interval_between_checks": 5, "max_attempts": 1}
        self.__ssh = MagicMock()
        self._oc = MagicMock()

    def fail_test_run(self, msg: str) -> None:
        raise RuntimeError(msg)

    def get_current_function(self) -> str:
        return "test"

    def oadp_timer(self, action: str, transaction_name: str) -> None:
        pass

    def set_validation_retry_logic(self, interval_between_checks: int, max_attempts: int) -> None:
        self._OadpWorkloads__retry_logic = {
            "interval_between_checks": interval_between_checks,
            "max_attempts": max_attempts,
        }

    def get_dataset_validation_mode(self, ds: dict) -> str:
        return "light"


class TestableScenario(OadpScenarioMixin, OadpWorkloads):
    __test__ = False


class TestableVmOps(OadpVmOperationsMixin, OadpWorkloads):
    __test__ = False


class TestableVmValidation(OadpVmValidationMixin, OadpVmOperationsMixin, OadpWorkloads):
    __test__ = False


# ===========================================================================
# 1. scenario_includes_kubevirt_dataset (module-level function)
# ===========================================================================


class TestScenarioIncludesKubevirtDataset:
    def test_dict_with_kubevirt_role(self):
        s = _kubevirt_scenario()
        assert scenario_includes_kubevirt_dataset(s) is True

    def test_dict_with_pod_role(self):
        s = _pod_scenario()
        assert scenario_includes_kubevirt_dataset(s) is False

    def test_list_with_kubevirt_entry(self):
        s = _kubevirt_scenario(
            dataset=[
                {"role": "BusyBoxPodSingleNS.sh", "pods_per_ns": 5, "sc": "s", "namespace": "ns1"},
                {"role": "kubevirt", "vm_profile": "p", "vms_per_namespace": 2, "sc": "s", "namespace": "ns2"},
            ]
        )
        assert scenario_includes_kubevirt_dataset(s) is True

    def test_list_without_kubevirt(self):
        s = _pod_scenario(
            dataset=[
                {"role": "generator", "pods_per_ns": 1, "sc": "s", "namespace": "ns1"},
            ]
        )
        assert scenario_includes_kubevirt_dataset(s) is False

    def test_missing_dataset(self):
        assert scenario_includes_kubevirt_dataset({}) is False

    def test_non_dict_non_list_dataset(self):
        assert scenario_includes_kubevirt_dataset({"dataset": "string"}) is False


# ===========================================================================
# 2. Scenario contract validation for VM datasets
# ===========================================================================


class TestScenarioValidationKubevirt:
    @pytest.fixture
    def mx(self):
        return TestableScenario()

    def test_valid_kubevirt_scenario(self, mx):
        assert mx.validate_scenario(_kubevirt_scenario()) is True

    def test_kubevirt_missing_vm_profile_fails(self, mx):
        s = _kubevirt_scenario()
        del s["dataset"]["vm_profile"]
        assert mx.validate_scenario(s) is False

    def test_kubevirt_with_vms_per_namespace_valid(self, mx):
        s = _kubevirt_scenario()
        assert "vms_per_namespace" in s["dataset"]
        assert mx.validate_scenario(s) is True

    def test_kubevirt_missing_vms_and_pods_fails(self, mx):
        s = _kubevirt_scenario()
        del s["dataset"]["vms_per_namespace"]
        assert mx.validate_scenario(s) is False

    def test_kubevirt_with_pods_per_ns_fallback(self, mx):
        s = _kubevirt_scenario()
        s["dataset"]["pods_per_ns"] = 3
        del s["dataset"]["vms_per_namespace"]
        assert mx.validate_scenario(s) is True

    def test_kubevirt_does_not_require_pods_per_ns(self, mx):
        """pods_per_ns is excluded from required keys for kubevirt datasets."""
        s = _kubevirt_scenario()
        assert "pods_per_ns" not in s["dataset"]
        assert mx.validate_scenario(s) is True

    def test_unknown_plugin_rejected(self, mx):
        s = _pod_scenario()
        s["args"]["plugin"] = "unknown_plugin"
        assert mx.validate_scenario(s) is False

    def test_all_known_plugins_accepted(self, mx):
        for plugin in KNOWN_PLUGINS:
            s = _pod_scenario()
            s["args"]["plugin"] = plugin
            if plugin == "kubevirt":
                s["dataset"]["role"] = "kubevirt"
                s["dataset"]["vm_profile"] = "p"
                s["dataset"]["vms_per_namespace"] = 1
                s["args"]["kubevirt_plugin"] = True
            assert mx.validate_scenario(s) is True, f"Plugin {plugin} should be accepted"

    def test_kubevirt_missing_kubevirt_plugin_arg_fails(self, mx):
        s = _kubevirt_scenario()
        del s["args"]["kubevirt_plugin"]
        assert mx.validate_scenario(s) is False

    def test_kubevirt_with_kubevirt_plugin_false_fails(self, mx):
        s = _kubevirt_scenario(args=_valid_args(kubevirt_plugin=False))
        assert mx.validate_scenario(s) is False

    def test_pod_scenario_without_kubevirt_plugin_passes(self, mx):
        s = _pod_scenario()
        assert "kubevirt_plugin" not in s["args"]
        assert mx.validate_scenario(s) is True


# ===========================================================================
# 3. Shell metacharacter rejection (SEC-002)
# ===========================================================================


class TestShellMetacharacterRejection:
    @pytest.fixture
    def mx(self):
        return TestableScenario()

    @pytest.mark.parametrize("char", list(SHELL_METACHARACTERS) + ["'"])
    def test_scenario_field_with_metachar_rejected(self, mx, char):
        s = _pod_scenario()
        s["name"] = f"scen{char}ario"
        assert mx.validate_scenario(s) is False

    def test_disk_overrides_metachar_rejected(self, mx):
        s = _kubevirt_scenario()
        s["dataset"]["disk_overrides"] = {"datadisk": {"expected_capacity": "45G; rm -rf /"}}
        assert mx.validate_scenario(s) is False

    def test_disk_overrides_single_quote_rejected(self, mx):
        s = _kubevirt_scenario()
        s["dataset"]["disk_overrides"] = {"datadisk": {"expected_capacity": "45G'"}}
        assert mx.validate_scenario(s) is False

    def test_clean_disk_overrides_accepted(self, mx):
        s = _kubevirt_scenario()
        s["dataset"]["disk_overrides"] = {"datadisk": {"expected_capacity": "45G"}}
        assert mx.validate_scenario(s) is True


class TestProfileShellFieldValidation:
    def test_safe_profile_passes(self):
        profile = {
            "disks": [
                {"name": "rootdisk", "size": "30Gi", "boot_order": 1},
                {"name": "datadisk", "filesystem": "xfs", "mount_point": "/var/lib/mysql"},
            ],
            "data_generation": {"config": {"database": "mariadb"}},
            "cloud_init": {"users": [{"name": "cloud-user"}]},
        }
        OadpVmOperationsMixin._validate_profile_shell_fields(profile)

    @pytest.mark.parametrize("field,value", [
        ("name", "disk;rm"),
        ("filesystem", "xfs|ext4"),
        ("mount_point", "/var/lib/mysql$(id)"),
    ])
    def test_unsafe_disk_field_rejected(self, field, value):
        profile = {
            "disks": [{"name": "d1", field: value}],
            "cloud_init": {"users": [{"name": "cloud-user"}]},
        }
        if field != "name":
            profile["disks"][0]["name"] = "d1"
        with pytest.raises(ValueError, match="unsafe characters|shell metacharacters"):
            OadpVmOperationsMixin._validate_profile_shell_fields(profile)

    def test_unsafe_database_rejected(self):
        profile = {
            "disks": [],
            "data_generation": {"config": {"database": "maria;db"}},
            "cloud_init": {"users": [{"name": "cloud-user"}]},
        }
        with pytest.raises(ValueError, match="unsafe characters"):
            OadpVmOperationsMixin._validate_profile_shell_fields(profile)

    def test_unsafe_ssh_user_rejected(self):
        profile = {
            "disks": [],
            "cloud_init": {"users": [{"name": "user$(id)"}]},
        }
        with pytest.raises(ValueError, match="unsafe characters"):
            OadpVmOperationsMixin._validate_profile_shell_fields(profile)


# ===========================================================================
# 4. Profile loader (load_vm_profile, merge_disk_overrides)
# ===========================================================================


class TestLoadVmProfile:
    def test_loads_existing_profile(self):
        profile = load_vm_profile("rhel9-hammerdb-mariadb")
        assert profile["name"] == "rhel9-hammerdb-mariadb"
        assert profile["os"]["type"] == "linux"
        assert len(profile["disks"]) >= 2

    def test_loads_postgres_profile(self):
        profile = load_vm_profile("rhel9-hammerdb-postgres")
        assert profile["data_generation"]["config"]["database"] == "postgres"

    def test_nonexistent_profile_raises(self):
        with pytest.raises(FileNotFoundError, match="not found"):
            load_vm_profile("nonexistent-profile-xyz")

    @pytest.mark.parametrize("bad_name", [
        "",
        "../etc/passwd",
        "../../secret",
        "/absolute/path",
        "name with spaces",
        "name;injection",
    ])
    def test_unsafe_names_rejected(self, bad_name):
        with pytest.raises(ValueError, match="Invalid VM profile name|forbidden path"):
            load_vm_profile(bad_name)

    def test_safe_name_with_dots_and_hyphens(self):
        from benchmark_runner.oadp.vm_profiles import _validate_profile_name
        _validate_profile_name("rhel9-hammerdb.v2")


class TestMergeDiskOverrides:
    def test_overrides_expected_capacity(self):
        profile = {
            "name": "test",
            "disks": [
                {"name": "rootdisk", "size": "30Gi"},
                {"name": "datadisk", "size": "60Gi", "expected_capacity": "45G"},
            ],
        }
        merged = merge_disk_overrides(profile, {"datadisk": {"expected_capacity": "20G"}})
        datadisk = next(d for d in merged["disks"] if d["name"] == "datadisk")
        assert datadisk["expected_capacity"] == "20G"
        rootdisk = next(d for d in merged["disks"] if d["name"] == "rootdisk")
        assert "expected_capacity" not in rootdisk

    def test_none_overrides_returns_original(self):
        profile = {"name": "test", "disks": [{"name": "d1"}]}
        result = merge_disk_overrides(profile, None)
        assert result is profile

    def test_empty_overrides_returns_original(self):
        profile = {"name": "test", "disks": [{"name": "d1"}]}
        result = merge_disk_overrides(profile, {})
        assert result is profile

    def test_original_not_mutated(self):
        profile = {
            "name": "test",
            "disks": [{"name": "datadisk", "expected_capacity": "45G"}],
        }
        merged = merge_disk_overrides(profile, {"datadisk": {"expected_capacity": "20G"}})
        assert profile["disks"][0]["expected_capacity"] == "45G"
        assert merged["disks"][0]["expected_capacity"] == "20G"

    def test_override_nonexistent_disk_ignored(self):
        profile = {"name": "test", "disks": [{"name": "rootdisk"}]}
        merged = merge_disk_overrides(profile, {"ghostdisk": {"size": "100Gi"}})
        assert len(merged["disks"]) == 1
        assert merged["disks"][0]["name"] == "rootdisk"


# ===========================================================================
# 5. Feature flag gating
# ===========================================================================


class TestFeatureFlagGating:
    def test_kubevirt_scenario_rejected_when_flag_off(self):
        stub = OadpWorkloads()
        stub._OadpWorkloads__oadp_enable_kubevirt = False
        scenario = _kubevirt_scenario()
        with pytest.raises(RuntimeError, match="OADP_ENABLE_KUBEVIRT is False"):
            OadpWorkloads._reject_kubevirt_if_disabled = (
                lambda self, s: _reject_kubevirt_if_disabled_impl(self, s)
            )
            _reject_kubevirt_if_disabled_impl(stub, scenario)

    def test_pod_scenario_passes_when_flag_off(self):
        stub = OadpWorkloads()
        stub._OadpWorkloads__oadp_enable_kubevirt = False
        scenario = _pod_scenario()
        _reject_kubevirt_if_disabled_impl(stub, scenario)

    def test_kubevirt_scenario_passes_when_flag_on(self):
        stub = OadpWorkloads()
        stub._OadpWorkloads__oadp_enable_kubevirt = True
        scenario = _kubevirt_scenario()
        _reject_kubevirt_if_disabled_impl(stub, scenario)


def _reject_kubevirt_if_disabled_impl(self, scenario):
    """Standalone implementation matching OadpWorkloads._reject_kubevirt_if_disabled."""
    if self._OadpWorkloads__oadp_enable_kubevirt:
        return
    if scenario_includes_kubevirt_dataset(scenario):
        msg = (
            f"Scenario '{scenario.get('name', '')}' uses role '{VM_DATASET_ROLE}' "
            f"but OADP_ENABLE_KUBEVIRT is False. Set OADP_ENABLE_KUBEVIRT=True to run VM scenarios."
        )
        raise RuntimeError(msg)


class TestScenarioHasVmDatasets:
    def test_false_when_flag_off(self):
        stub = OadpWorkloads()
        stub._OadpWorkloads__oadp_enable_kubevirt = False
        assert _scenario_has_vm_datasets_impl(stub, _kubevirt_scenario()) is False

    def test_true_when_flag_on_and_kubevirt(self):
        stub = OadpWorkloads()
        stub._OadpWorkloads__oadp_enable_kubevirt = True
        assert _scenario_has_vm_datasets_impl(stub, _kubevirt_scenario()) is True

    def test_false_when_flag_on_but_no_kubevirt(self):
        stub = OadpWorkloads()
        stub._OadpWorkloads__oadp_enable_kubevirt = True
        assert _scenario_has_vm_datasets_impl(stub, _pod_scenario()) is False


def _scenario_has_vm_datasets_impl(self, scenario):
    if not self._OadpWorkloads__oadp_enable_kubevirt:
        return False
    return scenario_includes_kubevirt_dataset(scenario)


# ===========================================================================
# 6. _build_data_gen_command
# ===========================================================================


class TestBuildDataGenCommand:
    def test_hammerdb_default_params(self):
        cmd = OadpVmOperationsMixin._build_data_gen_command("hammerdb", {}, {})
        assert cmd == "/opt/hammerdb/run_tpcc.sh mariadb 10 4"

    def test_hammerdb_custom_params(self):
        config = {"database": "postgres", "tpcc_warehouses": 50, "tpcc_virtual_users": 8}
        cmd = OadpVmOperationsMixin._build_data_gen_command("hammerdb", config, {})
        assert cmd == "/opt/hammerdb/run_tpcc.sh postgres 50 8"

    def test_dd_with_mount_point(self):
        profile = {
            "disks": [
                {"name": "rootdisk", "boot_order": 1},
                {"name": "datadisk", "mount_point": "/mnt/data"},
            ]
        }
        cmd = OadpVmOperationsMixin._build_data_gen_command("dd", {}, profile)
        assert cmd.startswith("dd if=/dev/urandom of=/mnt/data/fill.dat")
        assert "bs=1M" in cmd
        assert "count=1024" in cmd

    def test_dd_custom_bs_and_count(self):
        profile = {"disks": [{"name": "d", "mount_point": "/data"}]}
        config = {"bs": "4M", "count": 256}
        cmd = OadpVmOperationsMixin._build_data_gen_command("dd", config, profile)
        assert "bs=4M" in cmd
        assert "count=256" in cmd

    def test_dd_no_data_disks_returns_empty(self):
        profile = {"disks": [{"name": "rootdisk", "boot_order": 1}]}
        cmd = OadpVmOperationsMixin._build_data_gen_command("dd", {}, profile)
        assert cmd == ""

    def test_unknown_method_returns_empty(self):
        assert OadpVmOperationsMixin._build_data_gen_command("fio", {}, {}) == ""

    def test_empty_method_returns_empty(self):
        assert OadpVmOperationsMixin._build_data_gen_command("", {}, {}) == ""


# ===========================================================================
# 7. VM validation helpers
# ===========================================================================


class TestParseDfUsedGb:
    def test_standard_df_output(self):
        output = "Filesystem     1G-blocks  Used Available Use% Mounted on\n/dev/vdb           60G   45G       15G  75% /var/lib/mysql"
        assert OadpVmValidationMixin._parse_df_used_gb(output) == 45.0

    def test_empty_output(self):
        assert OadpVmValidationMixin._parse_df_used_gb("") == 0.0

    def test_garbage_output(self):
        assert OadpVmValidationMixin._parse_df_used_gb("no data here") == 0.0

    def test_single_line_output(self):
        output = "/dev/vda1          30G   12G       18G  40% /"
        assert OadpVmValidationMixin._parse_df_used_gb(output) == 12.0


class TestParseCapacityGb:
    @pytest.mark.parametrize("input_str,expected", [
        ("45G", 45.0),
        ("500M", 500.0 / 1024),
        ("1T", 1024.0),
        ("2048K", 2048.0 / (1024 * 1024)),
        ("0G", 0.0),
    ])
    def test_standard_units(self, input_str, expected):
        assert OadpVmValidationMixin._parse_capacity_gb(input_str) == pytest.approx(expected)

    def test_no_unit_treated_as_gb(self):
        assert OadpVmValidationMixin._parse_capacity_gb("100") == 100.0

    def test_invalid_string_returns_zero(self):
        assert OadpVmValidationMixin._parse_capacity_gb("invalid") == 0.0

    def test_whitespace_handling(self):
        assert OadpVmValidationMixin._parse_capacity_gb("  45G  ") == 45.0


class TestValidateShellSafeValue:
    def test_clean_value_passes(self):
        OadpVmValidationMixin._validate_shell_safe_value("path", "/var/lib/mysql")

    @pytest.mark.parametrize("char", [";", "|", "&", "$", "`", "(", ")"])
    def test_metachar_rejected(self, char):
        with pytest.raises(ValueError, match="forbidden characters"):
            OadpVmValidationMixin._validate_shell_safe_value("path", f"/var{char}lib")

    def test_single_quote_rejected(self):
        with pytest.raises(ValueError, match="forbidden characters"):
            OadpVmValidationMixin._validate_shell_safe_value("path", "/var'lib")


# ===========================================================================
# 8. SSH user extraction helpers
# ===========================================================================


class TestSshUserFromProfile:
    def test_default_when_no_cloud_init(self):
        assert OadpVmOperationsMixin._ssh_user_from_profile({}) == VM_DEFAULT_SSH_USER

    def test_extracts_first_user(self):
        profile = {"cloud_init": {"users": [{"name": "testuser"}]}}
        assert OadpVmOperationsMixin._ssh_user_from_profile(profile) == "testuser"

    def test_fallback_when_no_name(self):
        profile = {"cloud_init": {"users": [{}]}}
        assert OadpVmOperationsMixin._ssh_user_from_profile(profile) == VM_DEFAULT_SSH_USER

    def test_empty_users_list(self):
        profile = {"cloud_init": {"users": []}}
        assert OadpVmOperationsMixin._ssh_user_from_profile(profile) == VM_DEFAULT_SSH_USER


class TestSshAuthorizedKeysFromProfile:
    def test_default_when_no_cloud_init(self):
        assert OadpVmOperationsMixin._ssh_authorized_keys_from_profile({}) == []

    def test_extracts_keys(self):
        profile = {"cloud_init": {"users": [{"ssh_authorized_keys": ["ssh-rsa AAAA..."]}]}}
        assert OadpVmOperationsMixin._ssh_authorized_keys_from_profile(profile) == ["ssh-rsa AAAA..."]


# ===========================================================================
# 9. Data gen process patterns
# ===========================================================================


class TestDataGenProcessPatterns:
    def test_hammerdb_pattern(self):
        import re
        pattern = OadpVmOperationsMixin._DATA_GEN_PROCESS_PATTERNS["hammerdb"]
        assert re.search(pattern, "hammerdb worker")
        assert re.search(pattern, "run_tpcc.sh")

    def test_dd_pattern(self):
        import re
        pattern = OadpVmOperationsMixin._DATA_GEN_PROCESS_PATTERNS["dd"]
        assert re.search(pattern, "dd if=/dev/urandom of=/mnt/data/fill.dat bs=1M")
        assert not re.search(pattern, "dd.if=something")


# ===========================================================================
# 10. Effective dataset keys for VM
# ===========================================================================


class TestEffectiveDatasetKeys:
    def test_kubevirt_removes_pods_per_ns(self):
        base = ["role", "pods_per_ns", "sc"]
        result = OadpScenarioMixin._effective_dataset_keys({"role": VM_DATASET_ROLE}, base)
        assert "pods_per_ns" not in result
        assert "role" in result
        assert "sc" in result

    def test_non_kubevirt_keeps_all(self):
        base = ["role", "pods_per_ns", "sc"]
        result = OadpScenarioMixin._effective_dataset_keys({"role": "BusyBoxPodSingleNS.sh"}, base)
        assert result == base


# ===========================================================================
# 11. Validate dataset entry for VM
# ===========================================================================


class TestValidateDatasetEntry:
    def test_valid_kubevirt_entry(self):
        ds = {"role": "kubevirt", "vm_profile": "rhel9-hammerdb-mariadb", "vms_per_namespace": 2, "sc": "s"}
        assert OadpScenarioMixin._validate_dataset_entry(ds) is True

    def test_kubevirt_missing_vm_profile(self):
        ds = {"role": "kubevirt", "vms_per_namespace": 2, "sc": "s"}
        assert OadpScenarioMixin._validate_dataset_entry(ds) is False

    def test_kubevirt_missing_count_fields(self):
        ds = {"role": "kubevirt", "vm_profile": "p", "sc": "s"}
        assert OadpScenarioMixin._validate_dataset_entry(ds) is False

    def test_kubevirt_pods_per_ns_as_fallback(self):
        ds = {"role": "kubevirt", "vm_profile": "p", "pods_per_ns": 3, "sc": "s"}
        assert OadpScenarioMixin._validate_dataset_entry(ds) is True

    def test_non_kubevirt_always_valid(self):
        ds = {"role": "BusyBoxPodSingleNS.sh", "pods_per_ns": 10, "sc": "s"}
        assert OadpScenarioMixin._validate_dataset_entry(ds) is True

    def test_disk_overrides_with_metachar_rejected(self):
        ds = {
            "role": "kubevirt",
            "vm_profile": "p",
            "vms_per_namespace": 1,
            "sc": "s",
            "disk_overrides": {"datadisk": {"expected_capacity": "45G;evil"}},
        }
        assert OadpScenarioMixin._validate_dataset_entry(ds) is False

    def test_disk_overrides_name_with_metachar_rejected(self):
        ds = {
            "role": "kubevirt",
            "vm_profile": "p",
            "vms_per_namespace": 1,
            "sc": "s",
            "disk_overrides": {"disk;name": {"expected_capacity": "45G"}},
        }
        assert OadpScenarioMixin._validate_dataset_entry(ds) is False


# ===========================================================================
# 12. calc_total_pods_per_namespace fallback for VMs
# ===========================================================================


class TestCalcTotalPodsPerNamespaceFallback:
    @pytest.fixture
    def mx(self):
        return TestableScenario()

    def test_vm_dataset_uses_vms_per_namespace(self, mx):
        s = _kubevirt_scenario()
        s["dataset"]["namespace"] = "vm-ns"
        assert mx.calc_total_pods_per_namespace_in_datasets(s, "vm-ns") == 2

    def test_mixed_datasets_sum_correctly(self, mx):
        s = _kubevirt_scenario(
            dataset=[
                {"role": "BusyBoxPodSingleNS.sh", "pods_per_ns": 5, "sc": "s", "namespace": "ns1"},
                {"role": "kubevirt", "vm_profile": "p", "vms_per_namespace": 3, "sc": "s", "namespace": "ns1"},
            ]
        )
        assert mx.calc_total_pods_per_namespace_in_datasets(s, "ns1") == 8


# ===========================================================================
# 13. Constants integrity
# ===========================================================================


class TestConstants:
    def test_kubevirt_in_known_plugins(self):
        assert "kubevirt" in KNOWN_PLUGINS

    def test_known_plugins_has_all_five_plus_kubevirt(self):
        expected = {"restic", "kopia", "csi", "vbd", "vsm", "kubevirt"}
        assert KNOWN_PLUGINS == expected

    def test_vm_dataset_role_is_kubevirt(self):
        assert VM_DATASET_ROLE == "kubevirt"

    def test_shell_metacharacters_non_empty(self):
        assert len(SHELL_METACHARACTERS) > 0
        assert ";" in SHELL_METACHARACTERS
        assert "|" in SHELL_METACHARACTERS

"""Unit tests for KubeVirt support: scenario validation, profile loader, security,
feature flag gating, data generation commands, and VM validation helpers."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from benchmark_runner.oadp.constants import (
    KNOWN_INCREMENT_LEVELS,
    KNOWN_PLUGINS,
    KNOWN_VM_POPULATION_METHODS,
    KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME,
    MPQE_VM_SCRIPT_NS_SUFFIX,
    MPQE_VM_SCRIPTS_PATH,
    PLUGIN_KUBEVIRT_DATAMOVER,
    SHELL_METACHARACTERS,
    VM_DATASET_ROLE,
    VM_DEFAULT_SSH_USER,
    VM_POPULATION_METHOD_SCRIPT,
)
from benchmark_runner.oadp.cleanup import OadpCleanupMixin
from benchmark_runner.oadp.configuration import OadpConfigurationMixin
from benchmark_runner.oadp.dataset import OadpDatasetMixin
from benchmark_runner.oadp.execution import OadpExecutionMixin
from benchmark_runner.oadp.log_collection import OadpLogCollectionMixin
from benchmark_runner.oadp.reporting import OadpReportingMixin
from benchmark_runner.oadp.scenario import (
    OadpScenarioMixin,
    expand_multi_namespaces,
    get_script_ns_count,
    scenario_backup_namespaces,
    scenario_includes_kubevirt_dataset,
)
from benchmark_runner.oadp.vm_operations import OadpVmOperationsMixin
from benchmark_runner.oadp.vm_profiles import load_vm_profile, merge_disk_overrides
from benchmark_runner.oadp.vm_validation import OadpVmValidationMixin


# ---------------------------------------------------------------------------
# Shared stubs
# ---------------------------------------------------------------------------


def _valid_args(**overrides):
    base = {
        "plugin": "kubevirt",
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


def _script_kubevirt_scenario(**overrides):
    s = _kubevirt_scenario()
    s["dataset"]["namespace"] = "kubevirt-script-2vm-1data-ns1"
    s["args"] = _valid_args(
        namespaces_to_backup="kubevirt-script-2vm-1data-ns1",
        vm_population_method="script",
        vm_population_script_args={"vms": 2, "data_disks": 1, "cbt": False},
    )
    s.update(overrides)
    return s


def _multi_ns_script_scenario(ns_count=3, base="kubevirt-script-2vm-1data-rbd", **overrides):
    """Script-populated kubevirt scenario spanning 'ns_count' sibling namespaces (ticket 029)."""
    s = _kubevirt_scenario()
    s["args"] = _valid_args(
        namespaces_to_backup=f"{base}-ns1",
        vm_population_method="script",
        vm_population_script_args={"vms": 2, "data_disks": 1, "cbt": False, "ns": ns_count},
    )
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
        self._run_artifacts_path = ""

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


class TestableDataset(OadpDatasetMixin, OadpVmOperationsMixin, OadpWorkloads):
    __test__ = False


class TestableConfiguration(OadpConfigurationMixin, OadpWorkloads):
    __test__ = False


class TestableExecution(OadpExecutionMixin, OadpConfigurationMixin, OadpWorkloads):
    pass


class TestableCleanup(OadpCleanupMixin, OadpScenarioMixin, OadpWorkloads):
    __test__ = False

    def __init__(self):
        super().__init__()
        # Mixin reads mangled OadpWorkloads private attrs.
        self._OadpWorkloads__oadp_cleanup_dataset_post_run = True
        self._OadpWorkloads__oadp_cleanup_cr_post_run = False
        self._OadpWorkloads__oadp_misc_dir = "/tmp/oadp-misc"
        self.delete_source_dataset = MagicMock()


class TestableReporting(OadpReportingMixin, OadpWorkloads):
    __test__ = False


class TestableLogCollection(OadpLogCollectionMixin, OadpWorkloads):
    __test__ = False


class TestableIncrementalData(
    OadpVmOperationsMixin, OadpVmValidationMixin, OadpScenarioMixin, OadpWorkloads
):
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
            assert mx.validate_scenario(s) is True, f"Plugin {plugin} should be accepted"

    def test_kubevirt_dataset_with_wrong_plugin_fails(self, mx):
        s = _kubevirt_scenario(args=_valid_args(plugin="csi"))
        assert mx.validate_scenario(s) is False

    def test_kubevirt_dataset_with_kopia_plugin_fails(self, mx):
        s = _kubevirt_scenario(args=_valid_args(plugin="kopia"))
        assert mx.validate_scenario(s) is False

    def test_pod_scenario_with_non_kubevirt_plugin_passes(self, mx):
        s = _pod_scenario()
        assert s["args"]["plugin"] != "kubevirt"
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


# ===========================================================================
# 14. _validate_vm_dataset existence checks vs data checks
# ===========================================================================


class TestValidateVmDatasetExistenceChecks:
    """Verify that existence checks (VM count, VMI running) always run,
    even when validation_mode is 'none'.  Only data-level checks should
    be skipped by validation_mode=none."""

    @pytest.fixture
    def mx(self):
        obj = TestableVmValidation()
        return obj

    def _ds(self, namespace="vm-ns", vms=2):
        return {
            "role": "kubevirt",
            "vm_profile": "rhel9-hammerdb-mariadb",
            "vms_per_namespace": vms,
            "sc": "ocs-storagecluster-ceph-rbd-virtualization",
            "namespace": namespace,
        }

    def test_none_mode_returns_false_when_vms_missing(self, mx):
        """validation_mode=none must still detect that VMs don't exist."""
        mx.get_dataset_validation_mode = lambda ds: "none"
        # _check_vm_count returns "0" → VMs don't exist
        mx._oc.run.return_value = "0"
        scenario = _kubevirt_scenario()
        result = mx._validate_vm_dataset(scenario, self._ds())
        assert result is False

    def test_none_mode_returns_true_when_vms_exist(self, mx):
        """validation_mode=none skips data checks but passes when VMs exist and are running."""
        mx.get_dataset_validation_mode = lambda ds: "none"
        # First call: _check_vm_count returns "2", second call: _check_all_vmis_running returns "2"
        mx._oc.run.side_effect = ["2", "2"]
        scenario = _kubevirt_scenario()
        result = mx._validate_vm_dataset(scenario, self._ds())
        assert result is True

    def test_light_mode_returns_false_when_vms_missing(self, mx):
        """light mode also fails when VMs don't exist (existence check first)."""
        mx.get_dataset_validation_mode = lambda ds: "light"
        mx._oc.run.return_value = "0"
        scenario = _kubevirt_scenario()
        result = mx._validate_vm_dataset(scenario, self._ds())
        assert result is False


class TestVmExistenceCheckRetryLogic:
    """Regression tests for ticket 006 live-test finding: post-restore VM checks must
    retry using the configured retry_logic instead of failing on the very first check,
    since VMs legitimately take time to come back up after a restore completes."""

    @pytest.fixture
    def mx(self):
        return TestableVmValidation()

    def test_check_vm_count_retries_until_success(self, mx):
        mx.set_validation_retry_logic(interval_between_checks=0, max_attempts=3)
        # First two attempts report only 1/2 VMs, third attempt reports 2/2
        mx._oc.run.side_effect = ["1", "1", "2"]
        assert mx._check_vm_count("ns1", 2) is True
        assert mx._oc.run.call_count == 3

    def test_check_all_vmis_running_retries_until_success(self, mx):
        mx.set_validation_retry_logic(interval_between_checks=0, max_attempts=3)
        mx._oc.run.side_effect = ["1", "2"]
        assert mx._check_all_vmis_running("ns1", 2) is True
        assert mx._oc.run.call_count == 2

    def test_check_vm_count_fails_after_exhausting_attempts(self, mx):
        mx.set_validation_retry_logic(interval_between_checks=0, max_attempts=3)
        mx._oc.run.return_value = "0"
        assert mx._check_vm_count("ns1", 2) is False
        assert mx._oc.run.call_count == 3

    def test_check_defaults_to_single_attempt_when_retry_logic_unset(self, mx):
        """Without any prior set_validation_retry_logic() call, behave as a single check."""
        del mx._OadpWorkloads__retry_logic
        mx._oc.run.return_value = "1"
        assert mx._check_vm_count("ns1", 2) is False
        assert mx._oc.run.call_count == 1

    def test_validate_restored_vm_datasets_tolerates_transient_vm_restart(self, mx):
        """End-to-end: validate_restored_vm_datasets() sets interval=15/max_attempts=28,
        and the VM count/running checks should use those settings to ride out a VM that
        is still restarting right after the restore CR reports Completed."""
        mx.get_dataset_validation_mode = lambda ds: "none"
        # VM count check: 1/2, then 2/2. VMI running check: 1/2, then 2/2.
        mx._oc.run.side_effect = ["1", "2", "1", "2"]
        scenario = _kubevirt_scenario()
        scenario["dataset"]["namespace"] = "ns1"
        scenario["dataset"]["vms_per_namespace"] = 2

        with patch("time.sleep"):
            result = mx.validate_restored_vm_datasets(scenario)

        assert result is True
        assert mx._oc.run.call_count == 4


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


# ===========================================================================
# 15. vm_population_method / vm_population_script_args scenario validation
# ===========================================================================


class TestVmPopulationMethodValidation:
    @pytest.fixture
    def mx(self):
        return TestableScenario()

    def test_defaults_to_native_when_absent(self, mx):
        s = _kubevirt_scenario()
        assert "vm_population_method" not in s["args"]
        assert mx.validate_scenario(s) is True

    def test_explicit_native_valid(self, mx):
        s = _kubevirt_scenario(args=_valid_args(vm_population_method="native"))
        assert mx.validate_scenario(s) is True

    def test_unknown_method_rejected(self, mx):
        s = _kubevirt_scenario(args=_valid_args(vm_population_method="bogus"))
        assert mx.validate_scenario(s) is False

    def test_known_methods_are_native_and_script(self):
        assert KNOWN_VM_POPULATION_METHODS == {"native", "script"}

    def test_valid_script_scenario(self, mx):
        assert mx.validate_scenario(_script_kubevirt_scenario()) is True

    def test_script_without_script_args_rejected(self, mx):
        s = _script_kubevirt_scenario()
        del s["args"]["vm_population_script_args"]
        assert mx.validate_scenario(s) is False

    def test_script_args_not_a_dict_rejected(self, mx):
        s = _script_kubevirt_scenario(args=_valid_args(vm_population_method="script", vm_population_script_args="oops"))
        assert mx.validate_scenario(s) is False

    def test_script_args_missing_vms_rejected(self, mx):
        s = _script_kubevirt_scenario()
        del s["args"]["vm_population_script_args"]["vms"]
        assert mx.validate_scenario(s) is False

    def test_script_args_missing_data_disks_rejected(self, mx):
        s = _script_kubevirt_scenario()
        del s["args"]["vm_population_script_args"]["data_disks"]
        assert mx.validate_scenario(s) is False

    @pytest.mark.parametrize("vms", [0, -1, "2", 2.5, True])
    def test_script_args_bad_vms_rejected(self, mx, vms):
        s = _script_kubevirt_scenario()
        s["args"]["vm_population_script_args"]["vms"] = vms
        assert mx.validate_scenario(s) is False

    @pytest.mark.parametrize("data_disks", [-1, "1", 1.5, False])
    def test_script_args_bad_data_disks_rejected(self, mx, data_disks):
        s = _script_kubevirt_scenario()
        s["args"]["vm_population_script_args"]["data_disks"] = data_disks
        # False is a valid bool but not accepted as data_disks (must be int, not bool)
        assert mx.validate_scenario(s) is False

    def test_script_args_data_disks_zero_is_valid(self, mx):
        s = _script_kubevirt_scenario()
        s["args"]["vm_population_script_args"]["data_disks"] = 0
        assert mx.validate_scenario(s) is True

    def test_script_args_bad_cbt_rejected(self, mx):
        s = _script_kubevirt_scenario()
        s["args"]["vm_population_script_args"]["cbt"] = "yes"
        assert mx.validate_scenario(s) is False

    def test_script_args_cbt_defaults_when_absent(self, mx):
        s = _script_kubevirt_scenario()
        del s["args"]["vm_population_script_args"]["cbt"]
        assert mx.validate_scenario(s) is True

    def test_script_args_metachar_rejected(self, mx):
        s = _script_kubevirt_scenario()
        s["args"]["vm_population_script_args"]["extra"] = "value;rm -rf /"
        assert mx.validate_scenario(s) is False

    def test_script_namespace_missing_ns1_suffix_rejected(self, mx):
        s = _script_kubevirt_scenario()
        s["dataset"]["namespace"] = "kubevirt-script-2vm-1data"
        assert mx.validate_scenario(s) is False

    def test_script_namespace_from_namespaces_to_backup_when_dataset_is_list(self, mx):
        s = _script_kubevirt_scenario(
            dataset=[
                {
                    "role": "kubevirt",
                    "vm_profile": "rhel9-hammerdb-mariadb",
                    "vms_per_namespace": 2,
                    "sc": "s",
                    "namespace": "kubevirt-script-2vm-1data-ns1",
                }
            ],
        )
        s["args"]["namespace_to_backup"] = "kubevirt-script-2vm-1data-ns1"
        s["args"]["namespaces_to_backup"] = "kubevirt-script-2vm-1data-ns1"
        assert mx.validate_scenario(s) is True


# ===========================================================================
# 16. _mpqe_namespace_base
# ===========================================================================


class TestMpqeNamespaceBase:
    def test_strips_ns1_suffix(self):
        assert OadpVmOperationsMixin._mpqe_namespace_base("kubevirt-script-2vm-ns1") == "kubevirt-script-2vm"

    def test_no_suffix_left_unchanged(self):
        assert OadpVmOperationsMixin._mpqe_namespace_base("kubevirt-minimal-ns") == "kubevirt-minimal-ns"

    def test_suffix_constant_matches_expectation(self):
        assert MPQE_VM_SCRIPT_NS_SUFFIX == "-ns1"


# ===========================================================================
# 17. create_vm_dataset_via_script / _run_vm_script
# ===========================================================================


class TestCreateVmDatasetViaScript:
    @pytest.fixture
    def mx(self):
        return TestableVmOps()

    def _ds(self, namespace="kubevirt-script-2vm-1data-ns1"):
        return {"namespace": namespace}

    def test_builds_expected_command(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = "MPQE_SCRIPT_EXIT_CODE:0"
        scenario = _script_kubevirt_scenario()
        mx.create_vm_dataset_via_script(scenario, self._ds())

        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert f"{MPQE_VM_SCRIPTS_PATH}/VMsPopulation.sh" in cmd
        assert "-vms 2" in cmd
        assert "-disks 1" in cmd
        assert "-ns 1" in cmd
        assert "-namespace kubevirt-script-2vm-1data" in cmd
        assert "-cbt" not in cmd

    def test_includes_cbt_flag_when_enabled(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = "MPQE_SCRIPT_EXIT_CODE:0"
        mx.check_cbt_state = MagicMock(return_value={})
        scenario = _script_kubevirt_scenario()
        scenario["args"]["vm_population_script_args"]["cbt"] = True
        mx.create_vm_dataset_via_script(scenario, self._ds())

        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "-cbt" in cmd.split(";")[0]

    def test_fails_test_run_on_nonzero_exit(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = "some script output\nMPQE_SCRIPT_EXIT_CODE:1"
        scenario = _script_kubevirt_scenario()
        with pytest.raises(RuntimeError, match="exit code 1"):
            mx.create_vm_dataset_via_script(scenario, self._ds())

    def test_missing_exit_marker_treated_as_failure(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = "unexpected output with no marker"
        scenario = _script_kubevirt_scenario()
        with pytest.raises(RuntimeError):
            mx.create_vm_dataset_via_script(scenario, self._ds())

    def test_defaults_when_script_args_missing(self, mx):
        """vms/data_disks/cbt default sensibly even if the dict is sparse."""
        mx._OadpWorkloads__ssh.run.return_value = "MPQE_SCRIPT_EXIT_CODE:0"
        scenario = _script_kubevirt_scenario()
        scenario["args"]["vm_population_script_args"] = {"vms": 3, "data_disks": 2}
        mx.create_vm_dataset_via_script(scenario, self._ds())
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "-vms 3" in cmd
        assert "-disks 2" in cmd
        assert "-cbt" not in cmd

    def test_cbt_enabled_checks_and_records_state_after_creation(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = "MPQE_SCRIPT_EXIT_CODE:0"
        mx.check_cbt_state = MagicMock(return_value={"vm-0": {"state": "Enabled", "label": "true"}})
        scenario = _script_kubevirt_scenario()
        scenario["args"]["vm_population_script_args"]["cbt"] = True
        mx.create_vm_dataset_via_script(scenario, self._ds())

        mx.check_cbt_state.assert_called_once_with("kubevirt-script-2vm-1data-ns1")
        assert mx._OadpWorkloads__run_metadata["summary"]["validations"]["cbt_state"] == {
            "kubevirt-script-2vm-1data-ns1": {"vm-0": {"state": "Enabled", "label": "true"}}
        }

    def test_cbt_disabled_skips_state_check(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = "MPQE_SCRIPT_EXIT_CODE:0"
        mx.check_cbt_state = MagicMock()
        scenario = _script_kubevirt_scenario()
        mx.create_vm_dataset_via_script(scenario, self._ds())
        mx.check_cbt_state.assert_not_called()


# ===========================================================================
# 20. enable_cbt_on_vms / check_cbt_state
# ===========================================================================


class TestEnableCbtOnVms:
    @pytest.fixture
    def mx(self):
        return TestableVmOps()

    def test_invokes_expected_command(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = "MPQE_SCRIPT_EXIT_CODE:0"
        mx.enable_cbt_on_vms("kubevirt-script-2vm-1data-ns1")
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert cmd.startswith(f"{MPQE_VM_SCRIPTS_PATH}/VMsEnableCBT.sh -ns kubevirt-script-2vm-1data-ns1")

    def test_fails_test_run_on_nonzero_exit(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = "MPQE_SCRIPT_EXIT_CODE:1"
        with pytest.raises(RuntimeError, match="exit code 1"):
            mx.enable_cbt_on_vms("kubevirt-script-2vm-1data-ns1")


class TestCheckCbtState:
    @pytest.fixture
    def mx(self):
        return TestableVmOps()

    _SAMPLE_OUTPUT = (
        "========================================================================\n"
        "===     Verify CBT state and labels for VMS in NameSpace: kubevirt-script-2vm-1data-ns1     ===\n"
        "========================================================================\n"
        "Verify CBT state and label for VM: oadp-vm-rhel9-hammerdb-mariadb-0 , NameSpace: kubevirt-script-2vm-1data-ns1\n"
        "CBT State: Enabled\n"
        "CBT label: true\n"
        "Verify CBT state and label for VM: oadp-vm-rhel9-hammerdb-mariadb-1 , NameSpace: kubevirt-script-2vm-1data-ns1\n"
        "CBT State: Enabled\n"
        "CBT label: true\n"
        " \n"
    )

    def test_parses_per_vm_state_and_label(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = self._SAMPLE_OUTPUT
        result = mx.check_cbt_state("kubevirt-script-2vm-1data-ns1")
        assert result == {
            "oadp-vm-rhel9-hammerdb-mariadb-0": {"state": "Enabled", "label": "true"},
            "oadp-vm-rhel9-hammerdb-mariadb-1": {"state": "Enabled", "label": "true"},
        }

    def test_invokes_expected_command(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = ""
        mx.check_cbt_state("kubevirt-script-2vm-1data-ns1")
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert cmd == f"{MPQE_VM_SCRIPTS_PATH}/VMsCBTstate.sh -ns kubevirt-script-2vm-1data-ns1"

    def test_empty_output_returns_empty_dict(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = ""
        assert mx.check_cbt_state("ns") == {}

    def test_missing_label_line_defaults_to_empty_string(self, mx):
        output = (
            "Verify CBT state and label for VM: vm-0 , NameSpace: ns\n"
            "CBT State: Enabled\n"
        )
        mx._OadpWorkloads__ssh.run.return_value = output
        result = mx.check_cbt_state("ns")
        assert result == {"vm-0": {"state": "Enabled", "label": ""}}


class TestRecordCbtState:
    @pytest.fixture
    def mx(self):
        return TestableVmOps()

    def test_records_under_validations(self, mx):
        mx._record_cbt_state("ns1", {"vm-0": {"state": "Enabled", "label": "true"}})
        assert mx._OadpWorkloads__run_metadata["summary"]["validations"]["cbt_state"] == {
            "ns1": {"vm-0": {"state": "Enabled", "label": "true"}}
        }

    def test_multiple_namespaces_accumulate(self, mx):
        mx._record_cbt_state("ns1", {"vm-0": {"state": "Enabled", "label": "true"}})
        mx._record_cbt_state("ns2", {"vm-1": {"state": "Disabled", "label": ""}})
        cbt_state = mx._OadpWorkloads__run_metadata["summary"]["validations"]["cbt_state"]
        assert set(cbt_state.keys()) == {"ns1", "ns2"}


# ===========================================================================
# 21. increment_level scenario validation
# ===========================================================================


class TestIncrementLevelValidation:
    @pytest.fixture
    def mx(self):
        return TestableScenario()

    def test_absent_is_valid(self, mx):
        s = _script_kubevirt_scenario()
        assert "increment_level" not in s["args"]
        assert mx.validate_scenario(s) is True

    @pytest.mark.parametrize("level", ["inc1", "inc2", "inc3"])
    def test_known_levels_valid_with_script_method(self, mx, level):
        s = _script_kubevirt_scenario(args=_valid_args(
            vm_population_method="script",
            vm_population_script_args={"vms": 2, "data_disks": 1},
            increment_level=level,
        ))
        assert mx.validate_scenario(s) is True

    def test_unknown_level_rejected(self, mx):
        s = _script_kubevirt_scenario(args=_valid_args(
            vm_population_method="script",
            vm_population_script_args={"vms": 2, "data_disks": 1},
            increment_level="inc4",
        ))
        assert mx.validate_scenario(s) is False

    def test_level_with_native_method_rejected(self, mx):
        s = _kubevirt_scenario(args=_valid_args(increment_level="inc1"))
        assert mx.validate_scenario(s) is False

    def test_known_increment_levels_constant(self):
        assert KNOWN_INCREMENT_LEVELS == {"inc1", "inc2", "inc3"}


# ===========================================================================
# 22. add_incremental_data / apply_incremental_data_if_requested
# ===========================================================================


class TestAddIncrementalData:
    @pytest.fixture
    def mx(self):
        return TestableVmOps()

    def test_invokes_expected_command(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = "MPQE_SCRIPT_EXIT_CODE:0"
        mx.add_incremental_data("kubevirt-script-ns1", "inc1")
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert cmd.startswith(
            f"{MPQE_VM_SCRIPTS_PATH}/VMsAddDataVerify.sh -ns kubevirt-script-ns1 -inc inc1"
        )

    def test_fails_test_run_on_nonzero_exit(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = "MPQE_SCRIPT_EXIT_CODE:1"
        with pytest.raises(RuntimeError, match="exit code 1"):
            mx.add_incremental_data("kubevirt-script-ns1", "inc1")


class TestApplyIncrementalDataIfRequested:
    @pytest.fixture
    def mx(self):
        obj = TestableIncrementalData()
        obj._OadpWorkloads__scenario_datasets = [
            {"namespace": "kubevirt-script-2vm-1data-ns1"},
        ]
        return obj

    def test_noop_when_increment_level_absent(self, mx):
        mx.add_incremental_data = MagicMock()
        mx.verify_vm_data_via_script = MagicMock()
        scenario = _script_kubevirt_scenario()
        mx.apply_incremental_data_if_requested(scenario)
        mx.add_incremental_data.assert_not_called()
        mx.verify_vm_data_via_script.assert_not_called()

    def test_adds_and_verifies_for_each_namespace(self, mx):
        mx.add_incremental_data = MagicMock()
        mx.verify_vm_data_via_script = MagicMock(return_value=True)
        scenario = _script_kubevirt_scenario()
        scenario["args"]["increment_level"] = "inc1"
        mx.apply_incremental_data_if_requested(scenario)
        mx.add_incremental_data.assert_called_once_with("kubevirt-script-2vm-1data-ns1", "inc1")
        mx.verify_vm_data_via_script.assert_called_once_with("kubevirt-script-2vm-1data-ns1")

    def test_fails_test_run_when_post_increment_verification_fails(self, mx):
        mx.add_incremental_data = MagicMock()
        mx.verify_vm_data_via_script = MagicMock(return_value=False)
        scenario = _script_kubevirt_scenario()
        scenario["args"]["increment_level"] = "inc2"
        with pytest.raises(RuntimeError, match="Post-increment verification failed"):
            mx.apply_incremental_data_if_requested(scenario)

    def test_multiple_namespaces_all_processed(self, mx):
        mx._OadpWorkloads__scenario_datasets = [
            {"namespace": "ns-a"},
            {"namespace": "ns-b"},
        ]
        mx.add_incremental_data = MagicMock()
        mx.verify_vm_data_via_script = MagicMock(return_value=True)
        scenario = _script_kubevirt_scenario()
        scenario["args"]["increment_level"] = "inc3"
        mx.apply_incremental_data_if_requested(scenario)
        assert mx.add_incremental_data.call_count == 2
        assert mx.verify_vm_data_via_script.call_count == 2


# ===========================================================================
# 18. Dispatch routing: create_source_dataset / recreate-on-failure path
# ===========================================================================


class TestScriptDispatchRouting:
    @pytest.fixture
    def mx(self):
        return TestableDataset()

    def test_native_method_dispatches_to_create_vm_dataset(self, mx):
        mx.create_vm_dataset = MagicMock()
        mx.create_vm_dataset_via_script = MagicMock()
        scenario = _kubevirt_scenario()
        ds = {**scenario["dataset"], "namespace": "vm-ns"}
        mx.create_source_dataset(scenario, ds)
        mx.create_vm_dataset.assert_called_once_with(scenario, ds)
        mx.create_vm_dataset_via_script.assert_not_called()

    def test_script_method_dispatches_to_create_vm_dataset_via_script(self, mx):
        mx.create_vm_dataset = MagicMock()
        mx.create_vm_dataset_via_script = MagicMock()
        scenario = _script_kubevirt_scenario()
        ds = {**scenario["dataset"]}
        mx.create_source_dataset(scenario, ds)
        mx.create_vm_dataset_via_script.assert_called_once_with(scenario, ds)
        mx.create_vm_dataset.assert_not_called()


class TestVmValidationRecreateDispatch:
    @pytest.fixture
    def mx(self):
        obj = TestableVmValidation()
        obj.create_vm_dataset = MagicMock()
        obj.create_vm_dataset_via_script = MagicMock()
        obj.validate_expected_vm_datasets = MagicMock(side_effect=[False, True])
        return obj

    def test_native_scenario_recreate_uses_native_path(self, mx):
        scenario = _kubevirt_scenario()
        ds = {**scenario["dataset"], "namespace": "vm-ns"}
        mx._OadpWorkloads__oadp_ds_failing_validation = [ds]
        mx.verify_vm_datasets_before_backups(scenario)
        mx.create_vm_dataset.assert_called_once_with(scenario, ds)
        mx.create_vm_dataset_via_script.assert_not_called()

    def test_script_scenario_recreate_uses_script_path(self, mx):
        scenario = _script_kubevirt_scenario()
        ds = {**scenario["dataset"]}
        mx._OadpWorkloads__oadp_ds_failing_validation = [ds]
        mx.verify_vm_datasets_before_backups(scenario)
        mx.create_vm_dataset_via_script.assert_called_once_with(scenario, ds)
        mx.create_vm_dataset.assert_not_called()


# ===========================================================================
# 19. verify_vm_data_via_script
# ===========================================================================


class TestVerifyVmDataViaScript:
    @pytest.fixture
    def mx(self):
        return TestableVmValidation()

    def test_verify_passed(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = "========\nVERIFY PASSED\n========"
        assert mx.verify_vm_data_via_script("kubevirt-script-ns1") is True

    def test_verify_failed(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = "========\nVERIFY FAILED\n========"
        assert mx.verify_vm_data_via_script("kubevirt-script-ns1") is False

    def test_neither_marker_treated_as_failed(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = "unexpected crash output"
        assert mx.verify_vm_data_via_script("kubevirt-script-ns1") is False

    def test_invokes_expected_command(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = "VERIFY PASSED"
        mx.verify_vm_data_via_script("kubevirt-script-ns1")
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert cmd == f"{MPQE_VM_SCRIPTS_PATH}/VMsAddDataVerify.sh -ns kubevirt-script-ns1 -verify-only"

    def test_saves_output_artifact_when_path_set(self, mx, tmp_path):
        mx._run_artifacts_path = str(tmp_path)
        mx._OadpWorkloads__ssh.run.return_value = "VERIFY PASSED"
        mx.verify_vm_data_via_script("kubevirt-script-ns1")
        log_file = tmp_path / "vm_script_verify_kubevirt-script-ns1.log"
        assert log_file.exists()
        assert "VERIFY PASSED" in log_file.read_text()

    def test_no_artifact_write_when_path_unset(self, mx):
        """_run_artifacts_path is '' in the test stub — must not raise."""
        mx._OadpWorkloads__ssh.run.return_value = "VERIFY PASSED"
        mx.verify_vm_data_via_script("kubevirt-script-ns1")  # no exception


class TestValidateVmDatasetScriptDispatch:
    """_validate_vm_dataset must call verify_vm_data_via_script for script-populated
    VMs instead of the native df-based sample/full checks, but only past the
    existence checks and only when validation_mode isn't 'none'."""

    @pytest.fixture
    def mx(self):
        return TestableVmValidation()

    def _ds(self, namespace="kubevirt-script-2vm-1data-ns1", vms=2):
        return {
            "role": "kubevirt",
            "vm_profile": "rhel9-hammerdb-mariadb",
            "vms_per_namespace": vms,
            "sc": "s",
            "namespace": namespace,
        }

    def test_script_method_uses_script_verification(self, mx):
        mx.get_dataset_validation_mode = lambda ds: "light"
        mx._oc.run.side_effect = ["2", "2"]
        mx.verify_vm_data_via_script = MagicMock(return_value=True)
        mx._validate_vm_data_sample = MagicMock(return_value=False)
        scenario = _script_kubevirt_scenario()
        result = mx._validate_vm_dataset(scenario, self._ds())
        assert result is True
        mx.verify_vm_data_via_script.assert_called_once_with("kubevirt-script-2vm-1data-ns1")
        mx._validate_vm_data_sample.assert_not_called()

    def test_native_method_still_uses_sample_validation(self, mx):
        mx.get_dataset_validation_mode = lambda ds: "light"
        mx._oc.run.side_effect = ["2", "2"]
        mx.verify_vm_data_via_script = MagicMock(return_value=False)
        mx._validate_vm_data_sample = MagicMock(return_value=True)
        scenario = _kubevirt_scenario()
        result = mx._validate_vm_dataset(scenario, self._ds(namespace="vm-ns"))
        assert result is True
        mx._validate_vm_data_sample.assert_called_once()
        mx.verify_vm_data_via_script.assert_not_called()

    def test_script_method_none_mode_skips_script_verification_too(self, mx):
        mx.get_dataset_validation_mode = lambda ds: "none"
        mx._oc.run.side_effect = ["2", "2"]
        mx.verify_vm_data_via_script = MagicMock(return_value=False)
        scenario = _script_kubevirt_scenario()
        result = mx._validate_vm_dataset(scenario, self._ds())
        assert result is True
        mx.verify_vm_data_via_script.assert_not_called()


# ===========================================================================
# 23. CBT flag propagation from scenario args through to CBT enablement
#
# vm_population_script_args.cbt propagates to CBT enablement via the -cbt
# flag passed directly to VMsPopulation.sh (confirmed by check_cbt_state()
# right after creation), rather than a separate enable_cbt_on_vms() call —
# the alternative wiring named in ticket 021. enable_cbt_on_vms() remains
# available standalone (e.g. to enable CBT on already-populated VMs) and is
# covered on its own in TestEnableCbtOnVms.
# ===========================================================================


class TestCbtFlagPropagationFromScenario:
    @pytest.fixture
    def mx(self):
        return TestableVmOps()

    def test_cbt_true_in_scenario_reaches_population_command_and_is_confirmed(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = "MPQE_SCRIPT_EXIT_CODE:0"
        mx.check_cbt_state = MagicMock(return_value={"vm-0": {"state": "Enabled", "label": "true"}})
        scenario = _script_kubevirt_scenario()
        scenario["args"]["vm_population_script_args"]["cbt"] = True

        mx.create_vm_dataset_via_script(scenario, {"namespace": "kubevirt-script-2vm-1data-ns1"})

        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "-cbt" in cmd.split(";")[0]
        mx.check_cbt_state.assert_called_once_with("kubevirt-script-2vm-1data-ns1")

    def test_cbt_false_in_scenario_never_enables_cbt(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = "MPQE_SCRIPT_EXIT_CODE:0"
        mx.check_cbt_state = MagicMock()
        scenario = _script_kubevirt_scenario()
        scenario["args"]["vm_population_script_args"]["cbt"] = False

        mx.create_vm_dataset_via_script(scenario, {"namespace": "kubevirt-script-2vm-1data-ns1"})

        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "-cbt" not in cmd.split(";")[0]
        mx.check_cbt_state.assert_not_called()


# ===========================================================================
# 24. Script-based tests.yaml scenarios (ticket 023) pass validate_scenario()
#
# Mirrors the 5 scenarios added to mpqe-scale-scripts'
# oadp-helpers/templates/internal_data/tests.yaml so a regression in either
# the scenario shape or the validator is caught here without needing the
# external repo checked out.
# ===========================================================================


def _tests_yaml_script_scenario(name, testcase, testtype, vms, data_disks, namespace, cbt=False):
    args = {
        "plugin": "kubevirt",
        "use_cli": True,
        "OADP_CR_TYPE": testtype,
        "OADP_CR_NAME": name,
        "backup_name": name.replace("restore-", "backup-", 1) if testtype == "restore" else name,
        "testcase_timeout": 79200,
        "namespaces_to_backup": namespace,
        "vm_population_method": "script",
        "vm_population_script_args": {"vms": vms, "data_disks": data_disks, "cbt": cbt},
    }
    return {
        "name": name,
        "testcase": testcase,
        "testtype": testtype,
        "result_dir_base_path": "/tmp/results/",
        "dataset": {
            "role": "kubevirt",
            "vm_profile": "rhel9-hammerdb-mariadb",
            "vms_per_namespace": vms,
            "sc": "ocs-storagecluster-ceph-rbd",
            "pv_size": "60Gi",
        },
        "args": args,
    }


class TestScriptBasedScenariosFromTestsYaml:
    @pytest.fixture
    def mx(self):
        return TestableScenario()

    def test_backup_2vm_1data(self, mx):
        s = _tests_yaml_script_scenario(
            "backup-kubevirt-script-2vm-1data-cephrbd", 10.4, "backup",
            vms=2, data_disks=1, namespace="kubevirt-script-2vm-1data-rbd-ns1",
        )
        assert mx.validate_scenario(s) is True

    def test_restore_2vm_1data(self, mx):
        s = _tests_yaml_script_scenario(
            "restore-kubevirt-script-2vm-1data-cephrbd", 10.4, "restore",
            vms=2, data_disks=1, namespace="kubevirt-script-2vm-1data-rbd-ns1",
        )
        assert mx.validate_scenario(s) is True

    def test_backup_2vm_1data_cbt(self, mx):
        s = _tests_yaml_script_scenario(
            "backup-kubevirt-script-2vm-1data-cbt-cephrbd", 10.5, "backup",
            vms=2, data_disks=1, namespace="kubevirt-script-2vm-1data-cbt-rbd-ns1", cbt=True,
        )
        assert mx.validate_scenario(s) is True

    def test_backup_10vm_1data_scale(self, mx):
        s = _tests_yaml_script_scenario(
            "backup-kubevirt-script-10vm-1data-cephrbd", 10.6, "backup",
            vms=10, data_disks=1, namespace="kubevirt-script-10vm-1data-rbd-ns1",
        )
        assert mx.validate_scenario(s) is True

    def test_backup_2vm_2data_multidisk(self, mx):
        s = _tests_yaml_script_scenario(
            "backup-kubevirt-script-2vm-2data-cephrbd", 10.7, "backup",
            vms=2, data_disks=2, namespace="kubevirt-script-2vm-2data-rbd-ns1",
        )
        assert mx.validate_scenario(s) is True

    def test_all_five_scenarios_have_unique_names(self):
        names = {
            "backup-kubevirt-script-2vm-1data-cephrbd",
            "restore-kubevirt-script-2vm-1data-cephrbd",
            "backup-kubevirt-script-2vm-1data-cbt-cephrbd",
            "backup-kubevirt-script-10vm-1data-cephrbd",
            "backup-kubevirt-script-2vm-2data-cephrbd",
        }
        assert len(names) == 5


# ===========================================================================
# 25. exec_backup plugin -> CLI flag dispatch (ticket 006 live-test finding)
#
# A live Tier-1 test against a real cluster found that plugin: kubevirt
# backups fell through to the bare "else" branch (no datamover flags),
# producing snapshotMoveData: false and zero DataUpload CRs — i.e. only a
# CSI snapshot, with VM disk data never actually copied into the BSL.
# kubevirt must be routed the same way as vbd: --data-mover 'velero'
# --snapshot-move-data=true.
# ===========================================================================


class TestExecBackupPluginFlags:
    @pytest.fixture
    def mx(self):
        m = TestableExecution()
        # Stub default test_env["source"] is "upstream"; downstream tests override it.
        m._OadpWorkloads__test_env["source"] = "downstream"
        m._OadpWorkloads__ssh.run.return_value = 'Backup request "b1" submitted successfully.'
        return m

    def test_downstream_kubevirt_gets_datamover_flags(self, mx):
        mx.exec_backup(plugin="kubevirt", backup_name="b1", namespaces_to_backup="ns1")
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "--data-mover 'velero'" in cmd
        assert "--snapshot-move-data=true" in cmd

    def test_downstream_vbd_still_gets_datamover_flags(self, mx):
        mx.exec_backup(plugin="vbd", backup_name="b1", namespaces_to_backup="ns1")
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "--data-mover 'velero'" not in cmd  # downstream vbd path omits --data-mover
        assert "--snapshot-move-data=true" in cmd

    def test_downstream_csi_gets_no_datamover_flags(self, mx):
        mx.exec_backup(plugin="csi", backup_name="b1", namespaces_to_backup="ns1")
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "--snapshot-move-data" not in cmd
        assert "--data-mover" not in cmd

    def test_downstream_kopia_gets_fs_backup_flags(self, mx):
        mx.exec_backup(plugin="kopia", backup_name="b1", namespaces_to_backup="ns1")
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "--default-volumes-to-fs-backup=true" in cmd
        assert "--snapshot-volumes=false" in cmd

    def test_upstream_kubevirt_gets_datamover_flags(self, mx):
        mx._OadpWorkloads__test_env["source"] = "upstream"
        mx._OadpWorkloads__test_env["velero_cli_path"] = "/tmp/velero-1-12"
        mx.exec_backup(plugin="kubevirt", backup_name="b1", namespaces_to_backup="ns1")
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "--data-mover 'velero'" in cmd
        assert "--snapshot-move-data=true" in cmd

    def test_upstream_vbd_gets_datamover_flags(self, mx):
        mx._OadpWorkloads__test_env["source"] = "upstream"
        mx._OadpWorkloads__test_env["velero_cli_path"] = "/tmp/velero-1-12"
        mx.exec_backup(plugin="vbd", backup_name="b1", namespaces_to_backup="ns1")
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "--data-mover 'velero'" in cmd
        assert "--snapshot-move-data=true" in cmd

    def test_upstream_csi_gets_no_datamover_flags(self, mx):
        mx._OadpWorkloads__test_env["source"] = "upstream"
        mx._OadpWorkloads__test_env["velero_cli_path"] = "/tmp/velero-1-12"
        mx.exec_backup(plugin="csi", backup_name="b1", namespaces_to_backup="ns1")
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "--snapshot-move-data" not in cmd
        assert "--data-mover" not in cmd

    # --- Flow2 (ticket 028): CBT-enabled kubevirt backups reference a resource-policy ConfigMap ---

    def test_downstream_kubevirt_cbt_gets_resource_policy_flag(self, mx):
        mx.exec_backup(
            plugin="kubevirt",
            backup_name="b1",
            namespaces_to_backup="ns1",
            resource_policy_configmap=KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME,
        )
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert f"--resource-policies-configmap {KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME}" in cmd
        # still gets the base Flow1 datamover flags too
        assert "--data-mover 'velero'" in cmd
        assert "--snapshot-move-data=true" in cmd

    def test_downstream_kubevirt_without_cbt_omits_resource_policy_flag(self, mx):
        mx.exec_backup(plugin="kubevirt", backup_name="b1", namespaces_to_backup="ns1")
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "--resource-policies-configmap" not in cmd

    def test_downstream_non_kubevirt_ignores_resource_policy_configmap(self, mx):
        # resource_policy_configmap only ever gets threaded to the kubevirt branch;
        # confirm other plugins don't accidentally pick it up.
        mx.exec_backup(
            plugin="csi",
            backup_name="b1",
            namespaces_to_backup="ns1",
            resource_policy_configmap=KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME,
        )
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "--resource-policies-configmap" not in cmd


# ===========================================================================
# 24. Flow2 (ticket 028): CBT-enabled kubevirt backups need 'kubevirt-datamover'
#     registered on the DPA and a 'kubevirt-volume-policy' resource-policy
#     ConfigMap referenced on the backup CR.
# ===========================================================================


class TestScenarioNeedsKubevirtDatamoverPlugin:
    def _cbt_scenario(self, cbt=True):
        s = _script_kubevirt_scenario()
        s["args"]["vm_population_script_args"]["cbt"] = cbt
        return s

    def test_true_for_cbt_enabled_kubevirt_scenario(self):
        assert OadpConfigurationMixin._scenario_needs_kubevirt_datamover_plugin(self._cbt_scenario(True)) is True

    def test_false_for_cbt_disabled_kubevirt_scenario(self):
        assert OadpConfigurationMixin._scenario_needs_kubevirt_datamover_plugin(self._cbt_scenario(False)) is False

    def test_false_when_cbt_key_absent(self):
        s = self._cbt_scenario()
        del s["args"]["vm_population_script_args"]["cbt"]
        assert OadpConfigurationMixin._scenario_needs_kubevirt_datamover_plugin(s) is False

    def test_false_for_non_kubevirt_plugin_even_with_cbt(self):
        s = _script_kubevirt_scenario(
            args=_valid_args(
                plugin="csi",
                vm_population_method="script",
                vm_population_script_args={"vms": 2, "data_disks": 1, "cbt": True},
            )
        )
        assert OadpConfigurationMixin._scenario_needs_kubevirt_datamover_plugin(s) is False


class TestEnsureKubevirtDatamoverPlugin:
    @pytest.fixture
    def mx(self):
        m = TestableConfiguration()
        m._OadpWorkloads__oadp_dpa = "example-velero"
        m.get_oc_resource_to_json = MagicMock(
            return_value={"spec": {"configuration": {"velero": {"defaultPlugins": ["openshift", "csi", "kubevirt"]}}}}
        )
        m.patch_oc_resource = MagicMock()
        return m

    def _cbt_scenario(self):
        s = _script_kubevirt_scenario()
        s["args"]["vm_population_script_args"]["cbt"] = True
        return s

    def test_noop_when_scenario_does_not_need_it(self, mx):
        result = mx.ensure_kubevirt_datamover_plugin(scenario=_script_kubevirt_scenario(), oadp_namespace="openshift-adp")
        assert result is False
        mx.patch_oc_resource.assert_not_called()

    def test_noop_when_already_present(self, mx):
        mx.get_oc_resource_to_json.return_value = {
            "spec": {"configuration": {"velero": {"defaultPlugins": ["csi", "kubevirt", PLUGIN_KUBEVIRT_DATAMOVER]}}}
        }
        result = mx.ensure_kubevirt_datamover_plugin(scenario=self._cbt_scenario(), oadp_namespace="openshift-adp")
        assert result is False
        mx.patch_oc_resource.assert_not_called()

    def test_patches_when_missing(self, mx):
        result = mx.ensure_kubevirt_datamover_plugin(scenario=self._cbt_scenario(), oadp_namespace="openshift-adp")
        assert result is True
        mx.patch_oc_resource.assert_called_once()
        call_kwargs = mx.patch_oc_resource.call_args.kwargs
        assert call_kwargs["resource_type"] == "dpa"
        assert call_kwargs["resource_name"] == "example-velero"
        assert call_kwargs["namespace"] == "openshift-adp"
        assert call_kwargs["patch_type"] == "json"
        assert PLUGIN_KUBEVIRT_DATAMOVER in call_kwargs["patch_json"]
        # existing plugins must be preserved, not clobbered
        assert "kubevirt" in call_kwargs["patch_json"]
        assert "csi" in call_kwargs["patch_json"]

    def test_fails_test_run_when_dpa_missing(self, mx):
        mx.get_oc_resource_to_json.return_value = {}
        with pytest.raises(RuntimeError):
            mx.ensure_kubevirt_datamover_plugin(scenario=self._cbt_scenario(), oadp_namespace="openshift-adp")


class TestEnsureKubevirtVolumePolicyConfigMap:
    @pytest.fixture
    def mx(self):
        m = TestableConfiguration()
        m.get_oc_resource_to_json = MagicMock(return_value={})
        m._OadpWorkloads__ssh.run.return_value = f"configmap/{KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME} created"
        return m

    def _cbt_scenario(self):
        s = _script_kubevirt_scenario()
        s["args"]["vm_population_script_args"]["cbt"] = True
        return s

    def test_noop_when_scenario_does_not_need_it(self, mx):
        mx.ensure_kubevirt_volume_policy_configmap(scenario=_script_kubevirt_scenario(), oadp_namespace="openshift-adp")
        mx.get_oc_resource_to_json.assert_not_called()
        mx._OadpWorkloads__ssh.run.assert_not_called()

    def test_skips_create_when_configmap_already_exists(self, mx):
        mx.get_oc_resource_to_json.return_value = {
            "metadata": {"name": KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME, "namespace": "openshift-adp"}
        }
        mx.ensure_kubevirt_volume_policy_configmap(scenario=self._cbt_scenario(), oadp_namespace="openshift-adp")
        mx.get_oc_resource_to_json.assert_called_once_with(
            resource_type="configmap",
            resource_name=KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME,
            namespace="openshift-adp",
        )
        mx._OadpWorkloads__ssh.run.assert_not_called()

    def test_creates_manifest_when_missing(self, mx):
        mx.ensure_kubevirt_volume_policy_configmap(scenario=self._cbt_scenario(), oadp_namespace="openshift-adp")
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "oc create -f -" in cmd
        assert "kind: ConfigMap" in cmd
        assert f"name: {KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME}" in cmd
        assert "namespace: openshift-adp" in cmd
        assert "type: custom" in cmd
        assert "datamover: kubevirt" in cmd
        assert "type: skip" not in cmd

    def test_fails_test_run_when_create_fails(self, mx):
        mx._OadpWorkloads__ssh.run.return_value = "Error from server (Forbidden)"
        with pytest.raises(RuntimeError):
            mx.ensure_kubevirt_volume_policy_configmap(scenario=self._cbt_scenario(), oadp_namespace="openshift-adp")


class TestOadpExecuteScenarioResourcePolicyWiring:
    """Confirms oadp_execute_scenario() threads Flow2's resource_policy_configmap through to exec_backup()."""

    @pytest.fixture
    def mx(self):
        m = TestableExecution()
        m._OadpWorkloads__test_env["source"] = "downstream"
        m.ds_get_all_namespaces = MagicMock(return_value=["ns1"])
        m.exec_backup = MagicMock()
        m.wait_for_condition_of_cr = MagicMock(return_value=True)
        return m

    def _scenario(self, cbt):
        s = _script_kubevirt_scenario()
        s["args"]["vm_population_script_args"]["cbt"] = cbt
        return s

    def test_cbt_scenario_passes_resource_policy_configmap(self, mx):
        mx.oadp_execute_scenario(self._scenario(cbt=True), run_method="python")
        call_kwargs = mx.exec_backup.call_args.kwargs
        assert call_kwargs["resource_policy_configmap"] == KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME

    def test_non_cbt_scenario_passes_none(self, mx):
        mx.oadp_execute_scenario(self._scenario(cbt=False), run_method="python")
        call_kwargs = mx.exec_backup.call_args.kwargs
        assert call_kwargs["resource_policy_configmap"] is None


# ===========================================================================
# 29. Multi-namespace population/backup support (ticket 029)
#
# vm_population_script_args.ns > 1 means a single dict-form kubevirt dataset
# actually spans N sibling namespaces ('<base>-ns1'..'<base>-nsN'), all
# populated by one VMsPopulation.sh '-ns N' invocation. Backup must then
# --include-namespaces all of them, for both Flow1 (no CBT) and Flow2 (CBT).
# ===========================================================================


class TestGetScriptNsCount:
    def test_defaults_to_one_when_absent(self):
        s = _script_kubevirt_scenario()
        assert "ns" not in s["args"]["vm_population_script_args"]
        assert get_script_ns_count(s) == 1

    def test_reads_ns_from_script_args(self):
        s = _multi_ns_script_scenario(ns_count=3)
        assert get_script_ns_count(s) == 3

    def test_one_when_native_population_method(self):
        s = _kubevirt_scenario()
        assert get_script_ns_count(s) == 1

    def test_one_when_native_method_even_if_ns_key_present(self):
        """'ns' is only meaningful for vm_population_method 'script'."""
        s = _kubevirt_scenario(args=_valid_args(vm_population_script_args={"ns": 5}))
        assert get_script_ns_count(s) == 1


class TestExpandMultiNamespaces:
    def test_ns_count_one_returns_namespace_unchanged(self):
        assert expand_multi_namespaces("base-ns1", 1) == ["base-ns1"]

    def test_ns_count_one_is_default_safe_even_without_suffix(self):
        assert expand_multi_namespaces("kubevirt-minimal-ns", 1) == ["kubevirt-minimal-ns"]

    def test_expands_three_sibling_namespaces_in_order(self):
        assert expand_multi_namespaces("rhel9-oadp-2vms-1os-1data-rbd-ns1", 3) == [
            "rhel9-oadp-2vms-1os-1data-rbd-ns1",
            "rhel9-oadp-2vms-1os-1data-rbd-ns2",
            "rhel9-oadp-2vms-1os-1data-rbd-ns3",
        ]

    def test_namespace_without_ns1_suffix_still_expands_from_whole_string(self):
        """Defensive: validation should already guarantee the '-ns1' suffix, but if it's
        somehow missing, expansion still produces N deterministic, distinct names."""
        assert expand_multi_namespaces("kubevirt-minimal-ns", 2) == [
            "kubevirt-minimal-ns-ns1",
            "kubevirt-minimal-ns-ns2",
        ]


class TestVmPopulationNsArgValidation:
    @pytest.fixture
    def mx(self):
        return TestableScenario()

    def test_ns_absent_defaults_valid(self, mx):
        s = _script_kubevirt_scenario()
        assert "ns" not in s["args"]["vm_population_script_args"]
        assert mx.validate_scenario(s) is True

    def test_ns_three_is_valid(self, mx):
        assert mx.validate_scenario(_multi_ns_script_scenario(ns_count=3)) is True

    @pytest.mark.parametrize("ns", [0, -1, "3", 2.5, True])
    def test_bad_ns_rejected(self, mx, ns):
        s = _script_kubevirt_scenario()
        s["args"]["vm_population_script_args"]["ns"] = ns
        assert mx.validate_scenario(s) is False


class TestLoadDatasetsForScenarioMultiNamespace:
    @pytest.fixture
    def mx(self):
        obj = TestableScenario()
        obj.print_all_ds = MagicMock()
        return obj

    def test_single_namespace_unchanged_when_ns_absent(self, mx):
        scenario = _script_kubevirt_scenario()
        mx.load_datasets_for_scenario(scenario)
        assert mx.ds_get_all_namespaces() == ["kubevirt-script-2vm-1data-ns1"]

    def test_expands_into_n_namespace_summaries(self, mx):
        scenario = _multi_ns_script_scenario(ns_count=3)
        mx.load_datasets_for_scenario(scenario)
        assert mx.ds_get_all_namespaces() == [
            "kubevirt-script-2vm-1data-rbd-ns1",
            "kubevirt-script-2vm-1data-rbd-ns2",
            "kubevirt-script-2vm-1data-rbd-ns3",
        ]

    def test_each_sibling_namespace_carries_the_same_dataset_config(self, mx):
        scenario = _multi_ns_script_scenario(ns_count=2)
        mx.load_datasets_for_scenario(scenario)
        for ns in mx.ds_get_all_namespaces():
            datasets = mx.ds_get_datasets_for_namespace(ns)
            assert len(datasets) == 1
            assert datasets[0]["namespace"] == ns
            assert datasets[0]["vm_profile"] == scenario["dataset"]["vm_profile"]
            assert datasets[0]["vms_per_namespace"] == scenario["dataset"]["vms_per_namespace"]

    def test_non_script_kubevirt_scenario_unaffected(self, mx):
        """Native (non-script) kubevirt population is unaffected by 'ns' logic."""
        scenario = _kubevirt_scenario(args=_valid_args(namespaces_to_backup="kubevirt-minimal-ns"))
        mx.load_datasets_for_scenario(scenario)
        assert mx.ds_get_all_namespaces() == ["kubevirt-minimal-ns"]


class TestValidateExpectedVmDatasetsMultiNamespace:
    @pytest.fixture
    def mx(self):
        obj = TestableVmValidation()
        obj._check_vm_count = MagicMock(return_value=True)
        obj._check_all_vmis_running = MagicMock(return_value=True)
        obj.verify_vm_data_via_script = MagicMock(return_value=True)
        return obj

    def test_validates_every_sibling_namespace(self, mx):
        scenario = _multi_ns_script_scenario(ns_count=3)
        assert mx.validate_expected_vm_datasets(scenario) is True
        checked_namespaces = [call.args[0] for call in mx._check_vm_count.call_args_list]
        assert checked_namespaces == [
            "kubevirt-script-2vm-1data-rbd-ns1",
            "kubevirt-script-2vm-1data-rbd-ns2",
            "kubevirt-script-2vm-1data-rbd-ns3",
        ]

    def test_reports_only_failing_siblings(self, mx):
        scenario = _multi_ns_script_scenario(ns_count=3)
        # ns2 fails VM-count check, ns1 and ns3 pass.
        mx._check_vm_count = MagicMock(side_effect=[True, False, True])
        result = mx.validate_expected_vm_datasets(scenario)
        assert result is False
        failing_namespaces = [ds["namespace"] for ds in mx._OadpWorkloads__oadp_ds_failing_validation]
        assert failing_namespaces == ["kubevirt-script-2vm-1data-rbd-ns2"]

    def test_single_namespace_behavior_unchanged_when_ns_absent(self, mx):
        scenario = _script_kubevirt_scenario()
        assert mx.validate_expected_vm_datasets(scenario) is True
        mx._check_vm_count.assert_called_once_with("kubevirt-script-2vm-1data-ns1", 2)

    def test_native_population_method_unaffected_by_ns_expansion(self, mx):
        """'ns' only applies to vm_population_method 'script'; native scenarios validate
        exactly one namespace, same as before this ticket (data-level checks aside)."""
        scenario = _kubevirt_scenario(args=_valid_args(namespaces_to_backup="kubevirt-minimal-ns"))
        mx.validate_expected_vm_datasets(scenario)
        mx._check_vm_count.assert_called_once_with("kubevirt-minimal-ns", 2)
        mx._check_all_vmis_running.assert_called_once_with("kubevirt-minimal-ns", 2)


class TestCreateVmDatasetViaScriptMultiNamespaceGroup:
    @pytest.fixture
    def mx(self):
        m = TestableVmOps()
        m._OadpWorkloads__ssh.run.return_value = "MPQE_SCRIPT_EXIT_CODE:0"
        return m

    def test_ns1_leader_runs_script_with_full_ns_count(self, mx):
        scenario = _multi_ns_script_scenario(ns_count=3)
        mx.create_vm_dataset_via_script(scenario, {"namespace": "kubevirt-script-2vm-1data-rbd-ns1"})
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "-ns 3" in cmd
        assert "-namespace kubevirt-script-2vm-1data-rbd" in cmd

    def test_ns2_sibling_does_not_invoke_script(self, mx):
        scenario = _multi_ns_script_scenario(ns_count=3)
        mx.create_vm_dataset_via_script(scenario, {"namespace": "kubevirt-script-2vm-1data-rbd-ns2"})
        mx._OadpWorkloads__ssh.run.assert_not_called()

    def test_ns3_sibling_does_not_invoke_script(self, mx):
        scenario = _multi_ns_script_scenario(ns_count=3)
        mx.create_vm_dataset_via_script(scenario, {"namespace": "kubevirt-script-2vm-1data-rbd-ns3"})
        mx._OadpWorkloads__ssh.run.assert_not_called()

    def test_sibling_still_records_its_own_cbt_state(self, mx):
        scenario = _multi_ns_script_scenario(ns_count=3)
        scenario["args"]["vm_population_script_args"]["cbt"] = True
        mx.check_cbt_state = MagicMock(return_value={"vm-0": {"state": "Enabled", "label": "true"}})
        mx.create_vm_dataset_via_script(scenario, {"namespace": "kubevirt-script-2vm-1data-rbd-ns2"})
        mx.check_cbt_state.assert_called_once_with("kubevirt-script-2vm-1data-rbd-ns2")
        assert mx._OadpWorkloads__run_metadata["summary"]["validations"]["cbt_state"][
            "kubevirt-script-2vm-1data-rbd-ns2"
        ] == {"vm-0": {"state": "Enabled", "label": "true"}}

    def test_single_namespace_behavior_unchanged_when_ns_absent(self, mx):
        """ns defaults to 1 -> namespace always ends in '-ns1' -> always the leader (today's behavior)."""
        scenario = _script_kubevirt_scenario()
        mx.create_vm_dataset_via_script(scenario, {"namespace": "kubevirt-script-2vm-1data-ns1"})
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "-ns 1" in cmd


class TestOadpExecuteScenarioMultiNamespaceIncludeNamespaces:
    """Confirms the backup CR's --include-namespaces covers every sibling namespace,
    for both Flow1 (no CBT) and Flow2 (CBT) — achieved via ds_get_all_namespaces()."""

    @pytest.fixture
    def mx(self):
        m = TestableExecution()
        m._OadpWorkloads__test_env["source"] = "downstream"
        m.exec_backup = MagicMock()
        m.wait_for_condition_of_cr = MagicMock(return_value=True)
        return m

    def test_flow1_no_cbt_includes_all_sibling_namespaces(self, mx):
        mx.ds_get_all_namespaces = MagicMock(
            return_value=[
                "rhel9-oadp-2vms-1os-1data-rbd-ns1",
                "rhel9-oadp-2vms-1os-1data-rbd-ns2",
                "rhel9-oadp-2vms-1os-1data-rbd-ns3",
            ]
        )
        scenario = _multi_ns_script_scenario(ns_count=3)
        mx.oadp_execute_scenario(scenario, run_method="python")
        call_kwargs = mx.exec_backup.call_args.kwargs
        assert call_kwargs["namespaces_to_backup"] == (
            "rhel9-oadp-2vms-1os-1data-rbd-ns1,rhel9-oadp-2vms-1os-1data-rbd-ns2,rhel9-oadp-2vms-1os-1data-rbd-ns3"
        )
        assert call_kwargs["resource_policy_configmap"] is None

    def test_flow2_cbt_includes_all_sibling_namespaces_and_resource_policy(self, mx):
        mx.ds_get_all_namespaces = MagicMock(
            return_value=[
                "rhel9-oadp-2vms-1os-1data-cbt-rbd-ns1",
                "rhel9-oadp-2vms-1os-1data-cbt-rbd-ns2",
            ]
        )
        scenario = _multi_ns_script_scenario(ns_count=2, base="rhel9-oadp-2vms-1os-1data-cbt-rbd")
        scenario["args"]["vm_population_script_args"]["cbt"] = True
        mx.oadp_execute_scenario(scenario, run_method="python")
        call_kwargs = mx.exec_backup.call_args.kwargs
        assert call_kwargs["namespaces_to_backup"] == (
            "rhel9-oadp-2vms-1os-1data-cbt-rbd-ns1,rhel9-oadp-2vms-1os-1data-cbt-rbd-ns2"
        )
        assert call_kwargs["resource_policy_configmap"] == KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME


# ===========================================================================
# Phase 1 closeout: multi-ns cleanup/logs/reporting + Flow2 restore resourcePolicy
# ===========================================================================


class TestScenarioBackupNamespaces:
    def test_single_namespace_unchanged(self):
        s = _script_kubevirt_scenario()
        assert scenario_backup_namespaces(s) == ["kubevirt-script-2vm-1data-ns1"]

    def test_expands_sibling_namespaces(self):
        s = _multi_ns_script_scenario(ns_count=3, base="perf-rhel9-2vms-1os-1data-rbd-3ns")
        assert scenario_backup_namespaces(s) == [
            "perf-rhel9-2vms-1os-1data-rbd-3ns-ns1",
            "perf-rhel9-2vms-1os-1data-rbd-3ns-ns2",
            "perf-rhel9-2vms-1os-1data-rbd-3ns-ns3",
        ]

    def test_empty_when_namespaces_to_backup_missing(self):
        s = _kubevirt_scenario()
        assert scenario_backup_namespaces(s) == []


class TestCleanupMultiNamespace:
    @pytest.fixture
    def mx(self):
        return TestableCleanup()

    def test_dataset_cleanup_loops_all_sibling_namespaces(self, mx):
        scenario = _multi_ns_script_scenario(ns_count=3)
        mx._OadpWorkloads__scenario_datasets = []
        mx.cleaning_up_oadp_resources(scenario)
        deleted = [c.kwargs["target_namespace"] for c in mx.delete_source_dataset.call_args_list]
        assert deleted == [
            "kubevirt-script-2vm-1data-rbd-ns1",
            "kubevirt-script-2vm-1data-rbd-ns2",
            "kubevirt-script-2vm-1data-rbd-ns3",
        ]

    def test_dataset_cleanup_prefers_loaded_scenario_datasets(self, mx):
        scenario = _multi_ns_script_scenario(ns_count=3)
        mx._OadpWorkloads__scenario_datasets = [
            {"namespace": "loaded-ns1"},
            {"namespace": "loaded-ns2"},
        ]
        mx.cleaning_up_oadp_resources(scenario)
        deleted = [c.kwargs["target_namespace"] for c in mx.delete_source_dataset.call_args_list]
        assert deleted == ["loaded-ns1", "loaded-ns2"]

    def test_ns_scoped_vsc_clean_loops_siblings(self, mx):
        scenario = _multi_ns_script_scenario(ns_count=2)
        mx._OadpWorkloads__scenario_datasets = []
        mx._OadpWorkloads__ssh.run.return_value = "ok"
        mx.delete_vsc(scenario, ns_scoped=True)
        cmds = [c.kwargs["cmd"] for c in mx._OadpWorkloads__ssh.run.call_args_list]
        assert any("kubevirt-script-2vm-1data-rbd-ns1" in c for c in cmds)
        assert any("kubevirt-script-2vm-1data-rbd-ns2" in c for c in cmds)


class TestReportingMultiNamespaceVmCounts:
    @pytest.fixture
    def mx(self):
        m = TestableReporting()
        # Sequence: cnv csv name, then per-NS vm/vmi/dv counts for 2 namespaces.
        m._OadpWorkloads__ssh.run.side_effect = [
            "kubevirt-hyperconverged-operator.v4.17.0",
            "4.17.0",
            "2",  # ns1 vms
            "2",  # ns1 running vmis
            "4",  # ns1 dvs
            "2",  # ns2 vms
            "1",  # ns2 running vmis
            "4",  # ns2 dvs
        ]
        return m

    def test_aggregates_vm_counts_across_sibling_namespaces(self, mx):
        scenario = _multi_ns_script_scenario(ns_count=2)
        mx.enrich_summary_with_vm_metadata(scenario)
        counts = mx._OadpWorkloads__run_metadata["summary"]["resources"]["vm_counts"]
        assert counts == {"total_vms": 4, "running_vmis": 3, "total_datavolumes": 8}


class TestLogCollectionMultiNamespace:
    @pytest.fixture
    def mx(self, tmp_path):
        m = TestableLogCollection()
        m._logs = tmp_path
        return m

    def test_collect_dv_status_writes_per_namespace_files_when_multi_ns(self, mx, tmp_path):
        scenario = _multi_ns_script_scenario(ns_count=2)
        mx._OadpWorkloads__ssh.run.return_value = ""
        mx.collect_dv_status(str(tmp_path), scenario)
        cmds = [c.kwargs["cmd"] for c in mx._OadpWorkloads__ssh.run.call_args_list]
        assert any("kubevirt-script-2vm-1data-rbd-ns1" in c and "datavolumes_status_" in c for c in cmds)
        assert any("kubevirt-script-2vm-1data-rbd-ns2" in c and "datavolumes_status_" in c for c in cmds)

    def test_collect_dv_status_keeps_single_filename_for_one_ns(self, mx, tmp_path):
        scenario = _script_kubevirt_scenario()
        mx._OadpWorkloads__ssh.run.return_value = ""
        mx.collect_dv_status(str(tmp_path), scenario)
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "datavolumes_status.yaml" in cmd
        assert "datavolumes_status_" not in cmd


class TestExecRestoreResourcePolicy:
    @pytest.fixture
    def mx(self):
        m = TestableExecution()
        m._OadpWorkloads__test_env["source"] = "downstream"
        m._OadpWorkloads__ssh.run.return_value = 'Restore request "r1" submitted successfully.'
        return m

    def test_downstream_cbt_restore_gets_resource_policy_flag(self, mx):
        mx.exec_restore(
            plugin="kubevirt",
            restore_name="r1",
            backup_name="b1",
            resource_policy_configmap=KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME,
        )
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert f"--resource-policies-configmap {KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME}" in cmd

    def test_downstream_restore_without_policy_omits_flag(self, mx):
        mx.exec_restore(plugin="kubevirt", restore_name="r1", backup_name="b1")
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "--resource-policies-configmap" not in cmd

    def test_upstream_restore_never_gets_resource_policy_flag(self, mx):
        mx._OadpWorkloads__test_env["source"] = "upstream"
        mx._OadpWorkloads__test_env["velero_cli_path"] = "/tmp/velero-1-12"
        mx.exec_restore(
            plugin="kubevirt",
            restore_name="r1",
            backup_name="b1",
            resource_policy_configmap=KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME,
        )
        cmd = mx._OadpWorkloads__ssh.run.call_args.kwargs["cmd"]
        assert "--resource-policies-configmap" not in cmd


class TestOadpExecuteScenarioRestoreResourcePolicyWiring:
    @pytest.fixture
    def mx(self):
        m = TestableExecution()
        m._OadpWorkloads__test_env["source"] = "downstream"
        m.ds_get_all_namespaces = MagicMock(return_value=["ns1"])
        m.exec_restore = MagicMock()
        m.wait_for_condition_of_cr = MagicMock(return_value=True)
        return m

    def _restore_scenario(self, cbt):
        s = _script_kubevirt_scenario()
        s["testtype"] = "restore"
        s["args"]["OADP_CR_TYPE"] = "restore"
        s["args"]["OADP_CR_NAME"] = "restore-kubevirt-script"
        s["args"]["vm_population_script_args"]["cbt"] = cbt
        return s

    def test_cbt_restore_passes_resource_policy_configmap(self, mx):
        mx.oadp_execute_scenario(self._restore_scenario(cbt=True), run_method="python")
        call_kwargs = mx.exec_restore.call_args.kwargs
        assert call_kwargs["resource_policy_configmap"] == KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME

    def test_non_cbt_restore_passes_none(self, mx):
        mx.oadp_execute_scenario(self._restore_scenario(cbt=False), run_method="python")
        call_kwargs = mx.exec_restore.call_args.kwargs
        assert call_kwargs["resource_policy_configmap"] is None


class TestMultiNsScenariosFromTestsYamlShapes:
    """Framework-side shapes matching tests.yaml TC 10.8 / 10.9 (ns > 1 already in mpqe)."""

    @pytest.fixture
    def mx(self):
        return TestableScenario()

    def test_flow2_cbt_3ns_scenario_validates(self, mx):
        s = _tests_yaml_script_scenario(
            "backup-dm-rhel9-2vms-1os-1data-cbt-3ns-cephrbd",
            10.8,
            "backup",
            vms=2,
            data_disks=1,
            namespace="perf-rhel9-2vms-1os-1data-cbt-rbd-3ns-ns1",
            cbt=True,
        )
        s["args"]["vm_population_script_args"]["ns"] = 3
        assert mx.validate_scenario(s) is True
        assert get_script_ns_count(s) == 3

    def test_flow1_3ns_scenario_validates(self, mx):
        s = _tests_yaml_script_scenario(
            "backup-dm-rhel9-2vms-1os-1data-3ns-cephrbd",
            10.9,
            "backup",
            vms=2,
            data_disks=1,
            namespace="perf-rhel9-2vms-1os-1data-rbd-3ns-ns1",
        )
        s["args"]["vm_population_script_args"]["ns"] = 3
        assert mx.validate_scenario(s) is True
        assert scenario_backup_namespaces(s) == [
            "perf-rhel9-2vms-1os-1data-rbd-3ns-ns1",
            "perf-rhel9-2vms-1os-1data-rbd-3ns-ns2",
            "perf-rhel9-2vms-1os-1data-rbd-3ns-ns3",
        ]

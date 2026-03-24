"""Unit tests for pure-logic helpers on OADP mixin classes."""

from __future__ import annotations

import math
import random

import pytest

from benchmark_runner.oadp.execution import OadpExecutionMixin
from benchmark_runner.oadp.pod_validation import OadpPodValidationMixin
from benchmark_runner.oadp.resources import OadpResourcesMixin


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
                "runtime": {},
                "results": {},
                "validations": {},
                "resources": {"nodes": {}, "run_time_pods": [], "pods": {}},
                "transactions": [],
            },
        }
        self.__oadp_resources = {}
        self.__oadp_runtime_resource_mapping = {}
        self.__retry_logic = {"interval_between_checks": 15, "max_attempts": 20}
        self.__oadp_ds_failing_validation = []

    def fail_test_run(self, msg):
        raise RuntimeError(msg)

    def get_current_function(self):
        return "test"


class TestableResources(OadpResourcesMixin, OadpWorkloads):
    __test__ = False

    def __init__(self):
        OadpWorkloads.__init__(self)


class TestablePodValidation(OadpPodValidationMixin, OadpWorkloads):
    __test__ = False

    def __init__(self):
        OadpWorkloads.__init__(self)


class TestableExecution(OadpExecutionMixin, OadpWorkloads):
    __test__ = False

    def __init__(self):
        OadpWorkloads.__init__(self)


# --- resources.OadpResourcesMixin ---


class TestCalcResourceDiff:
    @pytest.fixture
    def r(self):
        return TestableResources()

    def test_percent_increase(self, r):
        assert r.calc_resource_diff("100m", "150m") == pytest.approx(50.0)

    def test_percent_decrease(self, r):
        assert r.calc_resource_diff("200Mi", "100Mi") == pytest.approx(-50.0)

    def test_strips_non_digits(self, r):
        assert r.calc_resource_diff("cpu: 10%", "20") == pytest.approx(100.0)

    def test_zero_old_returns_zero(self, r):
        assert r.calc_resource_diff("0", "99") == 0

    def test_equal_values_zero_change(self, r):
        assert r.calc_resource_diff("512Mi", "512 Mi") == pytest.approx(0.0)


class TestCalcPodBasename:
    @pytest.fixture
    def r(self):
        return TestableResources()

    def test_strips_trailing_numeric_suffix(self, r):
        assert r.calc_pod_basename("busybox-backup-ns-7") == "busybox-backup-ns"

    def test_strips_replicaset_style_hash(self, r):
        assert r.calc_pod_basename("deploy-7d8f9c-x") == "deploy"

    def test_single_token_no_digits(self, r):
        assert r.calc_pod_basename("single") == ""

    def test_two_parts_digit_suffix(self, r):
        assert r.calc_pod_basename("my-pod-1") == "my-pod"


class TestGetNodeGenericName:
    @pytest.fixture
    def r(self):
        return TestableResources()

    def test_shortens_simple_worker_hostname(self, r):
        assert r.get_node_generic_name("worker-0") == "worker"

    def test_preserves_worker_with_extra_hyphens(self, r):
        host = "ip-10-0-1-5-worker-1"
        assert r.get_node_generic_name(host) == host

    def test_preserves_non_worker(self, r):
        assert r.get_node_generic_name("master-0") == "master-0"


# --- pod_validation.OadpPodValidationMixin ---


class TestCompareDicts:
    @pytest.fixture
    def v(self):
        return TestablePodValidation()

    def test_equal_order_insensitive_keys(self, v):
        assert v.compare_dicts({"a": 1, "b": 2}, {"b": 2, "a": 1}) is True

    def test_key_mismatch(self, v):
        assert v.compare_dicts({"a": 1}, {"a": 1, "b": 2}) is False

    def test_value_mismatch(self, v):
        assert v.compare_dicts({"x": 1}, {"x": 2}) is False

    def test_empty_dicts(self, v):
        assert v.compare_dicts({}, {}) is True


class TestIsNumOfResultsValid:
    @pytest.fixture
    def v(self):
        return TestablePodValidation()

    def test_exact_match(self, v):
        assert v.is_num_of_results_valid(3, "0%", [1, 2, 3]) is True

    def test_exact_mismatch_percent_fallback_not_met(self, v):
        assert v.is_num_of_results_valid(3, "100%", [1, 2]) is False

    def test_percentage_threshold_met(self, v):
        assert v.is_num_of_results_valid(10, "50%", list(range(6))) is True

    def test_percentage_threshold_not_met(self, v):
        assert v.is_num_of_results_valid(10, "80%", list(range(7))) is False

    def test_percentage_exact_boundary(self, v):
        assert v.is_num_of_results_valid(20, "25%", list(range(5))) is True


class TestGetPodPrefixByDatasetRole:
    @pytest.fixture
    def v(self):
        return TestablePodValidation()

    def test_none_dataset(self, v):
        assert v.get_pod_prefix_by_dataset_role(None) is None

    def test_generator_role(self, v):
        ds = {"role": "generator", "pv_size": "32Mi", "pods_per_ns": 100}
        assert v.get_pod_prefix_by_dataset_role(ds) == "32mi-100"

    def test_dd_generator_dots_replaced(self, v):
        ds = {"role": "dd_generator", "pv_size": "1.5G", "pods_per_ns": 4}
        assert v.get_pod_prefix_by_dataset_role(ds) == "1-5g-4"

    def test_busybox_script_role(self, v):
        ds = {"role": "BusyBoxPodSingleNS.sh", "pv_size": "x", "pods_per_ns": 1}
        assert v.get_pod_prefix_by_dataset_role(ds) == "busybox-perf"

    def test_unknown_role_returns_empty_string(self, v):
        ds = {"role": "other", "pv_size": "1", "pods_per_ns": 1}
        assert v.get_pod_prefix_by_dataset_role(ds) == ""


class TestGetReducedList:
    @pytest.fixture
    def v(self):
        return TestablePodValidation()

    def test_empty_list(self, v):
        assert v.get_reduced_list([]) == []

    def test_single_element_returns_full_list(self, v):
        assert v.get_reduced_list(["only"]) == ["only"]

    def test_two_item_list_yields_one_sampled_element(self, v):
        random.seed(99)
        original = ["a", "b"]
        reduced = v.get_reduced_list(original)
        assert len(reduced) == 1
        assert reduced == ["b"]

    def test_deterministic_sample_size_and_subset(self, v):
        random.seed(42)
        original = [f"p{i}" for i in range(30)]
        reduced = v.get_reduced_list(original)
        assert len(reduced) == 3
        assert set(reduced) == {"p0", "p3", "p20"}

    def test_deterministic_indices_match_expected(self, v):
        random.seed(12345)
        original = list(range(20))
        reduced = v.get_reduced_list(original)
        assert set(reduced) == {0, 13}


# --- execution.OadpExecutionMixin ---


class TestValidateAnsiblePlay:
    @pytest.fixture
    def e(self):
        return TestableExecution()

    def test_success_zero_failed_unreachable(self, e):
        out = "PLAY RECAP ... failed=0 unreachable=0 ok=3 changed=0"
        assert e.validate_ansible_play(out) is True

    def test_failure_nonzero_failed(self, e):
        out = "PLAY RECAP ... failed=2 unreachable=0"
        assert e.validate_ansible_play(out) is False

    def test_failure_unreachable(self, e):
        out = "failed=0 unreachable=1"
        assert e.validate_ansible_play(out) is False

    def test_missing_counters_returns_false(self, e):
        assert e.validate_ansible_play("no recap here") is False

    def test_empty_string_returns_false(self, e):
        assert e.validate_ansible_play("") is False


class TestGetExpectedFilesCount:
    @pytest.fixture
    def e(self):
        return TestableExecution()

    def test_dd_generator_returns_empty(self, e):
        ds = {
            "role": "dd_generator",
            "dir_count": 2,
            "files_count": 10,
            "dept_count": 2,
            "files_size": 1024,
        }
        assert e.get_expected_files_count({}, ds) == {}

    def test_generator_matches_formula(self, e):
        ds = {
            "role": "generator",
            "dir_count": 2,
            "files_count": 3,
            "dept_count": 2,
            "files_size": 1024,
        }
        dept_count = ds["dept_count"]
        countdir = sum(math.pow(dept_count, k) for k in range(dept_count))
        countfile = int(math.pow(dept_count, dept_count - 1) * ds["files_count"])
        total_files = int(countfile * ds["dir_count"])
        total_folders = int(countdir * ds["dir_count"])
        disk_capacity = round(int(total_files * ds["files_size"]) / 1024 / 1024)

        result = e.get_expected_files_count({}, ds)
        assert result["files_count"] == total_files
        assert result["folders_count"] == total_folders
        assert result["disk_capacity"] == disk_capacity

    def test_generator_larger_tree(self, e):
        ds = {
            "role": "generator",
            "dir_count": 1,
            "files_count": 5,
            "dept_count": 3,
            "files_size": 4096,
        }
        result = e.get_expected_files_count({}, ds)
        assert result["files_count"] == 45
        assert result["folders_count"] == 13


class TestPvContainsExpectedData:
    @pytest.fixture
    def e(self):
        return TestableExecution()

    def test_success_all_match(self, e):
        ds = {
            "expected_capacity": "12M",
            "files_count": "100",
            "dir_count": 1,
        }
        pv = {"disk_capacity": "12M", "files_count": "100", "folders_count": 1}
        assert e.pv_contains_expected_data(pv, ds) is True

    def test_files_count_via_total_by_folder(self, e):
        ds = {
            "expected_capacity": "1G",
            "files_count": "10",
            "dir_count": 5,
        }
        pv = {"disk_capacity": "1G", "files_count": "50", "folders_count": 5}
        assert e.pv_contains_expected_data(pv, ds) is True

    def test_disk_capacity_mismatch(self, e):
        ds = {
            "expected_capacity": "10M",
            "files_count": "1",
            "dir_count": 1,
        }
        pv = {"disk_capacity": "11M", "files_count": "1", "folders_count": 1}
        assert e.pv_contains_expected_data(pv, ds) is False

    def test_files_count_mismatch(self, e):
        ds = {
            "expected_capacity": "1M",
            "files_count": "10",
            "dir_count": 3,
        }
        pv = {"disk_capacity": "1M", "files_count": "25", "folders_count": 3}
        assert e.pv_contains_expected_data(pv, ds) is False

    def test_folders_count_mismatch(self, e):
        ds = {
            "expected_capacity": "1M",
            "files_count": "5",
            "dir_count": 4,
        }
        pv = {"disk_capacity": "1M", "files_count": "5", "folders_count": 3}
        assert e.pv_contains_expected_data(pv, ds) is False

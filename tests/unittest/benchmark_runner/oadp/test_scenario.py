"""Unit tests for pure-logic helpers on OadpScenarioMixin (benchmark_runner.oadp.scenario)."""

from __future__ import annotations

import pytest

from benchmark_runner.oadp.scenario import OadpScenarioMixin


def _valid_args(**overrides):
    base = {
        "plugin": "kopia",
        "use_cli": False,
        "OADP_CR_TYPE": "backup",
        "OADP_CR_NAME": "cr1",
        "backup_name": "bk1",
        "testcase_timeout": 600,
    }
    base.update(overrides)
    return base


def _minimal_scenario(**overrides):
    s = {
        "name": "scen1",
        "testcase": "single-namespace-backup",
        "result_dir_base_path": "/tmp/results",
        "testtype": "backup",
        "dataset": {
            "role": "BusyBoxPodSingleNS.sh",
            "pods_per_ns": 10,
            "sc": "ocs-storagecluster-ceph-rbd",
        },
        "args": _valid_args(),
    }
    s.update(overrides)
    return s


class OadpWorkloads:
    """Minimal stub; `__*` attrs store as `_OadpWorkloads__*` for mixin access."""

    def __init__(self):
        self.__test_env = {"source": "upstream", "velero_ns": "openshift-adp"}
        self.__run_metadata = {
            "index": "",
            "metadata": {},
            "summary": {"runtime": {"datasetsize": {}, "dataset": {}}},
        }
        self.__scenario_datasets = []

    def fail_test_run(self, msg: str) -> None:
        raise RuntimeError(msg)

    def get_current_function(self) -> str:
        return "test"


class TestableScenario(OadpScenarioMixin, OadpWorkloads):
    """Concrete type to exercise mixin methods."""

    __test__ = False


@pytest.fixture
def mx():
    return TestableScenario()


class TestValidateScenario:
    @pytest.mark.parametrize(
        "missing_key",
        ["name", "testcase", "dataset", "result_dir_base_path", "testtype", "args"],
    )
    def test_missing_top_level_key_fails(self, mx, missing_key):
        s = _minimal_scenario()
        del s[missing_key]
        assert mx.validate_scenario(s) is False

    def test_valid_dict_dataset(self, mx):
        assert mx.validate_scenario(_minimal_scenario()) is True

    @pytest.mark.parametrize(
        "missing_ds_key",
        ["role", "pods_per_ns", "sc"],
    )
    def test_dict_dataset_missing_field_fails(self, mx, missing_ds_key):
        s = _minimal_scenario()
        del s["dataset"][missing_ds_key]
        assert mx.validate_scenario(s) is False

    def test_valid_list_dataset(self, mx):
        s = _minimal_scenario(
            dataset=[
                {
                    "role": "r1",
                    "pods_per_ns": 3,
                    "sc": "sc-a",
                    "namespace": "ns-a",
                },
                {
                    "role": "r2",
                    "pods_per_ns": 2,
                    "sc": "sc-b",
                    "namespace": "ns-b",
                },
            ],
        )
        assert mx.validate_scenario(s) is True

    def test_list_dataset_missing_namespace_fails(self, mx):
        s = _minimal_scenario(
            dataset=[
                {"role": "r1", "pods_per_ns": 1, "sc": "sc-a"},
            ],
        )
        assert mx.validate_scenario(s) is False

    def test_list_dataset_missing_role_fails(self, mx):
        s = _minimal_scenario(
            dataset=[
                {"pods_per_ns": 1, "sc": "sc-a", "namespace": "ns-a"},
            ],
        )
        assert mx.validate_scenario(s) is False

    def test_dataset_neither_list_nor_dict_fails(self, mx):
        s = _minimal_scenario(dataset="not-a-dataset")
        assert mx.validate_scenario(s) is False

    @pytest.mark.parametrize(
        "missing_arg",
        [
            "plugin",
            "use_cli",
            "OADP_CR_TYPE",
            "OADP_CR_NAME",
            "backup_name",
            "testcase_timeout",
        ],
    )
    def test_args_missing_key_fails(self, mx, missing_arg):
        s = _minimal_scenario()
        del s["args"][missing_arg]
        assert mx.validate_scenario(s) is False


class TestProcessDataset:
    def test_merges_namespace_and_flags(self, mx):
        ds = {"role": "r", "pods_per_ns": 5, "sc": "fast"}
        out = mx._process_dataset(ds, "my-ns")
        assert out == {
            **ds,
            "namespace": "my-ns",
            "exists": False,
            "validated": False,
        }

    def test_preserves_extra_keys(self, mx):
        ds = {"role": "r", "pods_per_ns": 1, "sc": "s", "expected_capacity": "1G"}
        out = mx._process_dataset(ds, "n")
        assert out["expected_capacity"] == "1G"
        assert out["namespace"] == "n"


class TestParseCapacity:
    @pytest.mark.parametrize(
        "text, value, unit",
        [
            ("1G", 1.0, "G"),
            ("512M", 512.0, "M"),
            ("0.5K", 0.5, "K"),
            ("2T", 2.0, "T"),
            ("10.25G", 10.25, "G"),
        ],
    )
    def test_parses_standard_forms(self, text, value, unit):
        assert OadpScenarioMixin._parse_capacity(text) == (value, unit)

    @pytest.mark.parametrize(
        "bad",
        [
            "",
            "1",
            "1X",
            "Gi",
            " 1G",
            "g",
        ],
    )
    def test_invalid_raises(self, bad):
        with pytest.raises(ValueError, match="Invalid expected_capacity"):
            OadpScenarioMixin._parse_capacity(bad)


class TestConvertToMb:
    @pytest.mark.parametrize(
        "value, unit, expected",
        [
            (1024.0, "K", 1.0),
            (1.0, "M", 1.0),
            (1.0, "G", 1024.0),
            (1.0, "T", 1024.0 * 1024.0),
            (3.0, "G", 3072.0),
        ],
    )
    def test_conversions(self, value, unit, expected):
        assert OadpScenarioMixin._convert_to_mb(value, unit) == pytest.approx(expected)

    def test_unknown_unit_returns_value(self):
        assert OadpScenarioMixin._convert_to_mb(99.0, "Z") == 99.0


class TestCapacityInBytes:
    @pytest.mark.parametrize(
        "qty, expected",
        [
            ("1Ki", 1024),
            ("1Mi", 1024**2),
            ("1Gi", 1024**3),
            ("2Gi", 2 * 1024**3),
            ("1Ti", 1024**4),
            ("512Mi", 512 * 1024**2),
        ],
    )
    def test_kubernetes_quantities(self, mx, qty, expected):
        assert mx.capacity_in_bytes(qty) == expected

    @pytest.mark.parametrize(
        "bad",
        [
            "10G",
            "1ii",
            "12ab",
            "100",
        ],
    )
    def test_invalid_unit_raises(self, mx, bad):
        with pytest.raises(ValueError, match="Invalid storage unit"):
            mx.capacity_in_bytes(bad)


class TestGetUniqueNamespaces:
    def test_list_collects_distinct(self, mx):
        s = _minimal_scenario(
            dataset=[
                {"role": "a", "pods_per_ns": 1, "sc": "s", "namespace": "ns1"},
                {"role": "b", "pods_per_ns": 2, "sc": "s", "namespace": "ns2"},
                {"role": "c", "pods_per_ns": 3, "sc": "s", "namespace": "ns1"},
            ],
        )
        out = sorted(mx.get_unique_namespaces(s))
        assert out == ["ns1", "ns2"]

    def test_list_skips_empty_namespace(self, mx):
        s = _minimal_scenario(
            dataset=[
                {"role": "a", "pods_per_ns": 1, "sc": "s", "namespace": ""},
                {"role": "b", "pods_per_ns": 1, "sc": "s", "namespace": "ns-x"},
            ],
        )
        assert mx.get_unique_namespaces(s) == ["ns-x"]

    def test_dict_with_namespace(self, mx):
        s = _minimal_scenario(
            dataset={
                "role": "r",
                "pods_per_ns": 1,
                "sc": "s",
                "namespace": "only-ns",
            },
        )
        assert mx.get_unique_namespaces(s) == ["only-ns"]

    def test_dict_without_namespace_returns_empty(self, mx):
        s = _minimal_scenario()
        assert mx.get_unique_namespaces(s) == []

    def test_no_dataset_key(self, mx):
        s = _minimal_scenario()
        del s["dataset"]
        assert mx.get_unique_namespaces(s) == []


class TestAreMultipleDatasetsPresent:
    def test_list_single_false(self, mx):
        s = _minimal_scenario(
            dataset=[
                {"role": "a", "pods_per_ns": 1, "sc": "s", "namespace": "n"},
            ],
        )
        assert mx.are_multiple_datasets_present(s) is False

    def test_list_multi_true(self, mx):
        s = _minimal_scenario(
            dataset=[
                {"role": "a", "pods_per_ns": 1, "sc": "s", "namespace": "n1"},
                {"role": "b", "pods_per_ns": 1, "sc": "s", "namespace": "n2"},
            ],
        )
        assert mx.are_multiple_datasets_present(s) is True

    def test_dict_scalar_values_false(self, mx):
        assert mx.are_multiple_datasets_present(_minimal_scenario()) is False

    def test_dict_with_list_value_true(self, mx):
        s = _minimal_scenario(
            dataset={
                "role": "r",
                "pods_per_ns": 1,
                "sc": "s",
                "tags": ["a", "b"],
            },
        )
        assert mx.are_multiple_datasets_present(s) is True

    def test_missing_dataset(self, mx):
        s = _minimal_scenario()
        del s["dataset"]
        assert mx.are_multiple_datasets_present(s) is False


class TestCalcTotalPodsPerNamespaceInDatasets:
    def test_list_sums_matching_namespace(self, mx):
        s = _minimal_scenario(
            dataset=[
                {"role": "a", "pods_per_ns": 3, "sc": "s", "namespace": "n1"},
                {"role": "b", "pods_per_ns": 7, "sc": "s", "namespace": "n1"},
                {"role": "c", "pods_per_ns": 100, "sc": "s", "namespace": "n2"},
            ],
        )
        assert mx.calc_total_pods_per_namespace_in_datasets(s, "n1") == 10
        assert mx.calc_total_pods_per_namespace_in_datasets(s, "n2") == 100

    def test_list_missing_pods_treated_as_zero(self, mx):
        s = _minimal_scenario(
            dataset=[
                {"role": "a", "sc": "s", "namespace": "n1"},
            ],
        )
        assert mx.calc_total_pods_per_namespace_in_datasets(s, "n1") == 0

    def test_dict_match(self, mx):
        s = _minimal_scenario(
            dataset={
                "role": "r",
                "pods_per_ns": 42,
                "sc": "s",
                "namespace": "target-ns",
            },
        )
        assert mx.calc_total_pods_per_namespace_in_datasets(s, "target-ns") == 42

    def test_dict_namespace_mismatch_zero(self, mx):
        s = _minimal_scenario(
            dataset={
                "role": "r",
                "pods_per_ns": 99,
                "sc": "s",
                "namespace": "a",
            },
        )
        assert mx.calc_total_pods_per_namespace_in_datasets(s, "b") == 0

    def test_non_list_non_dict_dataset(self, mx):
        s = _minimal_scenario(dataset=None)
        assert mx.calc_total_pods_per_namespace_in_datasets(s, "any") == 0

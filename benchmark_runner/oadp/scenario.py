"""
Mixin for OADP test scenario loading, validation, and dataset management helpers.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import yaml

from benchmark_runner.common.logger.logger_time_stamp import logger, logger_time_stamp
from benchmark_runner.oadp.constants import (
    ES_INDEX_PREFIX_DOWNSTREAM,
    ES_INDEX_PREFIX_UPSTREAM,
    KNOWN_PLUGINS,
    SHELL_METACHARACTERS,
    SOURCE_UPSTREAM,
    VM_DATASET_ROLE,
)
from benchmark_runner.oadp.oadp_exceptions import OadpError


def scenario_includes_kubevirt_dataset(scenario: dict) -> bool:
    """Return True if the scenario dataset (list or dict) includes role kubevirt."""
    dataset_value = scenario.get("dataset")
    if isinstance(dataset_value, list):
        return any(d.get("role") == VM_DATASET_ROLE for d in dataset_value)
    if isinstance(dataset_value, dict):
        return dataset_value.get("role") == VM_DATASET_ROLE
    return False


class OadpScenarioMixin:
    """Scenario YAML loading, validation, dataset helpers, index generation."""

    @logger_time_stamp
    def load_test_scenario(self) -> dict:
        """Load and return the active scenario dict from YAML by configured scenario name."""
        try:
            scenario_data = self._OadpWorkloads__oadp_scenario_data
            scenario_name = self._OadpWorkloads__oadp_scenario_name
            if os.path.exists(scenario_data) and os.stat(scenario_data).st_size != 0:
                test_data = yaml.safe_load(Path(scenario_data).read_text())
            for case in test_data["scenarios"]:
                if case["name"] == scenario_name:
                    logger.info(
                        f"### INFO ### load_test_scenario has loaded details of scenario: {scenario_name} from the yaml"
                    )
                    if case is None:
                        logger.exception("Test Scenario is undefined or not found ")
                        raise Exception("Test Scenario is undefined or not found ")
                    else:
                        if self.validate_scenario(case):
                            logger.info(f"#### INFO #### Scenario '{case['name']}' is valid.")
                        else:
                            logger.error(f"### INFO ### Scenario '{case['name']}' is invalid.")
                            logger.exception("Scenario described in yaml is not valid or missing expected details")
                        return case
            else:
                logger.error("Yaml for test scenarios is not found or empty!!")
                logger.error("Test Scenario index is not found")
                logger.exception(f"Test Scenario {scenario_name} index is not found")
                self.fail_test_run(
                    f"Test Scenario {scenario_name} index is not found occurred in " + self.get_current_function()
                )
        except OadpError as err:
            raise OadpError(err)
        except Exception as err:
            raise err

    def validate_scenario(self, scenario: dict) -> bool:
        """Return True if scenario contains required keys and well-formed dataset and args."""
        required_keys = ["name", "testcase", "dataset", "result_dir_base_path", "testtype", "args"]
        dataset_keys = ["role", "pods_per_ns", "sc"]
        args_keys = ["plugin", "use_cli", "OADP_CR_TYPE", "OADP_CR_NAME", "backup_name", "testcase_timeout"]

        for key in required_keys:
            if key not in scenario:
                logger.error(f"Error: Missing key '{key}' in the scenario.")
                return False

        if "dataset" in scenario:
            dataset = scenario["dataset"]
            if isinstance(dataset, list):
                check_keys = dataset_keys + ["namespace"]
                for data in dataset:
                    effective_keys = self._effective_dataset_keys(data, check_keys)
                    if not all(key in data for key in effective_keys):
                        logger.error("Error: Missing dataset key(s) in the scenario.")
                        return False
                    if not self._validate_no_shell_metacharacters(data, "dataset"):
                        return False
                    if not self._validate_dataset_entry(data):
                        return False
                if "args" in scenario and isinstance(dataset, dict) and "namespace_to_backup" not in scenario["args"]:
                    logger.error("Error: 'args' key must contain 'namespace_to_backup' when dataset is a list.")
                    return False
            elif isinstance(dataset, dict):
                effective_keys = self._effective_dataset_keys(dataset, dataset_keys)
                if not all(key in dataset for key in effective_keys):
                    logger.error("Error: Missing dataset key(s) in the scenario.")
                    return False
                if not self._validate_no_shell_metacharacters(dataset, "dataset"):
                    return False
                if not self._validate_dataset_entry(dataset):
                    return False
            else:
                logger.error("Error: 'dataset' must be either a list or a dictionary.")
                return False

        if "args" in scenario:
            args = scenario["args"]
            if not all(key in args for key in args_keys):
                logger.error("Error: Missing 'args' key(s) in the scenario.")
                return False

            plugin = args.get("plugin", "")
            if plugin and plugin not in KNOWN_PLUGINS:
                logger.error(f"Error: Unknown plugin '{plugin}'. Allowed: {sorted(KNOWN_PLUGINS)}")
                return False

            if not self._validate_no_shell_metacharacters(args, "args"):
                return False

        if scenario_includes_kubevirt_dataset(scenario):
            kubevirt_plugin = scenario.get("args", {}).get("kubevirt_plugin", False)
            if not kubevirt_plugin:
                logger.error(
                    "Error: Scenario with dataset role 'kubevirt' requires 'kubevirt_plugin: true' in args. "
                    "Without the kubevirt Velero plugin, VM backups will be incomplete."
                )
                return False

        if not self._validate_no_shell_metacharacters(scenario, "scenario"):
            return False

        return True

    @staticmethod
    def _effective_dataset_keys(dataset: dict, base_keys: list[str]) -> list[str]:
        """For VM datasets, accept ``vms_per_namespace`` as alternative to ``pods_per_ns``."""
        if dataset.get("role") == VM_DATASET_ROLE:
            return [k for k in base_keys if k != "pods_per_ns"]
        return base_keys

    @staticmethod
    def _validate_dataset_entry(dataset: dict) -> bool:
        """Validate VM-specific fields when role is 'kubevirt'."""
        if dataset.get("role") == VM_DATASET_ROLE:
            if "vm_profile" not in dataset:
                logger.error("Error: VM dataset (role=kubevirt) requires 'vm_profile' field.")
                return False
            if "vms_per_namespace" not in dataset and "pods_per_ns" not in dataset:
                logger.error("Error: VM dataset requires 'vms_per_namespace' or 'pods_per_ns'.")
                return False

            disk_overrides = dataset.get("disk_overrides", {})
            if isinstance(disk_overrides, dict):
                for disk_name, fields in disk_overrides.items():
                    if not isinstance(fields, dict):
                        continue
                    all_values = {disk_name: disk_name}
                    all_values.update({k: v for k, v in fields.items() if isinstance(v, str)})
                    for key, value in all_values.items():
                        if any(ch in value for ch in SHELL_METACHARACTERS) or "'" in value:
                            logger.error(f"Error: disk_overrides field '{key}' contains forbidden characters: {value!r}")
                            return False
        return True

    @staticmethod
    def _validate_no_shell_metacharacters(mapping: dict, context: str) -> bool:
        """Reject string values containing shell metacharacters or single quotes (SEC-002)."""
        for key, value in mapping.items():
            if isinstance(value, str) and (any(ch in value for ch in SHELL_METACHARACTERS) or "'" in value):
                logger.error(f"Error: {context} field '{key}' contains forbidden shell metacharacters: {value!r}")
                return False
        return True

    @logger_time_stamp
    def generate_elastic_index(self, scenario: dict) -> str:
        """Build and store the ElasticSearch index name from scenario test type and stream prefix."""
        try:
            if self._OadpWorkloads__test_env["source"] == SOURCE_UPSTREAM:
                index_prefix = ES_INDEX_PREFIX_UPSTREAM
            else:
                index_prefix = ES_INDEX_PREFIX_DOWNSTREAM

            index_name = f"{index_prefix}" + scenario["testtype"] + "-" + "single-namespace"
            logger.info(f":: INFO :: ELK index name is: {index_name}")
            self._OadpWorkloads__run_metadata["index"] = index_name
            return index_name
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def load_datasets_for_scenario(self, scenario: dict) -> None:
        """Populate scenario_datasets from scenario dataset list or dict and print summaries."""
        try:
            all_datasets = []

            if isinstance(scenario["dataset"], list):
                for dataset in scenario["dataset"]:
                    namespace = dataset.get("namespace", "")
                    existing_namespace_index = next(
                        (i for i, s in enumerate(all_datasets) if s["namespace"] == namespace),
                        None,
                    )
                    if existing_namespace_index is not None:
                        existing_entry = all_datasets[existing_namespace_index]
                        existing_entry["total_datasets_for_this_namespace"] += 1
                        existing_entry["total_pods_per_all_datasets_with_same_namespace"] += dataset.get(
                            "pods_per_ns", dataset.get("vms_per_namespace", 0)
                        )
                        existing_entry["list_of_datasets_which_belong_to_this_namespace"].append(
                            self._process_dataset(dataset, namespace)
                        )
                    else:
                        summary = {
                            "namespace": namespace,
                            "total_datasets_for_this_namespace": 1,
                            "total_pods_per_all_datasets_with_same_namespace": dataset.get(
                                "pods_per_ns", dataset.get("vms_per_namespace", 0)
                            ),
                            "list_of_datasets_which_belong_to_this_namespace": [self._process_dataset(dataset, namespace)],
                            "all_ds_exists": False,
                            "all_ds_validated": False,
                        }
                        all_datasets.append(summary)
                self._OadpWorkloads__scenario_datasets = all_datasets
                self.print_all_ds(scenario)
            elif isinstance(scenario["dataset"], dict):
                namespace = scenario["args"].get("namespaces_to_backup", "")
                ds_dict = scenario["dataset"]
                summary = {
                    "namespace": namespace,
                    "total_datasets_for_this_namespace": 1,
                    "total_pods_per_all_datasets_with_same_namespace": ds_dict.get(
                        "pods_per_ns", ds_dict.get("vms_per_namespace", 0)
                    ),
                    "list_of_datasets_which_belong_to_this_namespace": [
                        self._process_dataset(dataset=ds_dict, namespace=namespace)
                    ],
                    "all_ds_exists": False,
                    "all_ds_validated": False,
                }
                all_datasets.append(summary)
                self._OadpWorkloads__scenario_datasets = all_datasets
                self.print_all_ds(scenario)
        except Exception as err:
            self.fail_test_run(f"Exception {err} occurred in " + self.get_current_function())
            raise err

    def _process_dataset(self, dataset: dict, namespace: str) -> dict:
        """Return a dataset entry augmented with namespace and existence validation flags."""
        return {**dataset, "namespace": namespace, "exists": False, "validated": False}

    def ds_get_datasets_for_namespace(self, namespace: str) -> list:
        """Return the list of dataset dicts for the given namespace."""
        return next(
            (
                s["list_of_datasets_which_belong_to_this_namespace"]
                for s in self._OadpWorkloads__scenario_datasets
                if s["namespace"] == namespace
            ),
            [],
        )

    def ds_get_total_datasets_for_namespace(self, namespace: str) -> int:
        """Return how many datasets are defined for the given namespace."""
        return next(
            (
                s["total_datasets_for_this_namespace"]
                for s in self._OadpWorkloads__scenario_datasets
                if s["namespace"] == namespace
            ),
            0,
        )

    def ds_get_all_namespaces(self) -> list:
        """Return all namespace strings referenced in scenario_datasets."""
        return [s["namespace"] for s in self._OadpWorkloads__scenario_datasets]

    def ds_get_all_storageclass(self, scenario: dict) -> list | None:
        """Collect unique storage class names from the scenario or aggregated scenario_datasets."""
        try:
            if isinstance(scenario["dataset"], dict):
                return [scenario["dataset"]["sc"]]
            if isinstance(scenario["dataset"], list):
                unique_sc_list = set()
                for s in self._OadpWorkloads__scenario_datasets:
                    datasets = s.get("list_of_datasets_which_belong_to_this_namespace", [])
                    for dataset in datasets:
                        sc = dataset.get("sc")
                        if sc:
                            unique_sc_list.add(sc)
                return list(unique_sc_list)
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    def ds_get_datasets_for_namespace_with_status(
        self, namespace: str, exists: bool | None = None, validated: bool | None = None
    ) -> list:
        """Return datasets for a namespace, optionally filtered by exists or validated flags."""
        datasets = next(
            (
                s["list_of_datasets_which_belong_to_this_namespace"]
                for s in self._OadpWorkloads__scenario_datasets
                if s["namespace"] == namespace
            ),
            [],
        )
        if exists is not None or validated is not None:
            return [
                ds
                for ds in datasets
                if (exists is None or ds["exists"] == exists) and (validated is None or ds["validated"] == validated)
            ]
        return datasets

    @logger_time_stamp
    def get_unique_namespaces(self, scenario: dict) -> list:
        """Return distinct namespace names from the scenario dataset structure."""
        dataset_value = scenario.get("dataset")
        namespaces = set()

        if isinstance(dataset_value, list):
            for entry in dataset_value:
                namespace = entry.get("namespace")
                if namespace:
                    namespaces.add(namespace)
        elif isinstance(dataset_value, dict):
            namespace = dataset_value.get("namespace")
            if namespace:
                namespaces.add(namespace)

        logger.info(
            f"### INFO ### Dataset current contains {len(list(namespaces))} namespaces "
            f"are related to datasets {list(namespaces)}"
        )
        return list(namespaces)

    @logger_time_stamp
    def are_multiple_datasets_present(self, scenario: dict) -> bool:
        """Return True if the scenario defines more than one dataset or list-valued dataset fields."""
        dataset_value = scenario.get("dataset")
        if isinstance(dataset_value, list) and len(dataset_value) > 1:
            return True
        elif not isinstance(dataset_value, list) and isinstance(dataset_value, dict):
            return any(isinstance(value, list) for value in dataset_value.values())
        return False

    @logger_time_stamp
    def calc_total_pods_per_namespace_in_datasets(self, scenario: dict, namespace: str) -> int:
        """Sum pods/VMs count for entries matching namespace in list or dict dataset form."""
        dataset_value = scenario.get("dataset")
        if isinstance(dataset_value, list):
            total_pods = sum(
                entry.get("pods_per_ns", entry.get("vms_per_namespace", 0))
                for entry in dataset_value
                if entry.get("namespace") == namespace
            )
        elif isinstance(dataset_value, dict):
            total_pods = (
                dataset_value.get("pods_per_ns", dataset_value.get("vms_per_namespace", 0))
                if dataset_value.get("namespace") == namespace
                else 0
            )
        else:
            total_pods = 0
        logger.info(f"### INFO ### calc_total_pods_per_namespace shows {namespace} has {total_pods}")
        return total_pods

    @logger_time_stamp
    def calc_total_dataset_utilization_per_namespace(self, test_scenario: dict) -> dict | None:
        """Compute per-namespace dataset size in MB and record totals in run_metadata."""
        dataset_value = test_scenario.get("dataset")
        ns_capacities = {}

        if isinstance(dataset_value, list):
            for item in test_scenario["dataset"]:
                namespace = item["namespace"]
                expected_capacity = item.get("expected_capacity", "0K")
                asset_count = item.get("pods_per_ns", item.get("vms_per_namespace", 0))
                value, unit = self._parse_capacity(expected_capacity)
                value = self._convert_to_mb(value, unit)
                ns_capacities[namespace] = ns_capacities.get(namespace, 0) + (value * asset_count)

            total_data = sum(ns_capacities.values())
            ns_capacities["total_data_in_mb"] = total_data
            self._OadpWorkloads__run_metadata["summary"]["runtime"]["datasetsize"] = {}
            self._OadpWorkloads__run_metadata["summary"]["runtime"]["datasetsize"].update(ns_capacities)
            logger.info(f"ns_capacities: {ns_capacities} total data is: {total_data}")
            return ns_capacities

        elif isinstance(dataset_value, dict):
            ds_dict = test_scenario["dataset"]
            namespace = test_scenario["args"]["namespaces_to_backup"]
            expected_capacity = ds_dict.get("expected_capacity", "0K")
            asset_count = ds_dict.get("pods_per_ns", ds_dict.get("vms_per_namespace", 0))
            value, unit = self._parse_capacity(expected_capacity)
            value = self._convert_to_mb(value, unit)
            ns_capacities[namespace] = value * asset_count

            total_data = sum(ns_capacities.values())
            ns_capacities["total_data_in_mb"] = total_data
            self._OadpWorkloads__run_metadata["summary"]["runtime"].setdefault("dataset", {})
            self._OadpWorkloads__run_metadata["summary"]["runtime"]["dataset"]["sizes"] = {}
            self._OadpWorkloads__run_metadata["summary"]["runtime"]["dataset"]["sizes"].update(ns_capacities)
            logger.info(f"ns_capacities: {ns_capacities} total data is: {total_data}")
            return ns_capacities

    @staticmethod
    def _parse_capacity(expected_capacity: str) -> tuple[float, str]:
        """Parse a capacity string like '1G' into numeric value and unit letter."""
        match = re.match(r"(\d+\.?\d*)(K|M|G|T)", expected_capacity)
        if match:
            return float(match.group(1)), match.group(2)
        raise ValueError(f"Invalid expected_capacity format: {expected_capacity}")

    @staticmethod
    def _convert_to_mb(value: float, unit: str) -> float:
        """Convert a capacity value to megabytes using K/M/G/T unit suffix semantics."""
        if unit == "K":
            return value / 1024
        elif unit == "G":
            return value * 1024
        elif unit == "T":
            return value * 1024 * 1024
        return value

    def capacity_in_bytes(self, storage_str: str) -> int:
        """Convert a Kubernetes quantity string (e.g. '10Gi') to bytes."""
        storage_unit = storage_str[-2:]
        storage_value = int(storage_str[:-2])
        unit_multiplier = {
            "Ki": 2**10,
            "Mi": 2**20,
            "Gi": 2**30,
            "Ti": 2**40,
        }
        if storage_unit not in unit_multiplier:
            raise ValueError(f"Invalid storage unit: {storage_unit}")
        return storage_value * unit_multiplier[storage_unit]

    @logger_time_stamp
    def get_dataset_details(self, scenario: dict) -> None:
        """Load scenario datasets, merge scenario into run metadata, and compute utilization."""
        try:
            self.load_datasets_for_scenario(scenario)
            self._OadpWorkloads__run_metadata["summary"]["runtime"].update(scenario)
            self.calc_total_dataset_utilization_per_namespace(scenario)
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

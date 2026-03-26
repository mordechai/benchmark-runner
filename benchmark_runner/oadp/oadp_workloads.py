"""
OADP workload orchestrator.

This module is the entry point for running OADP (OpenShift API Data Protection)
benchmark workloads. It composes functionality from focused mixin modules and
delegates to the ``WorkloadsOperations`` base class for shared infrastructure.
"""

from __future__ import annotations

import os

from benchmark_runner.common.logger.logger_time_stamp import logger, logger_time_stamp
from benchmark_runner.common.ssh.ssh import SSH
from benchmark_runner.oadp.cleanup import OadpCleanupMixin
from benchmark_runner.oadp.cluster_ops import OadpClusterOpsMixin
from benchmark_runner.oadp.configuration import OadpConfigurationMixin
from benchmark_runner.oadp.constants import (
    DEFAULT_DPA_NAME,
    DEFAULT_RETRY_INTERVAL,
    DEFAULT_RETRY_MAX_ATTEMPTS,
    MINIO_DEFAULT_ACCESS_KEY,
    MINIO_DEFAULT_SECRET_KEY,
    MPQE_BUSYBOX_PATH,
    MPQE_MISC_DIR,
    MPQE_OADP_BASE_DIR,
    MPQE_SCENARIO_DATA,
    PREVIOUS_REPORT_PATH,
    RESULT_REPORT_PATH,
    SOURCE_UPSTREAM,
    VALIDATION_MODE_LIGHT,
    VELERO_CLI_PATH,
    VM_DATASET_ROLE,
)
from benchmark_runner.oadp.datamover import OadpDatamoverMixin
from benchmark_runner.oadp.dataset import OadpDatasetMixin
from benchmark_runner.oadp.environment import OadpEnvironmentMixin
from benchmark_runner.oadp.execution import OadpExecutionMixin
from benchmark_runner.oadp.log_collection import OadpLogCollectionMixin
from benchmark_runner.oadp.oadp_exceptions import MissingResultReport
from benchmark_runner.oadp.pod_validation import OadpPodValidationMixin
from benchmark_runner.oadp.reporting import OadpReportingMixin
from benchmark_runner.oadp.resources import OadpResourcesMixin
from benchmark_runner.oadp.scenario import OadpScenarioMixin
from benchmark_runner.oadp.validation import OadpValidationMixin
from benchmark_runner.oadp.vm_operations import OadpVmOperationsMixin
from benchmark_runner.oadp.vm_validation import OadpVmValidationMixin
from benchmark_runner.workloads.workloads_operations import WorkloadsOperations


class OadpWorkloads(
    OadpConfigurationMixin,
    OadpDatamoverMixin,
    OadpDatasetMixin,
    OadpVmOperationsMixin,
    OadpVmValidationMixin,
    OadpExecutionMixin,
    OadpPodValidationMixin,
    OadpValidationMixin,
    OadpReportingMixin,
    OadpLogCollectionMixin,
    OadpCleanupMixin,
    OadpResourcesMixin,
    OadpEnvironmentMixin,
    OadpScenarioMixin,
    OadpClusterOpsMixin,
    WorkloadsOperations,
):
    """
    Orchestrator for OADP (OpenShift API Data Protection) workloads.

    Composes focused mixin classes for cluster operations, scenario management,
    environment detection, configuration, dataset handling, execution, validation,
    resource collection, reporting, and cleanup.
    """

    def __init__(self) -> None:
        """Initialize paths, SSH client, test environment, and run metadata state."""
        super().__init__()
        # External script paths
        self.__oadp_path = MPQE_BUSYBOX_PATH
        self.__oadp_base_dir = MPQE_OADP_BASE_DIR
        self.__oadp_misc_dir = MPQE_MISC_DIR
        self.__oadp_scenario_data = MPQE_SCENARIO_DATA

        # Environment variables
        self.__namespace = self._environment_variables_dict.get("namespace", "")
        self.__oadp_workload = self._environment_variables_dict.get("oadp", "")
        self.__oadp_uuid = self._environment_variables_dict.get("oadp_uuid", "")
        self.__oadp_scenario_name = self._environment_variables_dict.get("oadp_scenario", "")
        self.__oadp_bucket = self._environment_variables_dict.get("oadp_bucket", False)
        self.__oadp_cleanup_cr_post_run = self._environment_variables_dict.get("oadp_cleanup_cr", False)
        self.__oadp_cleanup_dataset_post_run = self._environment_variables_dict.get(
            "oadp_cleanup_dataset",
            False,
        )
        self.__oadp_validation_mode = self._environment_variables_dict.get(
            "validation_mode",
            VALIDATION_MODE_LIGHT,
        )
        self.__oadp_enable_kubevirt = self._environment_variables_dict.get("oadp_enable_kubevirt", False)
        self.__oadp_resource_collection = False
        self.__oadp_ds_failing_validation = []
        self.__retry_logic = {
            "interval_between_checks": DEFAULT_RETRY_INTERVAL,
            "max_attempts": DEFAULT_RETRY_MAX_ATTEMPTS,
        }

        # Report / artifact paths (reassign _run_artifacts_path before derived paths)
        self.__result_report = RESULT_REPORT_PATH
        self._run_artifacts_path = self._environment_variables_dict.get("run_artifacts_path", "")
        self.__artifactdir = os.path.join(self._run_artifacts_path, "oadp-ci")
        self.__oadp_log = os.path.join(self._run_artifacts_path, "oadp.log")

        # SSH client
        self.__ssh = SSH()

        # Resource tracking
        self.__oadp_resources = {}
        self.__oadp_runtime_resource_mapping = {}

        # MinIO / DPA defaults
        self.__oadp_minio_bucket = {
            "access_key": MINIO_DEFAULT_ACCESS_KEY,
            "secret_key": MINIO_DEFAULT_SECRET_KEY,
        }
        self.__oadp_dpa = DEFAULT_DPA_NAME

        # Test environment
        self.__test_env = {
            "source": SOURCE_UPSTREAM,
            "velero_ns": self._environment_variables_dict.get("oadp_velero_ns", "openshift-adp"),
            "velero_cli_path": VELERO_CLI_PATH,
        }

        # Result state
        self.__result_dicts = []
        self.__run_labels = {
            "key": self._environment_variables_dict.get("oadp_label_name", ""),
            "value": self._environment_variables_dict.get("oadp_label_value", ""),
        }
        self.__scenario_datasets = []
        self.__run_metadata = {
            "index": "",
            "metadata": {},
            "status": "",
            "summary": {
                "env": {"ocp": {}, "storage": {}},
                "runtime": {},
                "results": {},
                "validations": {},
                "resources": {"nodes": {}, "run_time_pods": [], "pods": {}},
                "transactions": [],
            },
        }

    # ------------------------------------------------------------------
    # Dataset type detection
    # ------------------------------------------------------------------

    def _scenario_has_vm_datasets(self, scenario: dict) -> bool:
        """Return True when the feature flag is on and scenario contains VM datasets."""
        if not self.__oadp_enable_kubevirt:
            return False
        dataset_value = scenario.get("dataset")
        if isinstance(dataset_value, list):
            return any(d.get("role") == VM_DATASET_ROLE for d in dataset_value)
        if isinstance(dataset_value, dict):
            return dataset_value.get("role") == VM_DATASET_ROLE
        return False

    # ------------------------------------------------------------------
    # Lifecycle hooks
    # ------------------------------------------------------------------

    @logger_time_stamp
    def initialize_workload(self) -> None:
        """Start Prometheus collection when snapshot mode is enabled."""
        if self._enable_prometheus_snapshot:
            self.start_prometheus()

    @logger_time_stamp
    def finalize_workload(self) -> None:
        """Upload results, stop metrics, sync artifacts, and run teardown."""
        if os.path.exists(self.__result_report) and os.stat(self.__result_report).st_size != 0:
            self.upload_oadp_result_to_elasticsearch()

        if self._enable_prometheus_snapshot:
            self.end_prometheus()
        if self._endpoint_url:
            self.upload_run_artifacts_to_s3()
        if not self._save_artifacts_local:
            self.delete_local_artifacts()
        self.delete_all()

    # ------------------------------------------------------------------
    # Main workflow
    # ------------------------------------------------------------------

    @logger_time_stamp
    def run_workload(self) -> bool:
        """Execute the full OADP workload lifecycle for the configured scenario."""
        test_scenario = self.load_test_scenario()
        self.get_dataset_details(scenario=test_scenario)

        self.remove_previous_run_report()
        self.set_velero_stream_source()

        self.get_ocp_details()
        self.get_velero_details()
        self.get_storage_details()
        if self.this_is_downstream():
            self.oadp_get_version_info()

        self.generate_elastic_index(test_scenario)

        if self.this_is_downstream():
            for sc in self.ds_get_all_storageclass(test_scenario):
                self.set_default_storage_class(sc)
                self.set_volume_snapshot_class(sc, test_scenario)

        self.set_velero_log_level(oadp_namespace=self.__test_env["velero_ns"])

        if self.this_is_downstream():
            if self.is_dpa_change_needed(scenario=test_scenario, oadp_namespace=self.__test_env["velero_ns"]):
                self.config_dpa_for_plugin(scenario=test_scenario, oadp_namespace=self.__test_env["velero_ns"])
                self.wait_for_dpa_changes(oadp_namespace=self.__test_env["velero_ns"])
                self.verify_bsl_status()

        self.get_bucket_details("velero")

        has_vm_datasets = self._scenario_has_vm_datasets(test_scenario)

        if test_scenario["args"]["OADP_CR_TYPE"] == "backup":
            if has_vm_datasets:
                self.verify_vm_datasets_before_backups(test_scenario)
            else:
                self.verify_datsets_before_backups(test_scenario)

        if test_scenario["args"]["OADP_CR_TYPE"] == "restore":
            remove_source_dataset = test_scenario["args"].get("existingResourcePolicy", False)
            if remove_source_dataset != "Update":
                for ns in self.ds_get_all_namespaces():
                    self.delete_source_dataset(target_namespace=ns)

        oadp_cr_already_present = self.is_cr_present(
            ns=self.__test_env["velero_ns"],
            cr_type=test_scenario["args"]["OADP_CR_TYPE"],
            cr_name=test_scenario["args"]["OADP_CR_NAME"],
        )
        if oadp_cr_already_present:
            logger.warning(
                f"You are attempting to use CR name: {test_scenario['args']['OADP_CR_NAME']} "
                f"which is already present so it will be deleted"
            )
            self.delete_oadp_custom_resources(
                ns=self.__test_env["velero_ns"],
                cr_type=test_scenario["args"]["OADP_CR_TYPE"],
                cr_name=test_scenario["args"]["OADP_CR_NAME"],
            )

        if self.__oadp_resource_collection:
            self.collect_all_node_resource()
            self.get_resources_per_ns(namespace=self.__test_env["velero_ns"], label="start")

        self.oadp_execute_scenario(test_scenario, run_method="python")

        if test_scenario["args"]["OADP_CR_TYPE"] == "restore":
            if has_vm_datasets:
                self.validate_restored_vm_datasets(test_scenario)
            else:
                self.set_validation_retry_logic(interval_between_checks=15, max_attempts=28)
                dataset_restored = self.validate_expected_datasets(test_scenario)
                self.__run_metadata["summary"]["validations"]["dataset_restored_as_expected"] = (
                    "PASS" if dataset_restored else "FAIL"
                )
                self.__run_metadata["summary"]["results"]["dataset_post_run_validation"] = dataset_restored
                if not dataset_restored:
                    logger.error(
                        f"Restored Dataset for {test_scenario['args']['OADP_CR_NAME']} did not pass post run validations"
                    )
                else:
                    logger.info("Restore passed post run validations")

        self.validate_cr(
            ns=self.__test_env["velero_ns"],
            cr_type=test_scenario["args"]["OADP_CR_TYPE"],
            cr_name=test_scenario["args"]["OADP_CR_NAME"],
        )

        if test_scenario["args"]["OADP_CR_TYPE"] == "backup" and test_scenario["args"]["plugin"] == "kopia":
            self.validate_podvolumebackups(test_scenario)
        if test_scenario["args"]["OADP_CR_TYPE"] == "restore" and test_scenario["args"]["plugin"] == "kopia":
            self.validate_podvolumerestores(test_scenario)
        if test_scenario["args"]["OADP_CR_TYPE"] == "backup" and test_scenario["args"]["plugin"] == "vbd":
            self.validate_datauploads(test_scenario)
        if test_scenario["args"]["OADP_CR_TYPE"] == "restore" and test_scenario["args"]["plugin"] == "vbd":
            self.validate_dataDownload(test_scenario)

        if self.__oadp_resource_collection:
            self.collect_all_node_resource()
            self.get_resources_per_ns(namespace=self.__test_env["velero_ns"], label="end")

        self.parse_oadp_cr(
            ns=self.__test_env["velero_ns"],
            cr_type=test_scenario["args"]["OADP_CR_TYPE"],
            cr_name=test_scenario["args"]["OADP_CR_NAME"],
        )
        self.check_oadp_cr_for_errors_and_warns(scenario=test_scenario)
        self.get_oadp_velero_and_cr_log(
            cr_name=test_scenario["args"]["OADP_CR_NAME"],
            cr_type=test_scenario["args"]["OADP_CR_TYPE"],
        )
        self.invoke_log_collection(test_scenario)

        self.verify_pod_restarts(self.__test_env["velero_ns"])
        self.verify_cluster_operators_status()

        self.enrich_summary_with_vm_metadata(test_scenario)
        self.set_run_status()
        self.create_json_summary()

        self.cleaning_up_oadp_resources(scenario=test_scenario)

        if os.path.exists(self.__result_report) and os.stat(self.__result_report).st_size != 0:
            self.__ssh.run(cmd=f"cp {self.__result_report} {self._run_artifacts_path}")
            logger.info(f"### INFO ### OADP Report copied to {self._run_artifacts_path}")
            old_report_path = self.__result_report
            self.__result_report = os.path.join(self._run_artifacts_path, "oadp-report.json")
            self.__ssh.run(cmd=f"mv {old_report_path} {PREVIOUS_REPORT_PATH}")
            return True
        else:
            self.send_failed_result(scenario=test_scenario)
            logger.warning(f" WARN FYI - self.__result_report at {self.__result_report} was empty")
            raise MissingResultReport()

    @logger_time_stamp
    def run(self) -> bool:
        """Run the OADP workload: initialize, execute, finalize."""
        try:
            self.initialize_workload()
            if self.run_workload():
                self.finalize_workload()
        except Exception as e:
            logger.error(f"{self._workload} workload raised an exception: {e!s}")
            raise e

        return True

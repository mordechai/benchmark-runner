"""
Mixin for result reporting, Elasticsearch upload, log collection, and timing.
"""

from __future__ import annotations

import json
import os
import traceback
from datetime import datetime
from typing import Any, NoReturn

from benchmark_runner.common.logger.logger_time_stamp import logger, logger_time_stamp
from benchmark_runner.oadp.constants import DATETIME_FORMAT, RESULT_REPORT_PATH, VM_DATASET_ROLE


class OadpReportingMixin:
    """JSON summary, ES upload, log collection, timers, status."""

    @logger_time_stamp
    def create_json_summary(self) -> None:
        """Write run_metadata JSON to the standard OADP report path."""
        try:
            with open(RESULT_REPORT_PATH, "w", encoding="utf-8") as f:
                json.dump(self._OadpWorkloads__run_metadata, f, indent=4, sort_keys=True, default=str)
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def upload_oadp_result_to_elasticsearch(self) -> None:
        """Enrich the result report with metadata and upload it to Elasticsearch."""
        metadata_details = {}
        result_report_json_file = open(self._OadpWorkloads__result_report)
        result_report_json_str = result_report_json_file.read()
        result_report_json_data = json.loads(result_report_json_str)
        index = self._OadpWorkloads__run_metadata["index"]
        logger.info(f"upload index: {index}")
        metadata_details = {
            "labels": self._OadpWorkloads__run_labels,
            "uuid": self._environment_variables_dict["uuid"],
            "upload_date": datetime.now().strftime(DATETIME_FORMAT),
            "run_artifacts_url": os.path.join(
                self._run_artifacts_url,
                f"{self._get_run_artifacts_hierarchy(workload_name=self._workload, is_file=True)}"
                f"-{self._time_stamp_format}.tar.gz",
            ),
            "scenario": self._OadpWorkloads__run_metadata["summary"]["runtime"]["name"],
        }
        result_report_json_data["metadata"] = {}
        result_report_json_data["metadata"].update(metadata_details)
        with open(self._OadpWorkloads__result_report, "w") as output_file:
            json.dump(result_report_json_data, output_file, indent=4)

        try:
            self._es_operations.upload_to_elasticsearch(index=index, data=result_report_json_data)
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def send_failed_result(self, scenario: dict) -> None:
        """Upload a minimal failed-result payload to Elasticsearch for the scenario."""
        result_report_json_data = {
            "result": "Failed",
            "status": "Failed",
            "run_artifacts_url": os.path.join(
                self._run_artifacts_url,
                f"{self._get_run_artifacts_hierarchy(workload_name=self._workload, is_file=True)}"
                f"-{self._time_stamp_format}.tar.gz",
            ),
        }
        index = self.generate_elastic_index(scenario)
        logger.info(f"upload index: {index} after self.__result_report content check failed")
        metadata_details = {
            "uuid": self._environment_variables_dict["uuid"],
            "upload_date": datetime.now().strftime(DATETIME_FORMAT),
            "run_artifacts_url": result_report_json_data["run_artifacts_url"],
            "scenario": self._OadpWorkloads__run_metadata["summary"]["runtime"]["name"],
        }
        result_report_json_data["metadata"] = {}
        result_report_json_data["metadata"].update(metadata_details)
        try:
            self._es_operations.upload_to_elasticsearch(index=index, data=result_report_json_data)
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def get_logs_by_pod_ns(self, namespace: str) -> None:
        """Write oc logs for each Running pod in namespace into run artifacts."""
        list_of_pods_found = self.get_list_of_pods(namespace=namespace)
        if isinstance(list_of_pods_found, list):
            for p in list_of_pods_found:
                output_filename = os.path.join(self._run_artifacts_path, f"{p}.log")
                logger.info(f"performing oc logs {p} -n {namespace} redirected to {output_filename} ")
                self._OadpWorkloads__ssh.run(cmd=f"oc logs {p} -n {namespace} > {output_filename}")

    @logger_time_stamp
    def get_oadp_velero_and_cr_log(self, cr_name: str, cr_type: str) -> None:
        """Collect Velero CLI logs and CR JSON for the named backup/restore resource."""
        try:
            oadp_cr_log = os.path.join(self._run_artifacts_path, "oadp-cr.json")
            oadp_velero_log = os.path.join(self._run_artifacts_path, "oadp-velero.log")
            test_env = self._OadpWorkloads__test_env
            if test_env["source"] == "upstream":
                self._OadpWorkloads__ssh.run(
                    cmd=f"cd {test_env['velero_cli_path']}/velero/cmd/velero; "
                    f"./velero {cr_type} logs {cr_name} -n {test_env['velero_ns']} >> {oadp_velero_log}"
                )
            else:
                self._OadpWorkloads__ssh.run(
                    cmd=f"oc -n {test_env['velero_ns']} exec deployment/velero -c velero -it -- "
                    f"./velero {cr_type} logs {cr_name} --insecure-skip-tls-verify >> {oadp_velero_log}"
                )
            if not os.path.exists(oadp_velero_log) or os.stat(oadp_velero_log).st_size == 0:
                logger.warning(f"oadp_velero_log is either not present or empty check file path: {oadp_velero_log}")
            qualified_type = f"{cr_type}.velero.io" if cr_type in ("backup", "restore") else cr_type
            self._OadpWorkloads__ssh.run(
                cmd=f"oc get {qualified_type} {cr_name} -n {test_env['velero_ns']} -o json >> {oadp_cr_log}"
            )
            if not os.path.exists(oadp_cr_log) or os.stat(oadp_cr_log).st_size == 0:
                logger.warning(f"oadp_cr_log is either not present or empty check file path: {oadp_cr_log}")
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def invoke_log_collection(self, scenario: dict) -> None:
        """Collect diagnostic artifacts for the OADP run into the artifacts directory."""
        try:
            logs_folder = self._run_artifacts_path
            velero_ns = self._OadpWorkloads__test_env["velero_ns"]
            plugin = scenario["args"]["plugin"]
            cr_name = scenario["args"]["OADP_CR_NAME"]

            logger.info(
                f"invoke_log_collection is attempting to collect logs from cr: {cr_name} and write logs to dir: {logs_folder}"
            )

            self._ensure_log_dirs(logs_folder, scenario)
            self.collect_scenario_summary(logs_folder, scenario, velero_ns)
            self.collect_velero_describe(logs_folder, scenario, velero_ns)
            self.collect_bsl_yaml(logs_folder, velero_ns)
            if self.this_is_downstream():
                self.collect_dpa_yaml(logs_folder, scenario, velero_ns)
                self.collect_cr_yaml(logs_folder, scenario, velero_ns)
            self.collect_velero_ns_pod_logs(logs_folder, velero_ns)
            if plugin != "csi":
                self.collect_backup_repositories(logs_folder, scenario, velero_ns)
            self.collect_bucket_content(logs_folder)
            self.collect_pod_distribution(logs_folder, scenario)
            self.collect_plugin_objects(logs_folder, scenario, velero_ns)
            self.collect_cluster_events(logs_folder, scenario["name"])
            self.invoke_vm_log_collection(logs_folder, scenario, velero_ns)

            list_of_files = list(self._OadpWorkloads__ssh.run(cmd=f"find {logs_folder} -type f").splitlines())
            logger.info(f"Artifact files collected: {list_of_files}")

            if len(list_of_files) < 10:
                logger.error(f"Log collection produced fewer files than expected. Total files: {len(list_of_files)}")

            for filepath in list_of_files:
                if filepath and os.path.exists(filepath) and os.stat(filepath).st_size == 0:
                    logger.error(f"invoke_log_collection: artifact file is 0 bytes: {filepath}")
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def oadp_timer(self, action: str, transaction_name: str) -> None:
        """Start or stop a named transaction timer in run_metadata summary."""
        if action == "start":
            time_start = datetime.now()
            transaction = {
                "transaction_name": transaction_name,
                "start_at": time_start,
                "stopped_at": [],
                "duration": [],
            }
            self._OadpWorkloads__run_metadata["summary"]["transactions"].append(transaction)
        elif action == "stop":
            time_end = datetime.now()
            transactions = self._OadpWorkloads__run_metadata["summary"]["transactions"]
            for trans in range(len(transactions)):
                if transactions[trans]["transaction_name"] == transaction_name:
                    transactions[trans]["stopped_at"] = time_end
                    transactions[trans]["duration"] = str(time_end - transactions[trans]["start_at"])

    @logger_time_stamp
    def set_run_status(self, msg: dict | str = "") -> None:
        """Merge optional results message and set overall status from runtime results."""
        try:
            if msg:
                self._OadpWorkloads__run_metadata["summary"]["results"].update(msg)
            runtime_results = self._OadpWorkloads__run_metadata["summary"]["runtime"].get("results", {})
            self._OadpWorkloads__run_metadata["status"] = runtime_results.get("cr_status", "error")
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    def fail_test_run(self, msg: str) -> NoReturn:
        """Log and re-raise a runtime error to mark the test run as failed."""
        try:
            raise RuntimeError(msg)
        except Exception as e:
            traceback_info = traceback.extract_stack()[:-2]
            method_name = traceback_info[-1].name
            logger.error(f"Exception: {e}\nMethod: {method_name}\nMessage: {msg}")
            raise

    def log_this(self, level: str | None = None, msg: str | None = None, obj_to_json: Any | None = None) -> None:
        """Log an optional message and pretty-printed JSON object at info level."""
        full_msg = ""
        if msg is not None:
            full_msg = f"### {level} ### {msg} "
        if obj_to_json is not None:
            pretty_json = json.dumps(obj_to_json, indent=4, sort_keys=True)
            full_msg = full_msg + pretty_json
        if full_msg != "" or obj_to_json is not None:
            logger.info(full_msg)

    @logger_time_stamp
    def enrich_summary_with_vm_metadata(self, scenario: dict) -> None:
        """Add VM-specific fields to the run_metadata JSON summary."""
        try:
            dataset_value = scenario.get("dataset")
            has_vms = False
            if isinstance(dataset_value, list):
                has_vms = any(d.get("role") == VM_DATASET_ROLE for d in dataset_value)
            elif isinstance(dataset_value, dict):
                has_vms = dataset_value.get("role") == VM_DATASET_ROLE

            if not has_vms:
                return

            run_metadata = self._OadpWorkloads__run_metadata
            run_metadata["summary"]["runtime"]["dataset_type"] = "kubevirt"

            ssh = self._OadpWorkloads__ssh
            cnv_csv = ssh.run(
                cmd="oc get csv -n openshift-cnv --no-headers "
                '-o custom-columns=":metadata.name" 2>/dev/null '
                "| grep kubevirt-hyperconverged-operator | head -1"
            )
            if cnv_csv.strip():
                cnv_version = ssh.run(
                    cmd=f"oc get csv {cnv_csv.strip()} -n openshift-cnv -o jsonpath='{{.spec.version}}' 2>/dev/null"
                )
                run_metadata["summary"]["env"]["cnv"] = {
                    "version": cnv_version.strip(),
                    "csv": cnv_csv.strip(),
                }

            namespace = scenario["args"].get("namespaces_to_backup", "")
            if namespace:
                vm_count = ssh.run(cmd=f"oc get vm -n {namespace} --no-headers 2>/dev/null | wc -l")
                running_vmis = ssh.run(cmd=f"oc get vmi -n {namespace} --no-headers 2>/dev/null | grep -c Running")
                dv_count = ssh.run(cmd=f"oc get dv -n {namespace} --no-headers 2>/dev/null | wc -l")
                run_metadata["summary"]["resources"]["vm_counts"] = {
                    "total_vms": self._safe_int(vm_count),
                    "running_vmis": self._safe_int(running_vmis),
                    "total_datavolumes": self._safe_int(dv_count),
                }

        except Exception as err:
            logger.warning(f"enrich_summary_with_vm_metadata failed: {err}")

    @staticmethod
    def _safe_int(value: str) -> int:
        try:
            return int(value.strip())
        except (ValueError, AttributeError):
            return 0

    @logger_time_stamp
    def remove_previous_run_report(self) -> bool | None:
        """Delete a leftover OADP result report file if present; return True if removed."""
        try:
            if os.path.exists(self._OadpWorkloads__result_report):
                logger.warning(
                    f"### WARN ### existing file related to OADP Report found at "
                    f"{self._OadpWorkloads__result_report} this maybe a left over test result "
                )
                os.remove(self._OadpWorkloads__result_report)
                logger.info(f"### INFO ### OADP Report at {self._OadpWorkloads__result_report} was removed")
                return True
            else:
                logger.info(
                    f"### INFO ### Checks for left over OADP Reports were successful no left overs "
                    f"found at {self._OadpWorkloads__result_report} "
                )
        except Exception as err:
            self.fail_test_run(f" {err} index is not found occurred in " + self.get_current_function())
            raise err

"""
Mixin for OADP custom resource (CR) validation — volume operations,
CR parsing, and CR error/warning checks.
"""

import os
from datetime import datetime

from benchmark_runner.common.logger.logger_time_stamp import logger, logger_time_stamp
from benchmark_runner.oadp.constants import CR_TIMESTAMP_FORMAT


class OadpValidationMixin:
    """CR-level validation: PVB/PVR/DataUpload/Download, parse, and error checks."""

    def _validate_volume_operations(
        self, scenario, resource_type, name_label_path, cr_name_key, results_key, validation_key, completed_key, failed_key
    ):
        try:
            cr_name = scenario["args"][cr_name_key]
            data = self.get_oc_resource_to_json(
                resource_type=resource_type,
                resource_name="",
                namespace=self._OadpWorkloads__test_env["velero_ns"],
            )
            total_completed = 0
            total_failed = 0
            durations = {}

            for item in data["items"]:
                label_value = item["metadata"].get("labels", {}).get(name_label_path)
                if name_label_path in ("velero.io/restore-name", "velero.io/backup-name"):
                    label_value = item["metadata"].get("labels", {}).get(name_label_path)
                else:
                    label_value = item["spec"]["tags"].get(name_label_path)

                status_phase = item["status"].get("phase")

                if label_value == cr_name:
                    if status_phase == "Completed":
                        total_completed += 1
                        start_time = datetime.strptime(item["status"]["startTimestamp"], CR_TIMESTAMP_FORMAT)
                        end_time = datetime.strptime(item["status"]["completionTimestamp"], CR_TIMESTAMP_FORMAT)
                        duration = (end_time - start_time).total_seconds()
                        durations.setdefault("durations", []).append(duration)
                    else:
                        total_failed += 1
                        logger.warning(f"Failed {resource_type}: {item}")

            if durations.get("durations"):
                d = durations["durations"]
                durations["max"] = max(d)
                durations["min"] = min(d)
                durations["avg"] = sum(d) / len(d)
                del durations["durations"]

            durations[completed_key] = total_completed
            durations[failed_key] = total_failed

            self._OadpWorkloads__run_metadata["summary"]["results"][results_key] = {}
            self._OadpWorkloads__run_metadata["summary"]["results"][results_key].update(durations)

            if durations[failed_key] != 0:
                self._OadpWorkloads__run_metadata["summary"]["validations"][validation_key] = "FAIL"
            else:
                self._OadpWorkloads__run_metadata["summary"]["validations"][validation_key] = "PASS"

            logger.info(f"{results_key} results: {durations}")
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    def validate_podvolumerestores(self, scenario):
        self._validate_volume_operations(
            scenario,
            resource_type="podvolumerestore",
            name_label_path="velero.io/restore-name",
            cr_name_key="OADP_CR_NAME",
            results_key="podvolumerestores",
            validation_key="podvolumerestore_post_run_validation",
            completed_key="total_completed_pvr",
            failed_key="total_failed_pvr",
        )

    def validate_dataDownload(self, scenario):
        self._validate_volume_operations(
            scenario,
            resource_type="dataDownload",
            name_label_path="velero.io/restore-name",
            cr_name_key="OADP_CR_NAME",
            results_key="dataDownload",
            validation_key="dataDownload_post_run_validation",
            completed_key="total_completed_dataDownload",
            failed_key="total_failed_dataDownload",
        )

    def validate_podvolumebackups(self, scenario):
        self._validate_volume_operations(
            scenario,
            resource_type="podvolumebackup",
            name_label_path="backup",
            cr_name_key="OADP_CR_NAME",
            results_key="podvolumebackups",
            validation_key="podvolumebackup_post_run_validation",
            completed_key="total_completed_pvb",
            failed_key="total_failed_pvb",
        )

    def validate_datauploads(self, scenario):
        self._validate_volume_operations(
            scenario,
            resource_type="DataUploads",
            name_label_path="velero.io/backup-name",
            cr_name_key="OADP_CR_NAME",
            results_key="dataUpload",
            validation_key="dataUpload_post_run_validation",
            completed_key="total_completed_dataUpload",
            failed_key="total_failed_dataUpload",
        )

    @logger_time_stamp
    def parse_oadp_cr(self, ns, cr_type, cr_name):
        try:
            from dateutil import parser as date_parser

            oadp_cr_already_present = self.is_cr_present(ns=ns, cr_type=cr_type, cr_name=cr_name)
            if not oadp_cr_already_present:
                logger.warning(f"Warning no matching cr {cr_name} of type: {cr_type} was found")
            else:
                cr_info = {}
                ssh = self._OadpWorkloads__ssh

                jsonpath_cr_status = "'{.status.phase}'"
                cr_status = ssh.run(cmd=f"oc get {cr_type}/{cr_name} -n {ns} -o jsonpath={jsonpath_cr_status}")
                if cr_status != "":
                    cr_info["cr_status"] = cr_status
                    if cr_status != "Completed":
                        logger.error(f"### parse_oadp_cr: CR status is {cr_status}")
                    else:
                        logger.info(f"### parse_oadp_cr: CR status is {cr_status}")

                jsonpath_cr_kind = "'{.kind}'"
                cr_kind = ssh.run(cmd=f"oc get {cr_type}/{cr_name} -n {ns} -o jsonpath={jsonpath_cr_kind}")
                if cr_kind != "":
                    cr_info["cr_kind"] = cr_kind

                if cr_type == "backup":
                    jsonpath = "'{.status.progress.itemsBackedUp}'"
                    result = ssh.run(cmd=f"oc get {cr_type}/{cr_name} -n {ns} -o jsonpath={jsonpath}")
                    if result != "":
                        cr_info["cr_items_backedup"] = result

                if cr_type == "restore":
                    jsonpath = "'{.status.progress.itemsRestored}'"
                    result = ssh.run(cmd=f"oc get {cr_type}/{cr_name} -n {ns} -o jsonpath={jsonpath}")
                    if result != "":
                        cr_info["cr_items_restored"] = result

                jsonpath_total = "'{.status.progress.totalItems}'"
                cr_items_total = ssh.run(cmd=f"oc get {cr_type}/{cr_name} -n {ns} -o jsonpath={jsonpath_total}")
                if cr_items_total != "":
                    cr_info["cr_items_total"] = cr_items_total

                jsonpath_errors = "'{.status.errors}'"
                cr_errors = ssh.run(cmd=f"oc get {cr_type}/{cr_name} -n {ns} -o jsonpath={jsonpath_errors}")
                if cr_errors != "":
                    cr_info["cr_errors"] = cr_errors

                jsonpath_start = "'{.status.startTimestamp}'"
                cr_start_timestamp = ssh.run(cmd=f"oc get {cr_type}/{cr_name} -n {ns} -o jsonpath={jsonpath_start}")
                if cr_start_timestamp != "":
                    cr_info["cr_start_timestamp"] = cr_start_timestamp

                jsonpath_completion = "'{.status.completionTimestamp}'"
                cr_completion_timestamp = ssh.run(cmd=f"oc get {cr_type}/{cr_name} -n {ns} -o jsonpath={jsonpath_completion}")
                if cr_completion_timestamp != "":
                    cr_info["cr_completion_timestamp"] = cr_completion_timestamp
                    if cr_start_timestamp != "":
                        cr_info["total_duration"] = date_parser.parse(cr_info["cr_completion_timestamp"]) - date_parser.parse(
                            cr_info["cr_start_timestamp"]
                        )

                awk_cr_cluster = "'{print $2}'"
                cr_cluster = ssh.run(cmd=f"oc get route/console -n openshift-console | grep -v NAME | awk {awk_cr_cluster}")
                if cr_cluster != "":
                    cr_info["cr_cluster"] = cr_cluster

                logger.info(f"cr_info is {cr_info}")
                self._OadpWorkloads__run_metadata["summary"]["runtime"]["results"] = {}
                self._OadpWorkloads__run_metadata["summary"]["runtime"]["results"].update(cr_info)
                self._OadpWorkloads__result_dicts.append(self._OadpWorkloads__run_metadata["summary"]["runtime"]["results"])
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    def validate_cr(self, ns, cr_type, cr_name):
        try:
            cr_present = self.is_cr_present(ns=ns, cr_type=cr_type, cr_name=cr_name)
            if not cr_present:
                logger.exception(
                    f"Warning no matching cr {cr_name} of type: {cr_type} was found after backup or restore operation"
                )
            jsonpath_cr_status = "'{.status.phase}'"
            cr_status = self._OadpWorkloads__ssh.run(
                cmd=f"oc get {cr_type}/{cr_name} -n {ns} -o jsonpath={jsonpath_cr_status}"
            )
            if cr_status != "":
                if cr_status not in ("Completed", "PartiallyFailed"):
                    self._OadpWorkloads__run_metadata["summary"]["validations"]["cr_status_post_run_validation"] = "FAIL"
                    logger.exception(
                        f" CR status of {cr_status} was returned in validate_cr please check your timeout value on your test"
                    )
                else:
                    self._OadpWorkloads__run_metadata["summary"]["validations"]["cr_status_post_run_validation"] = "PASS"
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def check_oadp_cr_for_errors_and_warns(self, scenario):
        try:
            cr_type = scenario["args"]["OADP_CR_TYPE"]
            cr_name = scenario["args"]["OADP_CR_NAME"]
            test_env = self._OadpWorkloads__test_env
            ssh = self._OadpWorkloads__ssh
            jsonpath = "'{.status.errors}'"
            error_count = ssh.run(cmd=f"oc get {cr_type}/{cr_name} -n {test_env['velero_ns']} -o jsonpath={jsonpath}")
            if error_count != "" and "Error" not in error_count and int(error_count) > 0:
                oadp_velero_log = os.path.join(self._run_artifacts_path, f"{cr_name}-error-and-warning-summary.log")
                if test_env["source"] == "upstream":
                    warnings_and_errors = ssh.run(
                        cmd=f"cd {test_env['velero_cli_path']}/velero/cmd/velero; "
                        f"./velero {cr_type} logs {cr_name} -n {test_env['velero_ns']} "
                        r"| grep 'warn\|error\|critical\|exception'"
                    )
                    ssh.run(
                        cmd=f"cd {test_env['velero_cli_path']}/velero/cmd/velero; "
                        f"./velero {cr_type} logs {cr_name} -n {test_env['velero_ns']} "
                        rf"| grep 'warn\|error\|critical\|exception' >> {oadp_velero_log}"
                    )
                else:
                    warnings_and_errors = ssh.run(
                        cmd=f"oc -n {test_env['velero_ns']} exec deployment/velero -c velero -it -- "
                        f"./velero {cr_type} logs {cr_name} --insecure-skip-tls-verify "
                        r"| grep 'warn\|error\|critical\|exception'"
                    )
                    ssh.run(
                        cmd=f"oc -n {test_env['velero_ns']} exec deployment/velero -c velero -it -- "
                        f"./velero {cr_type} logs {cr_name} --insecure-skip-tls-verify "
                        rf"| grep 'warn\|error\|critical\|exception' >> {oadp_velero_log}"
                    )
                logger.info(f":: INFO :: validate_oadp_cr :: {error_count} errors and warnings for {cr_name} ")
                logger.warning(f":: WARN :: {cr_name} log showed: {warnings_and_errors}")
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

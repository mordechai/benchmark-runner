"""
Mixin for OADP backup/restore execution and CR wait logic.
"""

from __future__ import annotations

import math
import time

from benchmark_runner.common.logger.logger_time_stamp import logger, logger_time_stamp
from benchmark_runner.oadp.constants import (
    CR_AUTH_ERROR_STATES,
    CR_TERMINAL_STATES,
    DEFAULT_TESTCASE_TIMEOUT,
    FILE_LEVEL_PLUGINS,
    PLUGIN_CSI,
    PLUGIN_VBD,
    SOURCE_UPSTREAM,
)


class OadpExecutionMixin:
    """Backup/restore execution, CR wait, ansible play validation, PV util checks."""

    @logger_time_stamp
    def exec_restore(self, plugin: str, restore_name: str, backup_name: str) -> None:
        """Create a Velero restore CR via oc exec or upstream velero CLI."""
        try:
            test_env = self._OadpWorkloads__test_env
            ssh = self._OadpWorkloads__ssh
            if test_env["source"] != SOURCE_UPSTREAM:
                restore_cmd = ssh.run(
                    cmd=f"oc -n {test_env['velero_ns']} exec deployment/velero -c velero -it -- "
                    f"./velero restore create {restore_name} --from-backup {backup_name} "
                    f"-n {test_env['velero_ns']}"
                )
                logger.info(f"### INFO ### Executing OADP restore with velero CLI in {test_env['velero_ns']}")
            if test_env["source"] == SOURCE_UPSTREAM:
                restore_cmd = ssh.run(
                    cmd=f"cd {test_env['velero_cli_path']}/velero/cmd/velero; "
                    f"./velero restore create {restore_name} --from-backup {backup_name} "
                    f"-n {test_env['velero_ns']}"
                )
                logger.info("### INFO ### Executing UPSTREAM velero restore")
            if "submitted successfully" not in restore_cmd:
                logger.error(f"Error restore did not execute, stdout: {restore_cmd}")
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def exec_backup(self, plugin: str, backup_name: str, namespaces_to_backup: str) -> None:
        """Create a Velero backup with plugin-appropriate flags for downstream or upstream."""
        test_env = self._OadpWorkloads__test_env
        ssh = self._OadpWorkloads__ssh

        if test_env["source"] != SOURCE_UPSTREAM:
            base_cmd = (
                f"oc -n {test_env['velero_ns']} exec deployment/velero -c velero -it -- "
                f"./velero backup create {backup_name} --include-namespaces {namespaces_to_backup}"
            )
            if plugin in FILE_LEVEL_PLUGINS:
                backup_cmd = ssh.run(cmd=f"{base_cmd} --default-volumes-to-fs-backup=true --snapshot-volumes=false")
            elif plugin == PLUGIN_VBD:
                backup_cmd = ssh.run(cmd=f"{base_cmd} --snapshot-move-data=true")
            elif plugin == PLUGIN_CSI:
                backup_cmd = ssh.run(cmd=base_cmd)
            else:
                backup_cmd = ssh.run(cmd=base_cmd)
            if "submitted successfully" not in backup_cmd:
                logger.error(f"Error backup attempt failed stdout from command: {backup_cmd}")

        if test_env["source"] == SOURCE_UPSTREAM:
            base_cmd = (
                f"cd {test_env['velero_cli_path']}/velero/cmd/velero; "
                f"./velero backup create {backup_name} --include-namespaces {namespaces_to_backup}"
            )
            ns_flag = f" -n {test_env['velero_ns']}"
            if plugin in FILE_LEVEL_PLUGINS:
                backup_cmd = ssh.run(cmd=f"{base_cmd} --default-volumes-to-fs-backup=true --snapshot-volumes=false{ns_flag}")
            elif plugin == PLUGIN_CSI:
                backup_cmd = ssh.run(cmd=f"{base_cmd}{ns_flag}")
            elif plugin == PLUGIN_VBD:
                backup_cmd = ssh.run(cmd=f"{base_cmd} --data-mover 'velero' --snapshot-move-data=true{ns_flag}")
            else:
                backup_cmd = ssh.run(cmd=f"{base_cmd}{ns_flag}")
            if "submitted successfully" not in backup_cmd:
                logger.error(f"Error backup attempt failed stdout from command: {backup_cmd}")

    @logger_time_stamp
    def wait_for_condition_of_cr(
        self,
        cr_type: str,
        cr_name: str,
        testcase_timeout: int = DEFAULT_TESTCASE_TIMEOUT,
    ) -> bool | None:
        """Poll backup or restore CR phase until terminal state, error, or timeout."""
        try:
            test_env = self._OadpWorkloads__test_env
            ssh = self._OadpWorkloads__ssh
            if not self.is_cr_present(ns=test_env["velero_ns"], cr_type=cr_type, cr_name=cr_name):
                logger.info(f"{cr_name} OADPWaitForConditionTimeout raised an exception in is_oadp_cr_present returned false")
            jsonpath = "'{.status.phase}'"
            try:
                current_wait_time = 0
                while current_wait_time <= testcase_timeout:
                    state = ssh.run(cmd=f"oc get {cr_type}/{cr_name} -n {test_env['velero_ns']} -o jsonpath={jsonpath}")
                    if state in CR_TERMINAL_STATES:
                        logger.info(f"::: INFO ::: wait_for_condition_of_oadp_cr: CR {cr_name} state: {state}")
                        return True
                    if state in CR_AUTH_ERROR_STATES:
                        logger.warning("### Warning ### Unauthorized error detected during CR poll, issuing login attempt ")
                        logged_in = self.oc_log_in()
                        if not logged_in:
                            logger.error(
                                f":: ERROR :: wait_for_condition_of_oadp_cr: re-auth failed, CR {cr_name} state: {state}"
                            )
                            return False
                        state = ssh.run(cmd=f"oc get {cr_type}/{cr_name} -n {test_env['velero_ns']} -o jsonpath={jsonpath}")
                        if state in CR_TERMINAL_STATES:
                            return True

                    if "Error from server" in state:
                        logger.error(f":: ERROR :: wait_for_condition_of_oadp_cr: CR {cr_name} state: {state}")
                        return False
                    else:
                        logger.info(f"::: INFO ::: wait_for_condition_of_oadp_cr: {state} meaning its still running")
                        time.sleep(3)
                    current_wait_time += 3
            except Exception:
                logger.info(f"{cr_name} OADPWaitForConditionTimeout raised an exception")
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def oadp_execute_scenario(self, test_scenario: dict, run_method: str) -> None:
        """Run the scenario via ansible playbook or Python backup/restore and CR wait."""
        try:
            namespaces_to_backup = self.ds_get_all_namespaces()
            if len(namespaces_to_backup) > 1:
                namespaces_to_backup = ",".join(namespaces_to_backup)
            if len(namespaces_to_backup) == 1:
                namespaces_to_backup = str(namespaces_to_backup[0])
            logger.info(f"### INFO ### oadp_execute_scenario: Namespaces involved: {namespaces_to_backup} ")
            if run_method == "ansible":
                ansible_args = (
                    f"test=1 testcase={test_scenario['testcase']} "
                    f"plugin={test_scenario['args']['plugin']} "
                    f"use_cli={test_scenario['args']['use_cli']} "
                    f"OADP_CR_TYPE={test_scenario['args']['OADP_CR_TYPE']} "
                    f"OADP_CR_NAME={test_scenario['args']['OADP_CR_NAME']} "
                    f"backup_name={test_scenario['args']['backup_name']} "
                    f"namespaces_to_backup={namespaces_to_backup} "
                    f"result_dir_base_path={test_scenario['result_dir_base_path']}"
                )
                self._OadpWorkloads__ssh.run(
                    cmd=f'ansible-playbook {self._OadpWorkloads__oadp_base_dir}/test-oadp.yaml -e "{ansible_args}" -vv'
                )

            if run_method == "python":
                cr_type = test_scenario["args"]["OADP_CR_TYPE"]
                cr_name = test_scenario["args"]["OADP_CR_NAME"]
                if cr_type == "backup":
                    self.oadp_timer(action="start", transaction_name=cr_name)
                    self.exec_backup(
                        plugin=test_scenario["args"]["plugin"],
                        backup_name=test_scenario["args"]["backup_name"],
                        namespaces_to_backup=namespaces_to_backup,
                    )
                    self.wait_for_condition_of_cr(
                        cr_type=cr_type,
                        cr_name=cr_name,
                        testcase_timeout=test_scenario["args"]["testcase_timeout"],
                    )
                    self.oadp_timer(action="stop", transaction_name=cr_name)
                if cr_type == "restore":
                    self.oadp_timer(action="start", transaction_name=cr_name)
                    self.exec_restore(
                        plugin=test_scenario["args"]["plugin"],
                        restore_name=cr_name,
                        backup_name=test_scenario["args"]["backup_name"],
                    )
                    self.wait_for_condition_of_cr(
                        cr_type=cr_type,
                        cr_name=cr_name,
                        testcase_timeout=test_scenario["args"]["testcase_timeout"],
                    )
                    self.oadp_timer(action="stop", transaction_name=cr_name)
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def validate_ansible_play(self, playbook_output: str) -> bool:
        """Return True if ansible-playbook stdout shows zero failed and unreachable hosts."""
        try:
            if playbook_output == "":
                logger.exception("ansible-playbook stdout was empty and should not have been")
            import re as re_module

            failed_count = re_module.search(r"failed=(\d+)", playbook_output)
            unreachable_count = re_module.search(r"unreachable=(\d+)", playbook_output)
            if failed_count is None or unreachable_count is None:
                logger.error(f"ansible-playbook stdout did not contain expected values: {playbook_output}")
                return False
            if int(failed_count.group(1)) == 0 and int(unreachable_count.group(1)) == 0:
                logger.info("ansible-playbook output ran without failures or unreachable errors")
                return True
            logger.exception(f"ansible-playbook stdout failures/unreachable: {playbook_output}")
            return False
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def wait_until_process_inside_pod_completes(
        self,
        pod_name: str,
        namespace: str,
        process_text_to_monitor: str,
        timeout_value: int,
    ) -> bool | None:
        """Wait until pgrep shows no matching process in the pod or timeout is reached."""
        try:
            self._oc.wait_for_pod_ready(pod_name=pod_name, namespace=namespace)
            ssh = self._OadpWorkloads__ssh
            current_wait_time = 0
            while current_wait_time <= int(timeout_value):
                status_cmd = ssh.run(
                    cmd=f"oc exec -n{namespace} {pod_name} -- /bin/bash -c 'pgrep -flc {process_text_to_monitor}'"
                )
                status_cmd_value = status_cmd.split("\n")[0]
                logger.info(
                    f":: INFO :: wait_until_process_inside_pod_completes: returned "
                    f"{status_cmd_value}, remaining time: {int(timeout_value) - current_wait_time}"
                )
                disk_capacity = ssh.run(
                    cmd=f'oc exec -i -n{namespace} {pod_name} -- /bin/bash -c "du -sh {process_text_to_monitor}"'
                )
                if int(status_cmd_value) == 0:
                    logger.info(
                        f":: INFO :: wait_until_process_inside_pod_completes: process completed, size of data: {disk_capacity}"
                    )
                    return True
                else:
                    logger.info(
                        f":: INFO :: wait_until_process_inside_pod_completes: process STILL running, "
                        f"current size: {disk_capacity}"
                    )
                    time.sleep(10)
                current_wait_time += 10
        except Exception as err:
            logger.info(f"Error: {err} in wait_until_process_inside_pod_completes pod {pod_name}")
            return False

    @logger_time_stamp
    def get_pod_pv_utilization_info_by_podname(self, podname: str, ds: dict) -> dict:
        """Collect disk usage, file count, and folder count for a pod mount from the dataset spec."""
        results_capacity_usage = {}
        active_role = ds["role"]
        mount_point = ds["dataset_path"]
        namespace = ds["namespace"]

        try:
            disk_capacity_cmd = f"oc exec -n {namespace} {podname} -- /bin/bash -c 'du -sh {mount_point}'"
            disk_capacity = self.execute_with_retries(disk_capacity_cmd)
            if disk_capacity is not None:
                results_capacity_usage["disk_capacity"] = disk_capacity.split("\n")[-1].split("\t")[0] if disk_capacity else 0
            else:
                results_capacity_usage["disk_capacity"] = 0

            files_count_cmd = (
                f"oc exec -n {namespace} {podname} -- /bin/bash -c "
                f'\'find {mount_point}* -type f -name "my-random-file-*" -o -name "dd_file" | wc -l\''
            )
            files_count = self.execute_with_retries(files_count_cmd)
            if files_count is not None:
                results_capacity_usage["files_count"] = files_count.split("\n")[-1].split("\t")[0] if files_count else 0
            else:
                results_capacity_usage["files_count"] = 0

            folders_count_cmd = (
                f"oc exec -n {namespace} {podname} -- /bin/bash -c 'find {mount_point}python/* -type d | wc -l'"
            )
            folders_count = self.execute_with_retries(folders_count_cmd)
            if folders_count is not None:
                results_capacity_usage["folders_count"] = folders_count.split("\n")[-1].split("\t")[0] if folders_count else 0
            else:
                results_capacity_usage["folders_count"] = None

            results_capacity_usage["active_role"] = active_role
            logger.info(f"get_pod_pv_utilization_info saw pv contained: {results_capacity_usage}")
            return results_capacity_usage

        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def get_pod_pv_utilization_info(self, test_scenario: dict, ds: dict) -> dict:
        """Sample PV utilization from the first pod in the backup namespace for the dataset role."""
        results_capacity_usage = {}
        active_role = ds["role"]
        mount_point = ds["dataset_path"]
        namespace = test_scenario["args"]["namespaces_to_backup"]
        ssh = self._OadpWorkloads__ssh
        podname = ssh.run(cmd=f"oc get pods -o custom-columns=POD:.metadata.name --no-headers -n{namespace}")
        disk_capacity = ssh.run(cmd=f'oc exec -it -n{namespace} {podname} -- /bin/bash -c "du -sh {mount_point}"')
        results_capacity_usage["disk_capacity"] = disk_capacity.split("\n")[-1].split("\t")[0]
        files_count = ssh.run(
            cmd=f"oc exec -it -n{namespace} {podname} -- /bin/bash -c "
            f'"find {mount_point}* -type f -name \\"my-random-file-*\\" -o -name \\"dd_file\\" |wc -l"'
        )
        results_capacity_usage["files_count"] = files_count.split("\n")[-1].split("\t")[0]
        folders_count = ssh.run(
            cmd=f'oc exec -it -n{namespace} {podname} -- /bin/bash -c "find {mount_point}python/* -type d |wc -l"'
        )
        results_capacity_usage["folders_count"] = folders_count.split("\n")[-1].split("\t")[0]
        results_capacity_usage["active_role"] = active_role
        logger.info(f"get_pod_pv_utilization_info saw pv contained: {results_capacity_usage}")
        return results_capacity_usage

    @logger_time_stamp
    def pv_contains_expected_data(self, pv_util_details_returned_by_pod: dict, ds: dict) -> bool:
        """Compare reported PV stats to expected capacity, file count, and folder count from the dataset."""
        if pv_util_details_returned_by_pod["disk_capacity"] != ds["expected_capacity"]:
            logger.warning(
                f":: ERROR :: pv_contains_expected_data disk_capacity FAILED comparison "
                f"pod returned: {pv_util_details_returned_by_pod['disk_capacity']} "
                f"yaml expected: {ds['expected_capacity']}"
            )
            return False
        total_by_folder = int(ds["files_count"]) * ds["dir_count"]
        total_raw = int(ds["files_count"])
        actual = int(pv_util_details_returned_by_pod["files_count"])
        if total_by_folder != actual and total_raw != actual:
            logger.warning(
                f":: ERROR :: pv_contains_expected_data files_count FAILED comparison "
                f"pod returned: {actual} yaml expected: {ds['files_count']}"
            )
            return False
        if int(pv_util_details_returned_by_pod["folders_count"]) != ds["dir_count"]:
            logger.warning(
                f":: ERROR :: pv_contains_expected_data folders_count failed comparison "
                f"pod returned: {pv_util_details_returned_by_pod['folders_count']} "
                f"yaml expected: {ds['dir_count']}"
            )
            return False
        logger.info(":: INFO :: pv_contains_expected_data is returning True")
        return True

    @logger_time_stamp
    def get_expected_files_count(self, test_scenario: dict, ds: dict) -> dict:
        """Compute expected disk usage and counts for generator role datasets (dd_generator leaves dict sparse)."""
        results_capacity_expected = {}
        active_role = ds["role"]
        dir_count = ds["dir_count"]
        files_count = ds["files_count"]
        dept_count = ds["dept_count"]
        file_size = ds["files_size"]
        if active_role == "generator":
            countdir = 0
            for k in range(dept_count):
                countdir += math.pow(dept_count, k)
            countfile = int(math.pow(dept_count, (dept_count - 1)) * files_count)
            total_files = int(countfile * dir_count)
            total_folders = int(countdir * dir_count)
            disk_capacity = round(int(total_files * file_size) / 1024 / 1024)
            results_capacity_expected["disk_capacity"] = disk_capacity
            results_capacity_expected["files_count"] = total_files
            results_capacity_expected["folders_count"] = total_folders
        elif active_role == "dd_generator":
            pass
        return results_capacity_expected

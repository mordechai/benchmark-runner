"""
Mixin for pod, namespace, dataset, and cluster-level validation.
"""

import json
import random
import time

from benchmark_runner.common.logger.logger_time_stamp import logger, logger_time_stamp
from benchmark_runner.oadp.constants import VALIDATION_MODE_FULL, VALIDATION_MODE_LIGHT, VALIDATION_MODE_NONE


class OadpPodValidationMixin:
    """Pod lifecycle, dataset, and cluster operator validation."""

    @logger_time_stamp
    def verify_pod_pv_size_and_sc(self, podName, expected_ns, expected_sc, expected_size):
        ssh = self._OadpWorkloads__ssh
        query = '"{.spec.volumes[0].persistentVolumeClaim.claimName}"'
        pvc_name = ssh.run(cmd=f"oc get pod {podName} -n {expected_ns} -o jsonpath={query}")
        if pvc_name.find("Error") > 0 and pvc_name != "":
            return False
        query = "'{.spec.storageClassName} {.spec.resources.requests.storage}'"
        cmd_get_pvc = ssh.run(cmd=f"oc get pvc {pvc_name} -n {expected_ns} -o jsonpath={query}")
        current_sc = cmd_get_pvc.split(" ")[0]
        current_size = cmd_get_pvc.split(" ")[1]
        if current_sc != expected_sc:
            logger.warning(f"current pv storage class used: {current_sc} doesnt match expected: {expected_sc}")
            return False
        if current_size != expected_size:
            logger.warning(f"current pv size: {current_size} doesnt match expected: {expected_size}")
            return False
        logger.info(f"pod: {podName} in ns: {expected_ns} matches desired storage and pv size")
        return True

    @logger_time_stamp
    def verify_pod_restarts(self, target_namespace):
        try:
            ssh = self._OadpWorkloads__ssh
            run_metadata = self._OadpWorkloads__run_metadata
            restart_query = "'.items[] | select(.status.containerStatuses[].restartCount > 0) | .metadata.name'"
            pods_restarted_cmd = ssh.run(cmd=f"oc get pods -n {target_namespace} -o json | jq -r {restart_query}")
            if pods_restarted_cmd != "":
                run_metadata["summary"]["results"]["pod_restarts_post_run_validation"] = {}
                run_metadata["summary"]["results"]["pod_restarts_post_run_validation"]["status"] = False
                run_metadata["summary"]["validations"]["verify_no_pod_restarts_in_velero_ns"] = "FAIL"
                get_pod_details = ssh.run(cmd=f"oc get pods -n {target_namespace} -o json")
                data = json.loads(get_pod_details)
                pods_that_restarted = {}
                for pod in data["items"]:
                    if pod["status"]["containerStatuses"][0]["restartCount"] > 0:
                        name = pod["metadata"]["name"]
                        pods_that_restarted[name] = pod["status"]["containerStatuses"][0]["restartCount"]
                run_metadata["summary"]["results"]["pod_restarts_post_run_validation"]["restarted_pods"] = {}
                run_metadata["summary"]["results"]["pod_restarts_post_run_validation"]["restarted_pods"].update(
                    pods_that_restarted
                )
            else:
                run_metadata["summary"]["results"]["pod_restarts_post_run_validation"] = {}
                run_metadata["summary"]["results"]["pod_restarts_post_run_validation"]["status"] = True
                run_metadata["summary"]["validations"]["verify_no_pod_restarts_in_velero_ns"] = "PASS"
                run_metadata["summary"]["results"]["pod_restarts_post_run_validation"]["restarted_pods"] = {}
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def verify_cluster_operators_status(self):
        try:
            ssh = self._OadpWorkloads__ssh
            run_metadata = self._OadpWorkloads__run_metadata
            get_co_status = ssh.run(cmd="oc get co -o json")
            if get_co_status != "":
                run_metadata["summary"]["results"]["cluster_operator_post_run_validation"] = {}
                data = json.loads(get_co_status)
                co_degraded = {}
                co_not_available = {}
                for co in data["items"]:
                    for s in co["status"]["conditions"]:
                        if s["type"] == "Available" and s["status"] == "False":
                            co_not_available[co["metadata"]["name"]] = s["status"]
                        if s["type"] == "Degraded" and s["status"] == "True":
                            co_degraded[co["metadata"]["name"]] = s["status"]
                run_metadata["summary"]["results"]["cluster_operator_post_run_validation"]["unavailable"] = {}
                if co_not_available:
                    run_metadata["summary"]["results"]["cluster_operator_post_run_validation"]["unavailable"].update(
                        co_not_available
                    )
                run_metadata["summary"]["results"]["cluster_operator_post_run_validation"]["degraded"] = {}
                if co_degraded:
                    run_metadata["summary"]["results"]["cluster_operator_post_run_validation"]["degraded"].update(co_degraded)
                if not co_not_available and not co_degraded:
                    run_metadata["summary"]["validations"]["cluster_operator_post_run_validation"] = "PASS"
                else:
                    run_metadata["summary"]["validations"]["cluster_operator_post_run_validation"] = "Fail"
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def is_num_of_results_valid(self, expected_size, percentage_of_expected_size, list_of_values):
        if len(list_of_values) == expected_size:
            logger.info(f":: INFO :: {len(list_of_values)} matches total {expected_size}")
            return True
        elif percentage_of_expected_size[-1] == "%":
            percentage = int(percentage_of_expected_size[:-1])
            if len(list_of_values) >= (percentage / 100) * expected_size:
                return True
        return False

    @logger_time_stamp
    def compare_dicts(self, dict1, dict2):
        if set(dict1.keys()) != set(dict2.keys()):
            return False
        for key in dict1:
            if dict1[key] != dict2[key]:
                return False
        return True

    @logger_time_stamp
    def get_pod_prefix_by_dataset_role(self, ds):
        if ds is None:
            return None
        expected_pod_name = ""
        if ds["role"] in ("generator", "dd_generator"):
            expected_pod_name = f"{ds['pv_size']}-{ds['pods_per_ns']}"
            expected_pod_name = expected_pod_name.replace(".", "-").lower()
        if ds["role"] == "BusyBoxPodSingleNS.sh":
            expected_pod_name = "busybox-perf"
        if expected_pod_name == "":
            logger.exception(" get_pod_prefix_by_dataset_role not setting expected_pod_name its empty ")
        logger.info(f"### get_pod_prefix_by_dataset_role returns {expected_pod_name}")
        return expected_pod_name

    @logger_time_stamp
    def get_status_of_pods_by_ns(self, scenario, ds):
        target_namespace = ds["namespace"]
        ns_state = {}
        for state in [
            "Running",
            "Completed",
            "CrashLoopBackOff",
            "Error",
            "Pending",
            "ContainerCreating",
            "Terminating",
            "ImagePullBackOff",
            "Init",
            "Unknown",
        ]:
            ns_state[state] = len(
                self.get_list_of_pods_by_status(namespace=target_namespace, query_operator="=", status=state)
            )
        logger.info(f":: INFO :: get_status_of_pods_by_ns shows that ns {target_namespace} has {ns_state}")
        return ns_state

    @logger_time_stamp
    def verify_pods_are_progressing(self, scenario, interval_between_checks=15, max_attempts=1, ds=None):
        num_of_pods_expected = ds["pods_per_ns"]
        target_namespace = ds["namespace"]
        try:
            running_pods = self.get_list_of_pods_by_status(namespace=target_namespace, query_operator="=", status="Running")
            expected_pod_name = self.get_pod_prefix_by_dataset_role(ds)
            if expected_pod_name is not None:
                running_pods = [element for element in running_pods if expected_pod_name in element]
            if len(running_pods) == num_of_pods_expected:
                return False
            total_attempts = 0
            while total_attempts < max_attempts:
                status_of_pods = self.get_status_of_pods_by_ns(scenario, ds)
                time.sleep(int(interval_between_checks))
                current_status_of_pods = self.get_status_of_pods_by_ns(scenario, ds)
                are_same = self.compare_dicts(status_of_pods, current_status_of_pods)
                running_increased = current_status_of_pods["Running"] > status_of_pods["Running"]
                at_desired = current_status_of_pods["Running"] == num_of_pods_expected
                divisible_by_500 = current_status_of_pods["Running"] % 500 == 0

                if at_desired:
                    logger.info(":: INFO :: verify_pods_are_progressing: pods at desired state")
                    return True
                if not are_same and running_increased:
                    logger.info(":: INFO :: verify_pods_are_progressing: pods are progressing")
                    return True
                if (
                    divisible_by_500
                    and current_status_of_pods["Pending"] == 0
                    and current_status_of_pods["Error"] == 0
                    and not running_increased
                    and are_same
                ):
                    logger.info(":: INFO :: verify_pods_are_progressing: batch likely complete")
                    return False
                else:
                    logger.info(
                        f":: INFO :: verify_pods_are_progressing: pods NOT progressing "
                        f"previously: {status_of_pods} currently: {current_status_of_pods}"
                    )
                total_attempts += 1
            return False
        except Exception as err:
            logger.warning(f":: WARN :: in verify_pods_are_progressing raised an exception {err}")

    @logger_time_stamp
    def waiting_for_ns_to_reach_desired_pods(self, scenario, ds):
        num_of_pods_expected = ds["pods_per_ns"]
        target_namespace = ds["namespace"]
        timeout_value = int(scenario["args"]["testcase_timeout"])
        try:
            running_pods = self.get_list_of_pods_by_status(namespace=target_namespace, query_operator="=", status="Running")
            expected_pod_name = self.get_pod_prefix_by_dataset_role(ds)
            if expected_pod_name is None:
                logger.error("### ERROR ### waiting_for_ns_to_reach_desired_pods: Bad Pod Prefix")
            running_pods = [element for element in running_pods if expected_pod_name in element]
            logger.info(
                f":: INFO :: waiting_for_ns_to_reach_desired_pods: {target_namespace} has "
                f"{len(running_pods)} in running state, out of total desired: {num_of_pods_expected}"
            )

            if len(running_pods) == num_of_pods_expected:
                return True
            if len(running_pods) > num_of_pods_expected:
                logger.warning(
                    f":: WARN :: waiting_for_ns_to_reach_desired_pods: {target_namespace} has "
                    f"{len(running_pods)} in running state which is MORE than total desired: "
                    f"{num_of_pods_expected}"
                )
                return False

            retry_logic = self._OadpWorkloads__retry_logic
            pods_progressing = self.verify_pods_are_progressing(
                scenario=scenario,
                interval_between_checks=retry_logic["interval_between_checks"],
                max_attempts=retry_logic["max_attempts"],
                ds=ds,
            )
            if not pods_progressing:
                return False

            current_wait_time = 0
            while current_wait_time <= int(timeout_value):
                running_pods = self.get_list_of_pods_by_status(
                    namespace=target_namespace, query_operator="=", status="Running"
                )
                expected_pod_name = self.get_pod_prefix_by_dataset_role(ds)
                if expected_pod_name is None:
                    logger.error("### ERROR ### waiting_for_ns_to_reach_desired_pods: Bad Pod Prefix")
                running_pods = [element for element in running_pods if expected_pod_name in element]
                if len(running_pods) == num_of_pods_expected:
                    return True
                else:
                    time.sleep(3)
                current_wait_time += 3
        except Exception as err:
            logger.warning(f"Error in waiting_for_ns_to_reach_desired_pods {err} time out waiting to reach desired state")

    @logger_time_stamp
    def verify_running_pods(self, num_of_pods_expected, target_namespace):
        running_pods = self.get_list_of_pods_by_status(namespace=target_namespace, query_operator="=", status="Running")
        logger.info(
            f":: INFO :: verify_running_pods: {target_namespace} has {len(running_pods)} running, "
            f"desired: {num_of_pods_expected}"
        )
        if len(running_pods) == num_of_pods_expected:
            return True
        pods_not_yet_in_run_status = self.get_list_of_pods_by_status(
            namespace=target_namespace, query_operator="!=", status="Running"
        )
        if len(pods_not_yet_in_run_status) > 0:
            for pod in pods_not_yet_in_run_status:
                self._oc.wait_for_pod_ready(pod, target_namespace)
            running_pods = self.get_list_of_pods_by_status(namespace=target_namespace, query_operator="=", status="Running")
            if len(running_pods) == num_of_pods_expected:
                return True
            logger.warning(
                f":: WARNING :: verify_running_pods: {target_namespace} has {len(running_pods)} "
                f"running, expected {num_of_pods_expected}"
            )
            return False
        running_pods = self.get_list_of_pods_by_status(namespace=target_namespace, query_operator="=", status="Running")
        if len(running_pods) == num_of_pods_expected:
            return True
        logger.warning(
            f":: WARNING :: verify_running_pods: {target_namespace} has {len(running_pods)} "
            f"running, expected {num_of_pods_expected}"
        )
        return False

    def get_pods_by_ns_by_expected_name(self, ds):
        running_pods = self.get_list_of_pods(namespace=ds["namespace"])
        expected_pod_name = self.get_pod_prefix_by_dataset_role(ds)
        if expected_pod_name is None:
            logger.error("### ERROR ### get_pods_by_ns_by_expected_name: Bad Pod Prefix")
            logger.exception("get_pods_by_ns_by_expected_name: throwing exception")
        return [element for element in running_pods if expected_pod_name in element]

    @logger_time_stamp
    def validate_expected_datasets(self, scenario):
        try:
            dataset_value = scenario.get("dataset")
            if isinstance(dataset_value, list):
                logger.info("### INFO ### validate_expected_datasets identified dataset in list format")
                total_ds = len(scenario["dataset"])
                validated = 0
                validations_attempted = 0
                for ds in scenario["dataset"]:
                    logger.info(
                        f"### INFO ### validate_expected_datasets attempting to validate "
                        f"dataset {validations_attempted} of {total_ds} "
                    )
                    validation_status = self.validate_dataset(scenario, ds)
                    if validation_status:
                        validated += 1
                    else:
                        self._OadpWorkloads__oadp_ds_failing_validation.append(ds)
                        logger.warning(f"### WARN ### DS validation was not successful for ns {ds['namespace']}")
                    validations_attempted += 1
                if validated != total_ds:
                    logger.warning(
                        f"### ERROR ### validate_expected_datasets: Datasets validated: {validated} expected: {total_ds}"
                    )
                    return False
                logger.info(
                    f"### INFO ### validate_expected_datasets: Validation completed successfully for {validated} datasets"
                )
                return True
            else:
                ds = scenario["dataset"]
                ds["namespace"] = scenario["args"]["namespaces_to_backup"]
                validation_status = self.validate_dataset(scenario, ds)
                if not validation_status:
                    self._OadpWorkloads__oadp_ds_failing_validation.append(ds)
                return validation_status
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def validate_dataset(self, scenario, ds):
        num_of_pods_expected = ds["pods_per_ns"]
        target_namespace = ds["namespace"]
        role = ds["role"]
        dataset_validation_mode = self.get_dataset_validation_mode(ds)

        pod_presence_and_storage_as_expected = False
        reattempts = 0
        while not pod_presence_and_storage_as_expected and reattempts < 5:
            if dataset_validation_mode == VALIDATION_MODE_FULL:
                pod_presence_and_storage_as_expected = self.verify_pod_presence_and_storage(scenario, ds)
                if not pod_presence_and_storage_as_expected:
                    logger.warning(f"validate_dataset: using {dataset_validation_mode} validation returning FALSE")
                    return False
            if dataset_validation_mode == VALIDATION_MODE_LIGHT:
                pod_presence_and_storage_as_expected = self.waiting_for_ns_to_reach_desired_pods(scenario, ds)
                if not pod_presence_and_storage_as_expected:
                    logger.warning(f"validate_dataset using {dataset_validation_mode} returning false")
                    return False
                else:
                    logger.info(
                        " ## INFO ## Validation checking for pod_presence_and_storage_as_expected - pods are in correct state"
                    )
            if dataset_validation_mode == VALIDATION_MODE_NONE:
                logger.warning("validate_dataset is set to none - NO validation will be performed")
            reattempts += 1

        if role == "generator":
            all_pods = self.get_pods_by_ns_by_expected_name(ds)
            pods_to_validate = []
            if dataset_validation_mode == VALIDATION_MODE_NONE:
                logger.warning("validate_dataset: set to none - NO validation performed")
                return True
            if dataset_validation_mode == VALIDATION_MODE_FULL:
                pods_to_validate = all_pods
            if dataset_validation_mode == VALIDATION_MODE_LIGHT:
                pods_to_validate = self.get_reduced_list(original_list=all_pods)
                logger.info(
                    f"*** INFO *** Validation Mode: light will validate 10% of randomly "
                    f"selected pods: {len(pods_to_validate)} of total {num_of_pods_expected}"
                )
            if len(pods_to_validate) == 0:
                logger.error(f"No running pods were found in ns {target_namespace} expected {num_of_pods_expected}")
            for pod in pods_to_validate:
                pv_util_details = self.get_pod_pv_utilization_info_by_podname(pod, ds)
                pv_contents_as_expected = self.pv_contains_expected_data(pv_util_details, ds)
                if not pv_contents_as_expected:
                    logger.error(f"::: Validation - PV Contents not successful for pod: {pod} in ns {target_namespace}")
                    return False
                else:
                    logger.info(f"::: Validation - PV Contents successful for pod: {pod} in ns {target_namespace}")
        return True

    @logger_time_stamp
    def get_reduced_list(self, original_list):
        if not original_list:
            return []
        reduced_list_size = max(1, int(len(original_list) * 0.1))
        if reduced_list_size >= len(original_list):
            return original_list
        reduced_list_indexes = random.sample(range(len(original_list)), reduced_list_size)
        return [original_list[i] for i in reduced_list_indexes]

    def set_validation_retry_logic(self, interval_between_checks, max_attempts):
        try:
            self._OadpWorkloads__retry_logic = {
                "interval_between_checks": interval_between_checks,
                "max_attempts": max_attempts,
            }
            logger.info(
                f"### INFO ### set_validation_retry_logic: Set interval: "
                f"{interval_between_checks} max_attempts: {max_attempts}"
            )
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

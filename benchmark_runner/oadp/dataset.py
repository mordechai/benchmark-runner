"""
Mixin for OADP dataset creation, deletion, and verification.
"""

from __future__ import annotations

import os

from benchmark_runner.common.logger.logger_time_stamp import logger, logger_time_stamp
from benchmark_runner.oadp.constants import VM_DATASET_ROLE


class OadpDatasetMixin:
    """Dataset creation, deletion, verification, PV utilities."""

    @logger_time_stamp
    def print_all_ds(self, scenario: dict) -> None:
        """Log every loaded dataset per namespace for the scenario."""
        if not self._OadpWorkloads__scenario_datasets:
            logger.error(
                f"### ERROR ### Datasets are either not present or were not parsed from the "
                f"{scenario.get('name', 'unknown')} correctly as self.__scenario_datasets is empty"
            )
            logger.exception("Please check your datasets defined in your yaml as no datasets were loaded")

        for _ns_count, ns in enumerate(self.ds_get_all_namespaces()):
            self.log_this(
                level="INFO",
                msg=f" Total Datasets related to namespace {ns} are: ",
                obj_to_json=self.ds_get_total_datasets_for_namespace(namespace=ns),
            )
            for ds_count, ds in enumerate(self.ds_get_datasets_for_namespace(namespace=ns)):
                self.log_this(
                    level="INFO",
                    msg=f"Namespace: {ns} Dataset #{ds_count + 1} has total "
                    f"{ds['pods_per_ns']} pods in ns {ns} detailed info: ",
                    obj_to_json=ds,
                )

    @logger_time_stamp
    def create_all_datasets(self, scenario: dict) -> None:
        """Create source datasets for every namespace defined in the scenario."""
        if not self._OadpWorkloads__scenario_datasets:
            logger.error(
                "### ERROR ### Datasets are either not present or were not parsed correctly "
                "as self.__scenario_datasets is empty"
            )
            logger.exception("Please check your datasets defined in your yaml as no datasets were loaded")
        for ns in self.ds_get_all_namespaces():
            for ds in self.ds_get_datasets_for_namespace(namespace=ns):
                logger.info(f"create_dataset will begin creating dataset for ns {ns} that has a total datasets of: {ds}")
                self.create_source_dataset(scenario, ds)
                logger.info(
                    f"### INFO ### create_dataset completed adding pods of type {ds['role']} "
                    f"that created {ds['pods_per_ns']} pods in ns {ns}"
                )

    @logger_time_stamp
    def create_source_dataset(self, scenario: dict, ds: dict | None = None) -> None:
        """Dispatch dataset creation by role (busybox or generator variants)."""
        try:
            if not ds:
                logger.error(
                    "### ERROR ### create_source_dataset is showing ds as empty meaning "
                    "datasets are either not present or were not parsed correctly"
                )
                logger.exception("create_source_dataset: Please check your datasets defined in your yaml")
            self._OadpWorkloads__retry_logic = {
                "interval_between_checks": 15,
                "max_attempts": 20,
            }
            logger.info(f"### INFO ### create_source_dataset: ds is {ds}")
            if ds["role"] == "BusyBoxPodSingleNS.sh":
                self.busybox_dataset_creation(scenario, ds)
            elif ds["role"] in ("generator", "dd_generator"):
                self.create_multi_pvutil_dataset(scenario, ds)
            elif ds["role"] == VM_DATASET_ROLE:
                self.create_vm_dataset(scenario, ds)
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def busybox_dataset_creation(self, scenario: dict, ds: dict) -> None:
        """Run busybox shell scripts in batches to populate pods until the target count is reached."""
        logger.info(f"### INFO ### busybox_dataset_creation: ds is {ds}")
        num_of_assets_desired = ds["pods_per_ns"]
        target_namespace = ds["namespace"]
        pv_size = ds["pv_size"]
        storage = ds["sc"]
        role = ds["role"]
        dataset_creation_log = os.path.join(self._run_artifacts_path, "dataset_creation.log")
        self.oadp_timer(action="start", transaction_name="dataset_creation")
        start = 1
        end = 10
        count = 0
        oadp_path = self._OadpWorkloads__oadp_path
        retry_logic = self._OadpWorkloads__retry_logic

        while start <= num_of_assets_desired:
            if end > num_of_assets_desired:
                end = num_of_assets_desired
            logger.info(
                f":: INFO :: busybox_dataset_creation: executing {oadp_path}/{role} "
                f"{num_of_assets_desired} {pv_size} {storage} '' {start} {end}"
            )
            self._OadpWorkloads__ssh.run(
                cmd=f"{oadp_path}/{role} {num_of_assets_desired} {pv_size} {storage} "
                f"{target_namespace} {start} {end} >> {dataset_creation_log}",
                background=True,
            )
            start += 10
            end += 10
            count += 1

            if count % 50 == 0:
                logger.info(":: INFO :: busybox_dataset_creation: loop has executed 50 instances sleeping for 6 min")
                pods_progressing = True
                while pods_progressing:
                    pods_progressing = self.verify_pods_are_progressing(
                        scenario=scenario,
                        interval_between_checks=retry_logic["interval_between_checks"],
                        max_attempts=retry_logic["max_attempts"],
                        ds=ds,
                    )
                    logger.info(
                        ":: INFO :: busybox_dataset_creation: pods are no longer progressing "
                        "next batch can start if applicable"
                    )

        self.oadp_timer(action="stop", transaction_name="dataset_creation")
        pods_ready = self.waiting_for_ns_to_reach_desired_pods(scenario=scenario, ds=ds)
        if not pods_ready:
            logger.error(f"::: ERROR :: busybox_dataset_creation - expected pods running should be {num_of_assets_desired}")
        else:
            logger.info(f"::: INFO :: busybox_dataset_creation - successfully created {num_of_assets_desired}")

    @logger_time_stamp
    def create_multi_pvutil_dataset(self, test_scenario: dict, ds: dict) -> None:
        """Create per-pod PV data via ansible for generator or dd_generator dataset roles."""
        active_role = ds["role"]
        playbook_path = ds["playbook_path"]
        pvc_size = ds["pv_size"]
        dataset_path = ds["dataset_path"]
        num_of_pods_expected = ds["pods_per_ns"]
        create_data_ansible_output_responses = []
        mount_point = ds["dataset_path"]
        namespace = ds["namespace"]
        sc = ds["sc"]
        testcase_timeout = int(test_scenario["args"]["testcase_timeout"])
        generated_name = str(test_scenario["testcase"]) + f"-{pvc_size}-{num_of_pods_expected}"
        generated_name = generated_name.replace(".", "-")
        generated_name = "perf-datagen-" + generated_name.lower() + "-" + sc[-3:]

        logger.info(f"### INFO ### create_multi_pvutil_dataset: ds is {ds}")
        ssh = self._OadpWorkloads__ssh
        for i in range(num_of_pods_expected):
            pvc_name = "pvc-" + generated_name + "-" + str(i)
            deployment_name = "deploy-" + generated_name + "-" + str(i)
            if active_role == "generator":
                dir_count = ds["dir_count"]
                files_count = ds["files_count"]
                file_size = ds["files_size"]
                dept_count = ds["dept_count"]
                playbook_extra_var = (
                    f"dir_count={dir_count} files_count={files_count} files_size={file_size} "
                    f"dept_count={dept_count} pvc_size={pvc_size} deployment_name={deployment_name} "
                    f"dataset_path={dataset_path} namespace={namespace} pvc_name={pvc_name} sc={sc}"
                )
            if active_role == "dd_generator":
                bs = ds["bs"]
                count = ds["count"]
                playbook_extra_var = (
                    f"bs={bs} count={count} pvc_size={pvc_size} deployment_name={deployment_name} "
                    f"dataset_path={dataset_path} namespace={namespace} pvc_name={pvc_name} sc={sc}"
                )
            logger.info(
                f"### INFO Dataset invoke with command: ansible-playbook {playbook_path} "
                f"--extra-vars '{playbook_extra_var}' -vvvv"
            )
            create_data_ansible_output = ssh.run(
                cmd=f"ansible-playbook {playbook_path} --extra-vars '{playbook_extra_var}' -vvv"
            )
            create_data_ansible_output_responses.append(create_data_ansible_output)

        num_of_validated_ansible_plays = 0
        for response in create_data_ansible_output_responses:
            ansible_play_invoked_successfully = self.validate_ansible_play(response)
            if not ansible_play_invoked_successfully:
                logger.warning(f"Ansible playbook validation failed for: {response}")
            else:
                num_of_validated_ansible_plays += 1
        logger.info(
            f"::INFO:: Total ansible plays verified: {num_of_validated_ansible_plays} out of expected {num_of_pods_expected} "
        )

        pods_ready_for_pv_util_validation = self.waiting_for_ns_to_reach_desired_pods(
            scenario=test_scenario,
            ds=ds,
        )
        if not pods_ready_for_pv_util_validation:
            logger.error(
                f":: ERROR :: create_multi_pvutil_dataset: number of created pods not match "
                f"expected pods {num_of_pods_expected}"
            )
        else:
            running_pods = self.get_list_of_pods(namespace=namespace)
            expected_pod_name = self.get_pod_prefix_by_dataset_role(ds)
            if expected_pod_name is None:
                logger.error("### ERROR ### create_multi_pvutil_dataset: Bad Pod Prefix from get_pod_prefix_by_dataset_role")
                logger.exception("get_pod_prefix_by_dataset_role getting bad pod prefix")
            running_pods = [element for element in running_pods if expected_pod_name in element]
            for pod in running_pods:
                result_of_waiting = self.wait_until_process_inside_pod_completes(
                    pod_name=pod,
                    namespace=namespace,
                    process_text_to_monitor=mount_point,
                    timeout_value=testcase_timeout,
                )
                if result_of_waiting is not False:
                    logger.info(f"::INFO:: Population process inside pod {pod} in ns {namespace} has completed")
                else:
                    logger.error(f"::ERROR:: wait_until_process_inside_pod_completes returned False for {pod} in {namespace}")

    @logger_time_stamp
    def verify_pod_presence_and_storage(self, scenario: dict, ds: dict) -> bool:
        """Verify pod count, PVC size, and storage class for the dataset when validation is enabled."""
        num_of_pods_expected = ds["pods_per_ns"]
        target_namespace = ds["namespace"]
        expected_size = ds["pv_size"]
        expected_sc = ds["sc"]
        skip_dataset_validation = scenario["args"].get("skip_source_dataset_check", False)

        if skip_dataset_validation:
            logger.warning(
                "You are skipping dataset validations - verify_pod_presence_and_storage will return True without checks"
            )
            return True
        check_num_of_pods_and_state = self.verify_running_pods(num_of_pods_expected, target_namespace)
        if not check_num_of_pods_and_state:
            logger.warning(
                f"verify_pod_presence_and_storage: Dataset not as expected - did not find "
                f"{num_of_pods_expected} of pods in namespace {target_namespace}"
            )
            return False
        running_pods = self.get_pods_by_ns_by_expected_name(ds)
        for p in running_pods:
            pod_storage_status = self.verify_pod_pv_size_and_sc(
                podName=p,
                expected_ns=target_namespace,
                expected_sc=expected_sc,
                expected_size=expected_size,
            )
            if not pod_storage_status:
                return False
        return True

    @logger_time_stamp
    def get_dataset_validation_mode(self, ds: dict) -> str:
        """Resolve dataset validation mode from CLI, dataset yaml, or default full."""
        validation_set_by_yaml = ds.get("validation_mode", False)
        validation_set_by_cli = self._OadpWorkloads__oadp_validation_mode
        if validation_set_by_cli:
            logger.info(f":: INFO :: Dataset Validation mode is set by CLI as {validation_set_by_cli}")
            return self._OadpWorkloads__oadp_validation_mode
        if validation_set_by_yaml:
            logger.info(f":: INFO :: Dataset Validation mode is set by yaml as {validation_set_by_yaml}")
            return validation_set_by_yaml
        return "full"

    def verify_datsets_before_backups(self, test_scenario: dict) -> None:
        """Ensure datasets exist before backup, recreating any that fail validation."""
        try:
            self.set_validation_retry_logic(interval_between_checks=5, max_attempts=1)
            dataset_already_present = self.validate_expected_datasets(test_scenario)
            self._OadpWorkloads__run_metadata["summary"]["validations"]["dataset_already_present"] = dataset_already_present
            if not dataset_already_present:
                self.set_validation_retry_logic(interval_between_checks=15, max_attempts=28)
                logger.info(
                    f" dataset_already_present: {dataset_already_present} and "
                    f"self.__oadp_ds_failing_validation "
                    f"{self._OadpWorkloads__oadp_ds_failing_validation} "
                )
                for ds in self._OadpWorkloads__oadp_ds_failing_validation:
                    self.create_source_dataset(test_scenario, ds)
                dataset_created_as_expected = self.validate_expected_datasets(test_scenario)
                self._OadpWorkloads__run_metadata["summary"]["validations"]["dataset_recreated_as_expected"] = (
                    dataset_created_as_expected
                )
                if not dataset_created_as_expected:
                    logger.exception(
                        f"Creation of Dataset for {test_scenario['args']['OADP_CR_NAME']} "
                        f"did not pass validations BEFORE the backup"
                    )
                else:
                    logger.info(f"Dataset post creation validations are {dataset_created_as_expected}")
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

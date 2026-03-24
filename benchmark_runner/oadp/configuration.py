"""
Mixin for DPA configuration, volume snapshot, and storage class setup.
"""

import time

from benchmark_runner.common.logger.logger_time_stamp import logger, logger_time_stamp
from benchmark_runner.oadp.constants import (
    DEFAULT_DPA_NAME,
    PLUGIN_CSI,
    PLUGIN_RESTIC,
    PLUGIN_VBD,
    SC_CEPH_RBD,
    SC_CEPHFS,
    SC_CEPHFS_SHALLOW,
)


class OadpConfigurationMixin:
    """DPA config, volume snapshot, storage class management."""

    @logger_time_stamp
    def is_dpa_change_needed(self, scenario, oadp_namespace):
        try:
            dpa_data = self.get_oc_resource_to_json(
                resource_type="dpa",
                resource_name=self._OadpWorkloads__oadp_dpa,
                namespace=oadp_namespace,
            )
            if not dpa_data:
                logger.error(
                    ":: ERROR :: FAIL DPA is not present command to get dpa as json "
                    "resulted in empty dict will attempt to recreate"
                )

            result = dpa_data["spec"]["configuration"]

            if "nodeAgent" not in result or not result["nodeAgent"]["enable"]:
                logger.warning(
                    "## WARNING ### is_dpa_change_needed: Condition not met: "
                    "'nodeAgent' must be present and 'enable' must be True."
                )
                return True

            if (
                "velero" not in result
                or "defaultPlugins" not in result["velero"]
                or "csi" not in result["velero"]["defaultPlugins"]
            ):
                logger.warning(
                    "## WARNING ### is_dpa_change_needed: Condition not met: "
                    "'csi' must be present in 'defaultPlugins' of 'velero'."
                )
                return True

            uploader_type = PLUGIN_RESTIC if scenario["args"]["plugin"] == PLUGIN_RESTIC else "kopia"

            logger.info(
                f"### INFO ### current uploader type: {result['nodeAgent']['uploaderType']} "
                f"desired plugin type is: {uploader_type} "
            )
            if (
                "nodeAgent" in result
                and "uploaderType" in result["nodeAgent"]
                and result["nodeAgent"]["uploaderType"] != uploader_type
            ):
                logger.warning(
                    f"## WARNING ### is_dpa_change_needed: Condition not met: "
                    f"'uploaderType' of 'nodeAgent' must be {uploader_type}."
                )
                return True

            if "velero" in result and "logLevel" in result["velero"] and result["velero"]["logLevel"] != "debug":
                logger.warning(
                    "## WARNING ### is_dpa_change_needed: Condition not met: 'logLevel' of 'velero' must be 'debug'."
                )
                return True

            logger.info("### INFO ### DPA is valid and as expected - skipping DPA changes")
            return False

        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def config_dpa_for_plugin(self, scenario, oadp_namespace):
        try:
            ssh = self._OadpWorkloads__ssh
            base_dir = self._OadpWorkloads__oadp_base_dir
            dpa_data = self.get_oc_resource_to_json(
                resource_type="dpa",
                resource_name=self._OadpWorkloads__oadp_dpa,
                namespace=oadp_namespace,
            )
            if not dpa_data:
                logger.error(
                    ":: ERROR :: FAIL DPA is not present command to get dpa as json "
                    "resulted in empty dict will attempt to recreate"
                )
                logger.info(f"### INFO ### Updating DPA via ansible: ansible-playbook {base_dir}/modify-dpa.yaml -vvv")
                update_dpa = ssh.run(cmd=f"cd {base_dir}; ansible-playbook {base_dir}/modify-dpa.yaml -vvv")
                ran_without_errors = self.validate_ansible_play(update_dpa)
                if not ran_without_errors:
                    logger.warning(f":: WARN :: DPA update FAILED to run successfully see: {update_dpa} ")
                else:
                    logger.info(":: INFO :: DPA update invoked successfully via ansible-play")

            dpa_name = dpa_data["metadata"]["name"]
            bucket_name = dpa_data["spec"]["backupLocations"][0]["velero"]["objectStorage"]["bucket"]
            config_profile = dpa_data["spec"]["backupLocations"][0]["velero"]["config"]["profile"]
            cred_name = dpa_data["spec"]["backupLocations"][0]["velero"]["credential"]["name"]
            if scenario["args"]["plugin"] in (PLUGIN_CSI, PLUGIN_VBD):
                uploader_type = "kopia"
            else:
                uploader_type = scenario["args"]["plugin"]
            ansible_args = (
                f"dpa_name={dpa_name} bucket_name={bucket_name} plugin_type={uploader_type} "
                f"profile={config_profile} cred_name={cred_name} oadp_ns={oadp_namespace}"
            )
            logger.info(
                f'### INFO ### Updating DPA via ansible: ansible-playbook {base_dir}/modify-dpa.yaml -e "{ansible_args}" -vvv'
            )
            update_dpa = ssh.run(cmd=f'cd {base_dir}; ansible-playbook {base_dir}/modify-dpa.yaml -e "{ansible_args}" -vvv')
            ran_without_errors = self.validate_ansible_play(update_dpa)
            if not ran_without_errors:
                logger.warning(f":: WARN :: DPA update FAILED to run successfully see: {update_dpa} ")
            else:
                logger.info(":: INFO :: DPA update invoked successfully via ansible-play")

        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def set_volume_snapshot_class(self, sc, scenario):
        try:
            base_dir = self._OadpWorkloads__oadp_base_dir
            cmd_set_volume_snapshot_class = ""
            if sc == SC_CEPH_RBD:
                cmd_set_volume_snapshot_class = self._OadpWorkloads__ssh.run(cmd=f"oc apply -f {base_dir}/vsc-cephRBD.yaml")
            if sc in (SC_CEPHFS, SC_CEPHFS_SHALLOW):
                cmd_set_volume_snapshot_class = self._OadpWorkloads__ssh.run(cmd=f"oc apply -f {base_dir}/vsc-cephFS.yaml")
            expected_result_output = ["created", "unchanged", "configured", "already exists for driver"]
            if not any(ext in cmd_set_volume_snapshot_class for ext in expected_result_output):
                logger.error(
                    f":: INFO :: set_volume_snapshot_class: not successful to setup for {sc} "
                    f"output from command was {cmd_set_volume_snapshot_class} "
                )
                logger.exception(f"Unable to set volume-snapshot-class {sc}")
            else:
                logger.info(f":: INFO :: set_volume_snapshot_class: setup for {sc} ")
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def set_default_storage_class(self, sc):
        try:
            ssh = self._OadpWorkloads__ssh
            current_sc = self.get_default_storage_class()
            if current_sc != sc:
                if current_sc == SC_CEPHFS_SHALLOW:
                    import packaging.version

                    current_oc_version = packaging.version.parse(self._oc.get_ocp_server_version())
                    minimal_supported_oc_version = packaging.version.parse("4.12.0")
                    if current_oc_version < minimal_supported_oc_version:
                        logger.error(
                            f":: ERROR :: CEPHFS-Shallow set to desired sc on unsupported oc version. "
                            f"Please check your yaml scenario "
                            f"{self._OadpWorkloads__oadp_scenario_name} for the storage class is set "
                            f"{sc} which requires {minimal_supported_oc_version}, "
                            f"this env is: {current_oc_version} "
                        )
                json_sc = '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
                set_def_sc_cmd = ssh.run(cmd=f"oc patch storageclass {sc} -p '{json_sc}'")
                if set_def_sc_cmd.find("patched") < 0:
                    logger.exception(f"Unable to set {sc} as default storage class")
            cmd_output = ssh.run(cmd='oc get sc -o jsonpath="{.items[*].metadata.name}"')
            list_of_storage_class_present = list(filter(None, cmd_output.split(" ")))
            for storage in list_of_storage_class_present:
                if storage != sc:
                    json_sc = '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}'
                    ssh.run(cmd=f"oc patch storageclass {storage} -p '{json_sc}'")
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def set_velero_log_level(self, oadp_namespace):
        ssh = self._OadpWorkloads__ssh
        test_env = self._OadpWorkloads__test_env
        if self.this_is_downstream():
            json_query = """[{"op": "add", "path": "/spec/configuration/velero/logLevel", "value": "debug"}]"""
            ssh.run(
                cmd=f"oc patch dataprotectionapplication {DEFAULT_DPA_NAME} -n {oadp_namespace} --type=json -p='{json_query}'"
            )
            logger.info(":: INFO :: Setting debug log level on velero")
        else:
            velero_deployment = self.get_oc_resource_to_json(
                resource_type="deployment",
                resource_name="velero",
                namespace=test_env["velero_ns"],
            )
            if not velero_deployment or "spec" not in velero_deployment:
                logger.warning(
                    f":: WARN :: set_velero_log_level: velero deployment not found in {test_env['velero_ns']}"
                )
                return
            velero_current_args = velero_deployment["spec"]["template"]["spec"]["containers"][0]["args"]
            if "--log-level=debug" not in velero_current_args:
                velero_current_args.append("--log-level=debug")
                json_query = (
                    '[{"op": "replace", "path": "/spec/template/spec/containers/0/args", "value":'
                    + f"{velero_current_args}"
                    + "}]"
                )
                self.patch_oc_resource(
                    resource_type="deployment",
                    resource_name="velero",
                    namespace=test_env["velero_ns"],
                    patch_type="json",
                    patch_json=json_query,
                )
                logger.info(f":: INFO :: Setting debug log level on velero upstream instance in {test_env['velero_ns']}")

    @logger_time_stamp
    def wait_for_dpa_changes(self, oadp_namespace):
        try:
            get_workers_cmd = self._OadpWorkloads__ssh.run(
                cmd="""oc get nodes -l node-role.kubernetes.io/worker -o jsonpath='{.items[*].metadata.name}'"""
            )
            num_of_node_agents_expected = len(get_workers_cmd.split(" "))
            num_of_pods_expected = num_of_node_agents_expected + 2
            logger.info(":: INFO :: Waiting for DPA changes to take effect")
            time.sleep(15)
            self.verify_running_pods(num_of_pods_expected, target_namespace=oadp_namespace)
        except Exception as err:
            logger.error(f":: ERROR :: Issue in waiting for DPA changes to process in your oadp namespace {err}")
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def verify_bsl_status(self):
        try:
            retries = 0
            max_retries = 30
            test_env = self._OadpWorkloads__test_env
            bsl_default_name = self._OadpWorkloads__ssh.run(
                cmd=f"oc get bsl -n {test_env['velero_ns']} | grep true | awk '{{print $1}}'"
            )
            while retries < max_retries:
                bsl_data = self.get_oc_resource_to_json(
                    resource_type="bsl",
                    resource_name=bsl_default_name,
                    namespace=test_env["velero_ns"],
                )
                logger.info(f"### INFO ### verify_bsl_status: attempt {retries} of {max_retries} shows cmd_output {bsl_data}")
                if not bsl_data:
                    logger.warning(":: ERROR :: get_bsl_status showed that BSL is not present or json resulted in empty dict")
                if "phase" in bsl_data["status"]:
                    bsl_status = bsl_data["status"]["phase"]
                    logger.info(f"### INFO ### verify_bsl_status: returned status of {bsl_status}")
                    if bsl_status == "Available":
                        return
                    else:
                        logger.info(f"### INFO ### verify_bsl_status: returned status of {bsl_status}")
                else:
                    logger.info(
                        ":: INFO :: verify_bsl_status: BSL state is not ready as phase is not yet "
                        "available sleeping for 5 seconds this may take up to 30s"
                    )
                    time.sleep(5)
                    retries += 1
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def setup_ocs_cephfs_shallow(self):
        cmd_create_cephfs_shallow_sc = self._OadpWorkloads__ssh.run(
            cmd=f"oc apply -f {self._OadpWorkloads__oadp_base_dir}/templates/cephfs-shallow.yaml"
        )
        if "created" not in cmd_create_cephfs_shallow_sc and "configured" not in cmd_create_cephfs_shallow_sc:
            logger.error(
                f":: ERROR :: cephfs-shallow not deployed correctly - "
                f"following output returned: {cmd_create_cephfs_shallow_sc}"
            )
        else:
            logger.info(":: INFO :: cephfs-shallow sc is present")

"""
Mixin for DPA configuration, volume snapshot, and storage class setup.
"""

import json
import time

from benchmark_runner.common.logger.logger_time_stamp import logger, logger_time_stamp
from benchmark_runner.oadp.constants import (
    DEFAULT_DPA_NAME,
    KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME,
    PLUGIN_CSI,
    PLUGIN_KUBEVIRT,
    PLUGIN_KUBEVIRT_DATAMOVER,
    PLUGIN_RESTIC,
    PLUGIN_VBD,
    SC_CEPH_RBD,
    SC_CEPH_RBD_VIRTUALIZATION,
    SC_CEPHFS,
    SC_CEPHFS_SHALLOW,
    VM_DATASET_ROLE,
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

            if self._scenario_needs_kubevirt_plugin(scenario):
                default_plugins = result.get("velero", {}).get("defaultPlugins", [])
                if PLUGIN_KUBEVIRT not in default_plugins:
                    logger.warning(
                        "## WARNING ### is_dpa_change_needed: 'kubevirt' not in defaultPlugins but scenario uses VM datasets."
                    )
                    return True

            # 'kubevirt-datamover' (Flow2, CBT scenarios) is deliberately NOT checked here:
            # it's not managed by modify-dpa.yaml (the ansible playbook this function gates),
            # so reporting a change here would trigger a no-op ansible run. It's verified and
            # patched directly by ensure_kubevirt_datamover_plugin(), called unconditionally
            # from oadp_workloads.py right after this DPA block.

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
            if not self.validate_dpa_structure(oadp_namespace):
                logger.error("DPA pre-validation failed; aborting DPA patch")
                self.fail_test_run("DPA pre-validation failed before patching")

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
            if scenario["args"]["plugin"] in (PLUGIN_CSI, PLUGIN_VBD, PLUGIN_KUBEVIRT):
                uploader_type = "kopia"
            else:
                uploader_type = scenario["args"]["plugin"]
            kubevirt_flag = (
                "kubevirt_plugin=true" if self._scenario_needs_kubevirt_plugin(scenario) else "kubevirt_plugin=false"
            )
            ansible_args = (
                f"dpa_name={dpa_name} bucket_name={bucket_name} plugin_type={uploader_type} "
                f"profile={config_profile} cred_name={cred_name} oadp_ns={oadp_namespace} "
                f"{kubevirt_flag}"
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
            if sc in (SC_CEPH_RBD, SC_CEPH_RBD_VIRTUALIZATION):
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
                logger.warning(f":: WARN :: set_velero_log_level: velero deployment not found in {test_env['velero_ns']}")
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

    # ------------------------------------------------------------------
    # Flow2 setup: CBT-enabled kubevirt backups (ticket 028)
    # ------------------------------------------------------------------

    @logger_time_stamp
    def ensure_kubevirt_datamover_plugin(self, scenario: dict, oadp_namespace: str) -> bool:
        """Ensure 'kubevirt-datamover' is present in the DPA's defaultPlugins (Flow2).

        No-op unless the scenario is CBT-enabled kubevirt (see
        ``_scenario_needs_kubevirt_datamover_plugin``). Patches the DPA
        directly via ``oc patch`` rather than routing through
        ``modify-dpa.yaml`` (the ansible playbook only knows about the plain
        'kubevirt' flag) — mirrors the JSON-patch approach already used by
        ``set_velero_log_level()``.

        Returns True if a patch was applied (caller should then re-run
        ``wait_for_dpa_changes``/``verify_bsl_status``), False if already
        present or not needed.
        """
        if not self._scenario_needs_kubevirt_datamover_plugin(scenario):
            return False

        dpa_data = self.get_oc_resource_to_json(
            resource_type="dpa",
            resource_name=self._OadpWorkloads__oadp_dpa,
            namespace=oadp_namespace,
        )
        if not dpa_data:
            logger.error(
                ":: ERROR :: ensure_kubevirt_datamover_plugin: DPA not found; "
                "cannot verify/add 'kubevirt-datamover' plugin"
            )
            self.fail_test_run("DPA not present while ensuring 'kubevirt-datamover' plugin")
            return False

        default_plugins = dpa_data.get("spec", {}).get("configuration", {}).get("velero", {}).get("defaultPlugins", [])
        if PLUGIN_KUBEVIRT_DATAMOVER in default_plugins:
            logger.info(
                "### INFO ### ensure_kubevirt_datamover_plugin: 'kubevirt-datamover' already present in defaultPlugins"
            )
            return False

        updated_plugins = [*default_plugins, PLUGIN_KUBEVIRT_DATAMOVER]
        json_query = (
            '[{"op": "replace", "path": "/spec/configuration/velero/defaultPlugins", "value": '
            + json.dumps(updated_plugins)
            + "}]"
        )
        self.patch_oc_resource(
            resource_type="dpa",
            resource_name=self._OadpWorkloads__oadp_dpa,
            namespace=oadp_namespace,
            patch_type="json",
            patch_json=json_query,
        )
        logger.info(
            f"### INFO ### ensure_kubevirt_datamover_plugin: added 'kubevirt-datamover' to defaultPlugins: "
            f"{updated_plugins}"
        )
        return True

    @logger_time_stamp
    def ensure_kubevirt_volume_policy_configmap(self, scenario: dict, oadp_namespace: str) -> None:
        """Ensure the 'kubevirt-volume-policy' resource-policy ConfigMap exists (Flow2).

        No-op unless the scenario is CBT-enabled kubevirt (see
        ``_scenario_needs_kubevirt_datamover_plugin``). Referenced by
        ``exec_backup()`` via ``--resource-policies-configmap`` on CBT-enabled
        kubevirt backups so Velero routes matching volumes through the
        kubevirt datamover (``action.type: custom``, ``datamover: kubevirt``)
        instead of its own CSI-snapshot handling.

        If the ConfigMap is already present, it is left untouched — do not
        overwrite a cluster-managed or previously applied policy.
        """
        if not self._scenario_needs_kubevirt_datamover_plugin(scenario):
            return

        existing = self.get_oc_resource_to_json(
            resource_type="configmap",
            resource_name=KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME,
            namespace=oadp_namespace,
        )
        if existing:
            logger.info(
                f"### INFO ### ensure_kubevirt_volume_policy_configmap: "
                f"'{KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME}' already present in "
                f"{oadp_namespace}; leaving it unchanged"
            )
            return

        manifest = (
            "apiVersion: v1\n"
            "kind: ConfigMap\n"
            "metadata:\n"
            f"  name: {KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME}\n"
            f"  namespace: {oadp_namespace}\n"
            "data:\n"
            "  policy.yaml: |\n"
            "    version: v1\n"
            "    volumePolicies:\n"
            "      - conditions: {}\n"
            "        action:\n"
            "          type: custom\n"
            "          parameters:\n"
            "            datamover: kubevirt\n"
        )
        result = self._OadpWorkloads__ssh.run(cmd=f"cat <<'EOF' | oc create -f -\n{manifest}EOF")
        if "created" not in result:
            logger.error(
                f":: ERROR :: ensure_kubevirt_volume_policy_configmap: create failed for "
                f"'{KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME}' in {oadp_namespace}: {result}"
            )
            self.fail_test_run(f"Unable to create {KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME} ConfigMap")
        else:
            logger.info(f":: INFO :: ensure_kubevirt_volume_policy_configmap: {result}")

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

    @staticmethod
    def _scenario_needs_kubevirt_plugin(scenario: dict) -> bool:
        """Return True when the scenario uses the kubevirt Velero plugin."""
        return scenario.get("args", {}).get("plugin") == PLUGIN_KUBEVIRT

    @staticmethod
    def _scenario_needs_kubevirt_datamover_plugin(scenario: dict) -> bool:
        """Return True for CBT-enabled kubevirt VM scenarios (Flow2, ticket 028).

        Two flows exist for ``plugin: kubevirt`` VM backups:

        - Flow1 (no CBT / ``cbt: false``, e.g. TC 10.4): plain 'kubevirt' plugin
          in the DPA's defaultPlugins is sufficient; ``exec_backup()`` already
          runs a normal kubevirt-datamover backup (``--data-mover velero
          --snapshot-move-data=true``).
        - Flow2 (``cbt: true``, e.g. TC 10.5): the DPA additionally needs the
          'kubevirt-datamover' plugin registered, plus a resource-policy
          ConfigMap ('kubevirt-volume-policy') referenced on the backup CR —
          otherwise the CSI snapshot policy conflicts with the datamover's own
          snapshot handling of CBT-labeled volumes. This was observed live
          during the TC 10.5 test as backup warnings: "PVC ... has snapshot
          policy which conflicts with kubevirt datamover" (backup still
          completed, but the warnings indicate the setup was incomplete).
        """
        if not OadpConfigurationMixin._scenario_needs_kubevirt_plugin(scenario):
            return False
        script_args = scenario.get("args", {}).get("vm_population_script_args", {})
        return bool(script_args.get("cbt", False))

    @logger_time_stamp
    def validate_dpa_structure(self, oadp_namespace: str) -> bool:
        """Pre-validate DPA JSON before patching (SEC-004).

        Checks that the DPA has the expected top-level structure so
        that a bad patch cannot silently corrupt the resource.
        """
        dpa_data = self.get_oc_resource_to_json(
            resource_type="dpa",
            resource_name=self._OadpWorkloads__oadp_dpa,
            namespace=oadp_namespace,
        )
        if not dpa_data:
            logger.error("DPA pre-validation: resource not found")
            return False

        required_paths = [
            ("spec", "configuration", "velero", "defaultPlugins"),
            ("spec", "backupLocations"),
        ]
        for path_parts in required_paths:
            node = dpa_data
            for part in path_parts:
                if not isinstance(node, dict) or part not in node:
                    logger.error(f"DPA pre-validation: missing path {'.'.join(path_parts)} at '{part}'")
                    return False
                node = node[part]
        logger.info("DPA pre-validation: structure OK")
        return True

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

"""
Mixin for OADP DataMover, CephFS-shallow, and VolSync configuration.
"""

from __future__ import annotations

from benchmark_runner.common.logger.logger_time_stamp import logger, logger_time_stamp
from benchmark_runner.oadp.constants import (
    DEFAULT_DPA_NAME,
    PLUGIN_VSM,
    SC_CEPHFS_SHALLOW,
)


class OadpDatamoverMixin:
    """DataMover enable/disable, CephFS-shallow DPA patches, VolSync checks."""

    @logger_time_stamp
    def config_dpa_for_cephfs_shallow(self, enable: bool, oadp_namespace: str) -> None:
        """Merge or remove DPA dataMover volumeOptions for CephFS-shallow."""
        if enable:
            query = (
                '{"spec": {"features": {"dataMover": {"volumeOptions": {"destinationVolumeOptions": '
                '{"accessMode": "ReadOnlyMany","cacheAccessMode": "ReadWriteOnce",'
                '"storageClassName": "ocs-storagecluster-cephfs-shallow","moverSecurityContext": true}, '
                '"sourceVolumeOptions": {"accessMode": "ReadOnlyMany","cacheAccessMode": "ReadWriteMany",'
                '"cacheStorageClassName": "ocs-storagecluster-cephfs","moverSecurityContext": true,'
                '"storageClassName": "ocs-storagecluster-cephfs-shallow"}}}}}}'
            )
            self.patch_oc_resource(
                resource_type="dpa",
                resource_name=DEFAULT_DPA_NAME,
                namespace=oadp_namespace,
                patch_type="merge",
                patch_json=query,
            )
        if not enable:
            dpa_data = self.get_oc_resource_to_json(
                resource_type="dpa",
                resource_name=self._OadpWorkloads__oadp_dpa,
                namespace=oadp_namespace,
            )
            if not dpa_data:
                logger.error(":: ERROR :: DPA is not present command to get dpa as json resulted in empty dict")
            if dpa_data["spec"].get("Features", False):
                logger.info(":: INFO :: DPA has spec/features will remove volumeoptions if present")
                query = '[{"op":"remove", "path": "/spec/features/dataMover/volumeOptions"}]'
                self.patch_oc_resource(
                    resource_type="dpa",
                    resource_name=DEFAULT_DPA_NAME,
                    namespace=oadp_namespace,
                    patch_type="json",
                    patch_json=query,
                )
            else:
                logger.info(":: INFO :: DPA has NO spec/features so no volumeoptions to remove")

    @logger_time_stamp
    def config_datamover(self, oadp_namespace: str, scenario: dict) -> None:
        """Patch DPA dataMover limits from scenario config or remove keys when set to remove."""
        set_maxConcurrentBackupVolumes = scenario["config"]["dataMover"].get("maxConcurrentBackupVolumes", False)
        set_maxConcurrentRestoreVolumes = scenario["config"]["dataMover"].get("maxConcurrentRestoreVolumes", False)
        set_timeout = scenario["config"]["dataMover"].get("timeout", False)

        for key, path_segment in [
            (set_maxConcurrentBackupVolumes, "maxConcurrentBackupVolumes"),
            (set_maxConcurrentRestoreVolumes, "maxConcurrentRestoreVolumes"),
            (set_timeout, "timeout"),
        ]:
            if key is not False:
                value = scenario["config"]["dataMover"][path_segment]
                if value != "remove":
                    query = '{"spec": {"features": {"dataMover": {"' + path_segment + '": "' + str(value) + '"}}}}'
                    self.patch_oc_resource(
                        resource_type="dpa",
                        resource_name=DEFAULT_DPA_NAME,
                        namespace=oadp_namespace,
                        patch_type="merge",
                        patch_json=query,
                    )
                else:
                    query = f'[{{"op":"remove", "path": "/spec/features/dataMover/{path_segment}"}}]'
                    self.patch_oc_resource(
                        resource_type="dpa",
                        resource_name=DEFAULT_DPA_NAME,
                        namespace=oadp_namespace,
                        patch_type="json",
                        patch_json=query,
                    )

    @logger_time_stamp
    def is_datamover_enabled(self, oadp_namespace: str, scenario: dict) -> bool:
        """Return True if VSM plugin or dataMover feature is enabled on the DPA."""
        dpa_data = self.get_oc_resource_to_json(
            resource_type="dpa",
            resource_name=self._OadpWorkloads__oadp_dpa,
            namespace=oadp_namespace,
        )
        if not dpa_data:
            logger.error(":: ERROR :: DPA is not present command to get dpa as json resulted in empty dict")
        velero_enabled_plugins = dpa_data["spec"]["configuration"]["velero"]["defaultPlugins"]
        is_vsm_enabled = "vsm" in velero_enabled_plugins
        if dpa_data["spec"].get("features", False):
            is_datamover_enabled = dpa_data["spec"]["features"]["dataMover"]["enable"]
        else:
            is_datamover_enabled = False
        return is_vsm_enabled or is_datamover_enabled

    @logger_time_stamp
    def enable_datamover(self, oadp_namespace: str, scenario: dict) -> None:
        """Enable data mover: restic secret, S3 URL, VSM plugin, and dataMover feature on DPA."""
        ssh = self._OadpWorkloads__ssh
        test_env = self._OadpWorkloads__test_env
        base_dir = self._OadpWorkloads__oadp_base_dir
        dpa_data = self.get_oc_resource_to_json(
            resource_type="dpa",
            resource_name=self._OadpWorkloads__oadp_dpa,
            namespace=oadp_namespace,
        )
        if not dpa_data:
            logger.error(":: ERROR :: DPA is not present command to get dpa as json resulted in empty dict")

        dpa_name = dpa_data["metadata"]["name"]
        is_restic_enabled = dpa_data["spec"]["configuration"]["restic"]["enable"]
        velero_enabled_plugins = dpa_data["spec"]["configuration"]["velero"]["defaultPlugins"]
        restic_secret = self.get_oc_resource_to_json(
            resource_type="secret",
            resource_name="restic-secret",
            namespace=oadp_namespace,
        )
        if not restic_secret:
            logger.info(":: INFO :: Restic Secret not found - will deploy it now")
            cmd_restic_secret_apply = ssh.run(cmd=f"oc apply -f {base_dir}/templates/restic_secret.yaml")
            if "created" not in cmd_restic_secret_apply and "configured" not in cmd_restic_secret_apply:
                logger.error(":: ERROR :: Restic Secret not deployed correctly")
            else:
                logger.info(":: INFO :: Restic Secret created successfully")
        self.patch_oc_resource(
            resource_type="dpa",
            resource_name=DEFAULT_DPA_NAME,
            namespace=test_env["velero_ns"],
            patch_type="merge",
            patch_json='{"spec": {"features": {"dataMover": {"credentialName": "restic-secret"}}}}',
        )
        if is_restic_enabled:
            json_query = '{"spec": {"configuration": {"restic": {"enable": false}}}}'
            disable_restic_plugin = ssh.run(cmd=f"oc patch dpa {dpa_name} -n {oadp_namespace} --type merge -p '{json_query}'")
            if "error" in disable_restic_plugin:
                logger.error(f":: ERROR :: Attempted to disable restic: {disable_restic_plugin}")
            else:
                logger.info(":: INFO :: Restic Secret disabled successfully")
        if not self._OadpWorkloads__oadp_bucket:
            logger.info(":: INFO :: Using S3 bucket found in mcg route ")
            json_query = "{.spec.host}"
            cmd_get_s3_url = ssh.run(cmd=f" oc get route s3 -n openshift-storage -o jsonpath='{json_query}'")
        else:
            logger.info(f":: INFO :: Using S3 bucket passed by user as {self._OadpWorkloads__oadp_bucket} found in mcg route ")
            cmd_get_s3_url = self._OadpWorkloads__oadp_bucket
        if cmd_get_s3_url != "" and "error" not in cmd_get_s3_url:
            logger.info(f":: INFO :: Setting S3 {cmd_get_s3_url} to {dpa_name}")
            json_query = (
                f"""[{{"op": "replace", "path": "/spec/backupLocations/0/velero/config/s3Url", """
                f""""value": "http://{cmd_get_s3_url}"}}]"""
            )
            cmd_setting_s3_in_dpa = ssh.run(
                cmd=f"oc patch dataprotectionapplication {dpa_name} -n {oadp_namespace} --type=json -p='{json_query}'"
            )
            if cmd_setting_s3_in_dpa != "" and "error" not in cmd_setting_s3_in_dpa:
                logger.info(f":: INFO :: S3 set sucessfully {cmd_setting_s3_in_dpa} to {dpa_name}")
        self.verify_volsync_present()
        if "vsm" not in velero_enabled_plugins:
            logger.info(f":: INFO :: Setting VSM to defaultplugins in {dpa_name}")
            json_query = """[{"op": "add", "path": "/spec/configuration/velero/defaultPlugins/-", "value": "vsm"}]"""
            cmd_setting_vsm = ssh.run(
                cmd=f"oc patch dataprotectionapplication {dpa_name} -n {oadp_namespace} --type=json -p='{json_query}'"
            )
            if cmd_setting_vsm != "" and "error" not in cmd_setting_vsm:
                logger.info(f":: INFO :: VSM to defaultplugins in {dpa_name}")

        json_query = '{"spec": {"features": {"dataMover": {"enable": true}}}}'
        enable_dataMover = ssh.run(cmd=f"oc patch dpa {dpa_name} -n {oadp_namespace} --type merge -p '{json_query}'")
        if enable_dataMover != "" and "error" not in enable_dataMover:
            logger.info(f":: INFO :: Datamover is now enabled for {dpa_name}")

    @logger_time_stamp
    def disable_datamover(self, oadp_namespace: str) -> None:
        """Disable data mover, re-enable restic if needed, and remove VSM from defaultPlugins."""
        ssh = self._OadpWorkloads__ssh
        dpa_data = self.get_oc_resource_to_json(
            resource_type="dpa",
            resource_name=self._OadpWorkloads__oadp_dpa,
            namespace=oadp_namespace,
        )
        if not dpa_data:
            logger.error(":: ERROR :: DPA is not present command to get dpa as json resulted in empty dict")

        dpa_name = dpa_data["metadata"]["name"]
        velero_enabled_plugins = dpa_data["spec"]["configuration"]["velero"]["defaultPlugins"]
        is_restic_enabled = dpa_data["spec"]["configuration"]["restic"]["enable"]
        if not is_restic_enabled:
            json_query = '{"spec": {"configuration": {"restic": {"enable": true}}}}'
            enable_restic = ssh.run(cmd=f"oc patch dpa {dpa_name} -n {oadp_namespace} --type merge -p '{json_query}'")
            if "error" in enable_restic:
                logger.error(f":: ERROR :: Attempted to enable restic: {enable_restic}")
            else:
                logger.info(":: INFO :: Restic Secret enabled successfully")
        if dpa_data["spec"].get("features", False):
            is_datamover_enabled = dpa_data["spec"]["features"]["dataMover"]["enable"]
        else:
            is_datamover_enabled = False
        if is_datamover_enabled:
            self.patch_oc_resource(
                resource_type="dpa",
                resource_name=DEFAULT_DPA_NAME,
                namespace=oadp_namespace,
                patch_type="merge",
                patch_json='{"spec": {"features": {"dataMover": {"enable": false}}}}',
            )
            self.patch_oc_resource(
                resource_type="dpa",
                resource_name=DEFAULT_DPA_NAME,
                namespace=oadp_namespace,
                patch_type="json",
                patch_json='[{"op":"remove", "path": "/spec/features"}]',
            )
        if "vsm" in velero_enabled_plugins:
            logger.info(f":: INFO :: Current plugins that are enabled are: {velero_enabled_plugins}")
            for i in range(len(dpa_data["spec"]["configuration"]["velero"]["defaultPlugins"])):
                if dpa_data["spec"]["configuration"]["velero"]["defaultPlugins"][i] == "vsm":
                    query = '[{"op":"remove", "path": "/spec/configuration/velero/defaultPlugins/' + str(i) + '"}]'
                    self.patch_oc_resource(
                        resource_type="dpa",
                        resource_name=DEFAULT_DPA_NAME,
                        namespace=oadp_namespace,
                        patch_type="json",
                        patch_json=query,
                    )

    @logger_time_stamp
    def verify_volsync_present(self) -> bool:
        """Return True if VolSync CSV in openshift-operators is in Succeeded phase."""
        cmd_volsync_status = self._OadpWorkloads__ssh.run(
            cmd="oc get csv -n openshift-operators | grep VolSync | awk {'print $5'}"
        )
        if "error" in cmd_volsync_status or cmd_volsync_status == "":
            logger.error(f":: ERROR :: Volsync not installed {cmd_volsync_status}")
            return False
        if cmd_volsync_status == "Succeeded":
            logger.info(":: INFO :: Volsync is present")
            return True
        return False

    @logger_time_stamp
    def checking_for_configurations_for_datamover(self, test_scenario: dict) -> None:
        """Align datamover and CephFS-shallow DPA settings with the test scenario plugin and storage class."""
        expected_sc = test_scenario["dataset"]["sc"]
        if self.this_is_downstream():
            test_env = self._OadpWorkloads__test_env
            if test_scenario["args"]["plugin"] == PLUGIN_VSM:
                if not self.is_datamover_enabled(oadp_namespace=test_env["velero_ns"], scenario=test_scenario):
                    self.enable_datamover(oadp_namespace=test_env["velero_ns"], scenario=test_scenario)
                    self.config_datamover(oadp_namespace=test_env["velero_ns"], scenario=test_scenario)
                    if expected_sc == SC_CEPHFS_SHALLOW:
                        self.config_dpa_for_cephfs_shallow(enable=True, oadp_namespace=test_env["velero_ns"])
                    else:
                        self.config_dpa_for_cephfs_shallow(enable=False, oadp_namespace=test_env["velero_ns"])
                else:
                    self.config_dpa_for_cephfs_shallow(enable=False, oadp_namespace=test_env["velero_ns"])
            else:
                self.config_dpa_for_cephfs_shallow(enable=False, oadp_namespace=test_env["velero_ns"])
                if self.is_datamover_enabled(oadp_namespace=test_env["velero_ns"], scenario=test_scenario):
                    self.disable_datamover(oadp_namespace=test_env["velero_ns"])

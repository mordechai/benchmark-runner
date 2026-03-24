"""
Mixin for post-run cleanup of OADP resources.

Handles deletion of CRs, S3 bucket cleaning, ODF pool purging,
VSC removal, and dataset namespace deletion.
"""

from __future__ import annotations

import time

from benchmark_runner.common.logger.logger_time_stamp import logger, logger_time_stamp
from benchmark_runner.oadp.constants import PLUGIN_VBD, SOURCE_DOWNSTREAM, SOURCE_UPSTREAM


class OadpCleanupMixin:
    """S3 bucket, ODF pool, VSC, CR, and dataset cleanup."""

    @logger_time_stamp
    def delete_all(self) -> None:
        """Delete all pods cluster-wide matching the workload namespace label."""
        self._OadpWorkloads__ssh.run(cmd=f"oc delete pod -A -l {self._OadpWorkloads__namespace}")

    @logger_time_stamp
    def cleaning_up_oadp_resources(self, scenario: dict) -> None:
        """Run post-run cleanup of OADP CRs, S3, VSC, ODF pool, and dataset namespaces per flags."""
        try:
            test_env = self._OadpWorkloads__test_env
            if self._OadpWorkloads__oadp_cleanup_cr_post_run:
                if "restore" == scenario["testtype"]:
                    logger.info(
                        f"*** Attempting post run: clean up for {scenario['args']['OADP_CR_NAME']} "
                        f"that is a {scenario['testtype']} relevant CRs to remove are: "
                        f"restore: {scenario['args']['OADP_CR_NAME']} & relevant CRs related "
                        f"backup CR: {scenario['args']['backup_name']} "
                    )
                    self.delete_oadp_custom_resources(
                        ns=test_env["velero_ns"],
                        cr_type=scenario["args"]["OADP_CR_TYPE"],
                        cr_name=scenario["args"]["OADP_CR_NAME"],
                    )
                    self.delete_oadp_custom_resources(
                        ns=test_env["velero_ns"],
                        cr_type="backup",
                        cr_name=scenario["args"]["backup_name"],
                    )
                    self.clean_s3_bucket(scenario=scenario, oadp_namespace=test_env["velero_ns"])
                    self.delete_vsc(scenario=scenario, ns_scoped=False)
                    self.clean_odf_pool(scenario=scenario)
                if "backup" == scenario["testtype"]:
                    logger.info(
                        f"*** Attempting post run: clean up for {scenario['args']['OADP_CR_NAME']} "
                        f"that is a {scenario['testtype']} relevant CRs to remove are: "
                        f"{scenario['args']['OADP_CR_NAME']}"
                    )
                    self.delete_oadp_custom_resources(
                        test_env["velero_ns"],
                        cr_type=scenario["args"]["OADP_CR_TYPE"],
                        cr_name=scenario["args"]["OADP_CR_NAME"],
                    )
            else:
                logger.info(
                    f"*** Skipping post run cleaning up of OADP CR *** as "
                    f"self.__oadp_cleanup_cr_post_run: "
                    f"{self._OadpWorkloads__oadp_cleanup_cr_post_run}"
                )
            if self._OadpWorkloads__oadp_cleanup_dataset_post_run:
                logger.info(
                    f"*** Attempting post run: clean up of OADP dataset *** as "
                    f"self.__oadp_cleanup_dataset_post_run: "
                    f"{self._OadpWorkloads__oadp_cleanup_dataset_post_run}"
                )
                self.delete_source_dataset(target_namespace=scenario["args"]["namespaces_to_backup"])
            else:
                logger.info(
                    f"*** Skipping post run cleaning up of OADP dataset *** as "
                    f"self.__oadp_cleanup_dataset_post_run: "
                    f"{self._OadpWorkloads__oadp_cleanup_dataset_post_run}"
                )
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def clean_s3_bucket(self, scenario: dict, oadp_namespace: str) -> None:
        """Empty the backup S3 bucket using ansible-playbook and DPA-derived endpoint."""
        dpa_data = self.get_oc_resource_to_json(
            resource_type="dpa",
            resource_name=self._OadpWorkloads__oadp_dpa,
            namespace=oadp_namespace,
        )
        if not dpa_data:
            logger.error(":: ERROR :: DPA is not present command to get dpa as json resulted in empty dict")
        s3_from_dpa = dpa_data["spec"]["backupLocations"][0]["velero"]["config"]["s3Url"]
        s3_from_dpa = s3_from_dpa.split("//")[-1]
        if s3_from_dpa == "":
            logger.warning(":: INFO :: WARNING DPA did not contain S3 URL we will assume mcg will be used")
            json_query = "{.spec.host}"
            mcg_s3_url = self._OadpWorkloads__ssh.run(cmd=f" oc get route s3 -n openshift-storage -o jsonpath='{json_query}'")
            s3_from_dpa = mcg_s3_url
        json_query = "{.spec.host}"
        mcg_s3_url = self._OadpWorkloads__ssh.run(cmd=f" oc get route s3 -n openshift-storage -o jsonpath='{json_query}'")
        s3_uses_local_mcg = mcg_s3_url in s3_from_dpa
        logger.info(
            f":: INFO :: clean_s3_bucket detected s3_uses_local_mcg: {s3_uses_local_mcg} "
            f"now attempting rm on bucket {s3_from_dpa} "
        )
        misc_dir = self._OadpWorkloads__oadp_misc_dir
        clean_bucket_cmd = self._OadpWorkloads__ssh.run(
            cmd=f"ansible-playbook {misc_dir}/bucket-clean.yaml -e 'use_nooba={s3_uses_local_mcg} s3_url={s3_from_dpa}' -vvvv"
        )
        ran_without_errors = self.validate_ansible_play(clean_bucket_cmd)
        if not ran_without_errors:
            logger.warning(f":: WARN :: clean_s3_bucket FAILED to run successfully see: {clean_bucket_cmd} ")
        else:
            logger.info(":: INFO :: clean_s3_bucket invoked successfully via ansible-play")

    @logger_time_stamp
    def clean_odf_pool(self, scenario: dict) -> None:
        """Purge RBD images or CephFS subvolume snapshots left after backup tests."""
        storage = scenario["dataset"]["sc"]
        ssh = self._OadpWorkloads__ssh
        ceph_pod = ssh.run(
            cmd="oc get pods -n openshift-storage --field-selector status.phase=Running "
            '--no-headers -o custom-columns=":metadata.name" | grep tools'
        )
        if ceph_pod == "":
            logger.warning(":: WARN :: clean_odf_pool was not run ")
            return

        purg_csi_snap_and_rm_in_one = """'
        for i in $(rbd ls --pool=ocs-storagecluster-cephblockpool); do
            res=$(rbd status --pool=ocs-storagecluster-cephblockpool $i)
            echo "rbd status --pool=ocs-storagecluster-cephblockpool $i ==> $res"
            if [[ $res == "Watchers: none" ]]; then
                echo $i
                rbd snap purge --pool=ocs-storagecluster-cephblockpool $i
                rbd rm --pool=ocs-storagecluster-cephblockpool $i
            fi
        done'
        """
        purge_cephfs = """'
        for i in $(ceph fs subvolume ls ocs-storagecluster-cephfilesystem csi | grep csi | awk "{print \\$2}" | tr -d "\\""); do
            res=$(ceph fs subvolume info ocs-storagecluster-cephfilesystem $i csi | grep state | awk "{print \\$2}" | tr -d "\\",")
            echo "Status CSI VOL:" $res "CSI Vol: " $i
            if [[ $res == "snapshot-retained" ]]; then
                for j in $(ceph fs subvolume snapshot ls ocs-storagecluster-cephfilesystem $i csi | grep csi-snap | awk "{print \\$2}" | tr -d "\\""); do
                    echo "Delete CSI State:" $res "CSI Vol: " $i "Snapshotname: " $j
                    ceph fs subvolume snapshot rm ocs-storagecluster-cephfilesystem $i $j csi
                done
            fi
        done'
        """

        if storage == "ocs-storagecluster-ceph-rbd":
            ssh.run(
                cmd=f"oc exec -n openshift-storage {ceph_pod} -- /bin/bash -c 'rbd ls --pool=ocs-storagecluster-cephblockpool'"
            )
            num_of_csi_snaps_found = ssh.run(
                cmd=f"oc exec -n openshift-storage {ceph_pod} -- /bin/bash -c "
                f"'rbd ls --pool=ocs-storagecluster-cephblockpool' | wc -l"
            )
            logger.info(f"::: INFO :: clean_odf_pool has found {num_of_csi_snaps_found} will attempt purge")
            purg_result = ssh.run(cmd=f"oc exec -n openshift-storage {ceph_pod} -- /bin/bash -c {purg_csi_snap_and_rm_in_one}")
            logger.info(f"::: INFO :: clean_odf_pool attempted purge and rm output: {purg_result}")
            num_of_csi_snaps_found_postclean = ssh.run(
                cmd=f"oc exec -n openshift-storage {ceph_pod} -- /bin/bash -c "
                f"'rbd ls --pool=ocs-storagecluster-cephblockpool' | wc -l"
            )
            logger.info(f"::: INFO :: clean_odf_pool POST removal has found {num_of_csi_snaps_found_postclean}")

        if storage in ("ocs-storagecluster-cephfs", "ocs-storagecluster-cephfs-shallow"):
            purge_cephfs_result = ssh.run(cmd=f"oc exec -n openshift-storage {ceph_pod} -- /bin/bash -c {purge_cephfs}")
            logger.info(f"::: INFO :: clean_odf_pool attempted cleanup of cephfs resulting in: {purge_cephfs_result}")

    @logger_time_stamp
    def delete_vsc(self, scenario: dict, ns_scoped: bool) -> None:
        """Remove volume snapshot content cluster-wide or namespace-scoped per scenario plugin."""
        cr_type = scenario["args"]["OADP_CR_TYPE"]
        cr_name = scenario["args"]["OADP_CR_NAME"]
        target_namespace = scenario["args"]["namespaces_to_backup"]

        if ns_scoped:
            logger.info("::: INFO :: Attempting VSC Clean via vsc_clean.sh script meaning scoped to specific ns")
            vsc_cmd = self._OadpWorkloads__ssh.run(
                cmd=f"{self._OadpWorkloads__oadp_misc_dir}/vsc_clean.sh {target_namespace} {cr_type} {cr_name}"
            )
        else:
            logger.info("::: INFO :: Attempting VSC Clean ALL vsc")
            if scenario["args"]["plugin"] != PLUGIN_VBD:
                vsc_removal_all = (
                    """for i in `oc get vsc -A -o custom-columns=NAME:.metadata.name`; """
                    """do echo $i; oc patch vsc $i -p '{"metadata":{"finalizers":null}}' --type=merge; done"""
                )
            else:
                vsc_removal_all = (
                    "oc delete vsb -A --all; oc delete vsr -A --all; oc delete vsc -A --all; "
                    "oc delete vs -A --all; oc delete replicationsources.volsync.backube -A --all; "
                    "oc delete replicationdestination.volsync.backube -A --all"
                )
            logger.info(f"::: INFO :: delete_vsc Attempting command {vsc_removal_all}")
            vsc_cmd = self._OadpWorkloads__ssh.run(cmd=vsc_removal_all)
        logger.info(f"::: INFO :: delete_vsc result of command with scope ns_scoped: {ns_scoped} was {vsc_cmd}")

    @logger_time_stamp
    def delete_oadp_custom_resources(self, ns: str, cr_type: str, cr_name: str) -> bool | None:
        """Delete Velero backup or restore CRs by name or wildcard and wait for removal."""
        try:
            self.oadp_timer(action="start", transaction_name="Delete existing OADP CR")
            test_env = self._OadpWorkloads__test_env
            ssh = self._OadpWorkloads__ssh
            if cr_name == "*":
                list_of_crs_to_delete = self.get_custom_resources(cr_type, ns)
                if len(list_of_crs_to_delete) > 0:
                    for i in range(len(list_of_crs_to_delete)):
                        if test_env["source"] == SOURCE_DOWNSTREAM:
                            del_cmd = ssh.run(
                                cmd=f"oc -n {ns} exec deployment/velero -c velero -it -- "
                                f"./velero {cr_type} delete {list_of_crs_to_delete[i]} --confirm"
                            )
                        if test_env["source"] == SOURCE_UPSTREAM:
                            del_cmd = ssh.run(
                                cmd=f"cd {test_env['velero_cli_path']}/velero/cmd/velero; "
                                f"./velero {cr_type} delete {list_of_crs_to_delete[i]} --confirm -n {ns}"
                            )
                        if del_cmd.find("submitted successfully") < 0:
                            logger.warning("Error did not delete successfully")
            else:
                if cr_name != "*" and cr_name != "":
                    if test_env["source"] == SOURCE_DOWNSTREAM:
                        del_cmd = ssh.run(
                            cmd=f"oc -n {ns} exec deployment/velero -c velero -it -- "
                            f"./velero {cr_type} delete {cr_name} --confirm"
                        )
                    if test_env["source"] == SOURCE_UPSTREAM:
                        del_cmd = ssh.run(
                            cmd=f"cd {test_env['velero_cli_path']}/velero/cmd/velero; "
                            f"./velero {cr_type} delete {cr_name} --confirm -n {ns}"
                        )
                    if del_cmd is not None and ("submitted successfully" in del_cmd or "deleted" in del_cmd):
                        logger.info(f":: INFO :: OADP CR deleted by: velero {cr_type} delete {cr_name} completed successfully")
                    elif del_cmd is not None and f"No {cr_type}s found" in del_cmd:
                        logger.info("CR not found so delete failed as it doesnt exist")
                    else:
                        logger.info(f"=== Attempt to delete was not successful the output was: {del_cmd}")
            self.oadp_timer(action="stop", transaction_name="Delete existing OADP CR")
            try:
                is_present = self.is_cr_present(ns=ns, cr_type=cr_type, cr_name=cr_name)
                if is_present:
                    time_spent_waiting_for_deletion = 0
                    while time_spent_waiting_for_deletion < 900:
                        is_present = self.is_cr_present(ns=ns, cr_type=cr_type, cr_name=cr_name)
                        if is_present:
                            cr_data = self.get_oc_resource_to_json(resource_type=cr_type, resource_name=cr_name, namespace=ns)
                            if not cr_data:
                                logger.info(f":: info :: delete_oadp_custom_resources {cr_name} in {ns} deletion is completed")
                                return True
                            else:
                                logger.info(
                                    f":: info :: delete_oadp_custom_resources {cr_name} deletion so far "
                                    f"taken: {time_spent_waiting_for_deletion} of 900s"
                                )
                            time.sleep(2)
                            time_spent_waiting_for_deletion += 2
                        else:
                            logger.info(f"::INFO:: {cr_name} no longer found delete has completed")
                            return True
                else:
                    logger.info(f"::INFO:: {cr_name} no longer found delete has completed successfully")
            except Exception as err:
                logger.warning(
                    f":: WARN :: in delete_oadp_custom_resources raised an exception "
                    f"related to {cr_name} deletion attempt {err}"
                )

        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def delete_source_dataset(self, target_namespace: str) -> bool | None:
        """Delete dataset namespaces listed in target_namespace and wait until they are gone."""
        try:
            self.oadp_timer(action="start", transaction_name="delete_oadp_source_dataset")
            for n in target_namespace.split(","):
                del_ns_cmd = self._OadpWorkloads__ssh.run(cmd=f"oc delete ns {n}")
                if del_ns_cmd.find("deleted") < 0:
                    logger.warning(f"attempt to delete namespace {n} failed")
            self.oadp_timer(action="stop", transaction_name="delete_oadp_source_dataset")
            try:
                time_spent_waiting_for_deletion = 0
                while time_spent_waiting_for_deletion < 900:
                    ns_data = self.get_oc_resource_to_json(
                        resource_type="ns",
                        resource_name=target_namespace,
                        namespace=self._OadpWorkloads__test_env["velero_ns"],
                    )
                    if not ns_data:
                        logger.info(f":: info :: NS {target_namespace} deletion is completed")
                        return True
                    else:
                        logger.info(
                            f":: info :: NS {target_namespace} deletion so far taken: "
                            f"{time_spent_waiting_for_deletion} of 900s"
                        )
                        time.sleep(2)
                        time_spent_waiting_for_deletion += 2
            except Exception as err:
                logger.warning(
                    f":: WARN :: in delete_source_dataset :: raised an exception "
                    f"related to {target_namespace} deletion attempt {err}"
                )
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

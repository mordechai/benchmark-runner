"""
Mixin for collecting environment metadata: OCP, Velero, storage, and bucket details.
"""

from __future__ import annotations

import json
import math
import re

from minio import Minio

from benchmark_runner.common.logger.logger_time_stamp import logger, logger_time_stamp
from benchmark_runner.oadp.constants import (
    SOURCE_DOWNSTREAM,
    SOURCE_UPSTREAM,
)


class OadpEnvironmentMixin:
    """OCP/Velero/storage/bucket metadata collection, upstream/downstream detection."""

    @logger_time_stamp
    def set_velero_stream_source(self) -> None:
        """Detect upstream Velero vs downstream OADP and set test_env source accordingly."""
        try:
            test_env = self._OadpWorkloads__test_env
            logger.info(
                f"### INFO ### set_velero_stream_source: Namespace to inspect for upstream/downstream: {test_env['velero_ns']}"
            )
            check_for_oadp_pod_presence = self._OadpWorkloads__ssh.run(
                cmd=f"oc get pods -n {test_env['velero_ns']} --field-selector status.phase=Running "
                f"--no-headers -o custom-columns=':metadata.name' "
                f"| grep -c 'openshift-adp-controller-manager'"
            )
            if check_for_oadp_pod_presence != "1":
                test_env["source"] = SOURCE_UPSTREAM
                logger.info(f":: INFO :: Velero namespace {test_env['velero_ns']} is upstream")
            else:
                test_env["source"] = SOURCE_DOWNSTREAM
                logger.info(f":: INFO :: Velero namespace {test_env['velero_ns']} is downstream")
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def this_is_downstream(self) -> bool:
        """Return True when test_env source is not upstream (OADP downstream)."""
        try:
            if self._OadpWorkloads__test_env["source"] != SOURCE_UPSTREAM:
                return True
            return False
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def get_ocp_details(self) -> None:
        """Collect cluster name, OCP version, and master/worker counts into run_metadata."""
        try:
            ssh = self._OadpWorkloads__ssh
            run_metadata = self._OadpWorkloads__run_metadata

            jsonpath_cluster_name = "'{print $2}'"
            cluster_name = ssh.run(
                cmd=f"oc get route/console -n openshift-console | grep -v NAME | awk {jsonpath_cluster_name}"
            )
            if cluster_name != "":
                run_metadata["summary"]["env"]["ocp"]["cluster"] = cluster_name

            get_ocp_version_cmd = ssh.run(cmd="oc version | grep 'Server Version'")
            ocp_version = get_ocp_version_cmd.split("Version:")[1]
            if ocp_version != "":
                run_metadata["summary"]["env"]["ocp"]["version"] = ocp_version

            get_masters_cmd = ssh.run(
                cmd="""oc get nodes -l node-role.kubernetes.io/master -o jsonpath='{.items[*].metadata.name}'"""
            )
            num_of_masters = len(get_masters_cmd.split(" "))
            if num_of_masters != "":
                run_metadata["summary"]["env"]["ocp"]["num_of_masters"] = num_of_masters

            get_workers_cmd = ssh.run(
                cmd="""oc get nodes -l node-role.kubernetes.io/worker -o jsonpath='{.items[*].metadata.name}'"""
            )
            num_of_workers = len(get_workers_cmd.split(" "))
            if num_of_workers != "":
                run_metadata["summary"]["env"]["ocp"]["num_of_workers"] = num_of_workers

        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def get_velero_details(self) -> None:
        """Populate run_metadata with Velero pod image, args, and version or git info."""
        try:
            ssh = self._OadpWorkloads__ssh
            test_env = self._OadpWorkloads__test_env
            run_metadata = self._OadpWorkloads__run_metadata

            velero_pod_name = ssh.run(
                cmd=f"oc get pods -n {test_env['velero_ns']} --field-selector status.phase=Running "
                f"--no-headers -o custom-columns=':metadata.name' | grep velero"
            )
            if velero_pod_name != "":
                get_velero_pod_details = ssh.run(cmd=f"oc get pods {velero_pod_name} -n {test_env['velero_ns']} -o json")
                data = json.loads(get_velero_pod_details)
                velero_details = {"velero": {}}
                for container in data["spec"]["containers"]:
                    velero_details["velero"]["name"] = container["name"]
                    velero_details["velero"]["args"] = container["args"]
                    velero_details["velero"]["image"] = container["image"]
                    velero_details["velero"]["resources"] = container["resources"]
                if test_env["source"] == SOURCE_DOWNSTREAM:
                    get_velero_version = ssh.run(
                        cmd=f"oc -n {test_env['velero_ns']} exec deployment/velero -c velero -it -- "
                        f"./velero version | grep Version: | tail -n -1"
                    )
                    velero_version = get_velero_version.split("Version:")[1]
                    if velero_version != "":
                        velero_details["velero"]["velero_version"] = velero_version
                if test_env["source"] == SOURCE_UPSTREAM:
                    get_velero_branch = ssh.run(cmd=f"cd {test_env['velero_cli_path']}/velero; git branch")
                    velero_details["velero"]["version"] = get_velero_branch.split("*")[-1].strip().replace("release-", "")
                    velero_details["velero"]["branch"] = get_velero_branch.split("*")[-1].strip()
                    velero_details["velero"]["commit"] = ssh.run(
                        cmd=f"cd {test_env['velero_cli_path']}/velero; git rev-parse HEAD"
                    )
                run_metadata["summary"]["env"].update(velero_details)
                self._OadpWorkloads__result_dicts.append(run_metadata["summary"]["env"])
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def get_storage_details(self) -> None:
        """Gather storage classes, Ceph version, default SC, and ODF CSV version into metadata."""
        try:
            ssh = self._OadpWorkloads__ssh
            run_metadata = self._OadpWorkloads__run_metadata
            storage_details = {}
            get_sc_cmd = ssh.run(cmd="oc get sc --no-headers | awk '{print $1}'")
            if len(get_sc_cmd.splitlines()) > 0:
                storage_details["storage_class"] = get_sc_cmd.splitlines()
            ceph_pod = ssh.run(
                cmd="oc get pods -n openshift-storage --field-selector status.phase=Running "
                '--no-headers -o custom-columns=":metadata.name" | grep tools'
            )
            if ceph_pod != "":
                ceph_version_cmd = ssh.run(cmd=f"oc -n openshift-storage rsh {ceph_pod} ceph version")
                if ceph_version_cmd.split("ceph version ")[1] != "":
                    storage_details["ceph"] = ceph_version_cmd.split("ceph version ")[1]
            default_storage_class = self.get_default_storage_class()
            storage_details["default_storage_class"] = default_storage_class
            openshift_storage_version_cmd = ssh.run(
                cmd="oc -n openshift-storage get csv -o yaml | grep full_version | tail -n 1"
            )
            openshift_storage_version = openshift_storage_version_cmd.split("full_version: ")[1]
            storage_details["openshift_storage_version"] = openshift_storage_version
            self._OadpWorkloads__result_dicts.append(storage_details)
            run_metadata["summary"]["env"]["storage"].update(storage_details)
            self._OadpWorkloads__result_dicts.append(run_metadata["summary"]["env"]["storage"])
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def get_default_storage_class(self) -> str:
        """Return the name of the default StorageClass from the cluster."""
        jsonpath_check_default_sc = (
            """'{.items[?(@.metadata.annotations.storageclass\\.kubernetes\\.io/"""
            """is-default-class=="true")].metadata.name}'"""
        )
        return self._OadpWorkloads__ssh.run(cmd=f"oc get storageclass -o=jsonpath={jsonpath_check_default_sc}")

    @logger_time_stamp
    def get_noobaa_version_details(self) -> None:
        """Read NooBaa CR spec and status into run_metadata storage section."""
        ssh = self._OadpWorkloads__ssh
        get_noobaa_details = ssh.run(cmd="oc get noobaa -n openshift-storage -o json")
        data = json.loads(get_noobaa_details)
        noobaa_details = {"noobaa": {}}
        dbimage = data["items"][0]["spec"]["dbImage"]
        noobaa_details["noobaa"]["db_image"] = dbimage
        noobaa_details["noobaa"]["db_storage_class"] = data["items"][0]["spec"]["dbStorageClass"]
        noobaa_details["noobaa"]["db_volume_resource"] = data["items"][0]["spec"]["dbVolumeResources"]
        noobaa_details["noobaa"]["endpoints"] = data["items"][0]["spec"]["endpoints"]
        noobaa_details["noobaa"]["core_resources"] = data["items"][0]["spec"]["coreResources"]
        for line in data["items"][0]["status"]["readme"].split("\n"):
            if "NooBaa Core Version:" in line:
                noobaa_details["noobaa"]["noobaa_core_version"] = line.split("NooBaa Core Version:")[1].strip()
            if "NooBaa Operator Version:" in line:
                noobaa_details["noobaa"]["noobaa_operator_version"] = line.split("NooBaa Operator Version:")[1].strip()
        self._OadpWorkloads__run_metadata["summary"]["env"]["storage"].update(noobaa_details)
        self._OadpWorkloads__result_dicts.append(noobaa_details)

    def get_s3_details_from_dpa(self) -> tuple[bool, str, str]:
        """Return whether MinIO matches DPA S3 URL, endpoint host, and bucket name from DPA."""
        try:
            dpa_data = self.get_oc_resource_to_json(
                resource_type="dpa",
                resource_name=self._OadpWorkloads__oadp_dpa,
                namespace=self._OadpWorkloads__test_env["velero_ns"],
            )
            if not dpa_data:
                self.fail_test_run(
                    " Attempts to get bucket info from the dpa returned an empty dpa this occurred in "
                    + self.get_current_function()
                )
            bucket_name_from_dpa = dpa_data["spec"]["backupLocations"][0]["velero"]["objectStorage"]["bucket"]
            s3_url_from_dpa = dpa_data["spec"]["backupLocations"][0]["velero"]["config"]["s3Url"]
            s3_url_from_dpa = s3_url_from_dpa.split("//")[-1]
            minio_route = self._OadpWorkloads__ssh.run(
                cmd="oc get route -n minio-bucket --field-selector metadata.name=minio "
                '--no-headers -o custom-columns=":spec.host"'
            )
            minio_deployed_locally = minio_route == s3_url_from_dpa
            return minio_deployed_locally, s3_url_from_dpa, bucket_name_from_dpa
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    def get_bucket_details(self, folder_prefix: str) -> None:
        """Scan Minio bucket objects under folder_prefix and update run_metadata storage bucket stats."""
        total_size = 0
        bucket_details = {
            "bucket": {
                "minio_deployed_locally": "",
                "endpoint": "",
                "bucket_name": "",
                "bucket_creation_date": "",
                "total_bucket_size": "",
                "bucket_utilization_in_bytes": "",
                "bucket_capacity_in_bytes": "",
            }
        }
        try:
            minio_bucket = self._OadpWorkloads__oadp_minio_bucket
            minio_deployed_locally, endpoint, bucket_name = self.get_s3_details_from_dpa()

            client = Minio(
                endpoint,
                access_key=minio_bucket["access_key"],
                secret_key=minio_bucket["secret_key"],
                secure=False,
            )

            total_objs_in_bucket = 0
            for bucket in client.list_buckets():
                if bucket.name == bucket_name:
                    bucket_details["bucket"]["bucket_creation_date"] = bucket.creation_date

            objects = client.list_objects(bucket_name, prefix=folder_prefix, recursive=True)
            logger.info(
                "### INFO ### minio_details: is about to count objects in the minio bucket this make take a few minutes"
            )
            for obj in objects:
                total_size += obj.size
                total_objs_in_bucket += 1

            if minio_deployed_locally != "":
                bucket_details["bucket"]["minio_deployed_locally"] = minio_deployed_locally
            if endpoint != "":
                bucket_details["bucket"]["endpoint"] = endpoint
            if bucket_name != "":
                bucket_details["bucket"]["bucket_name"] = bucket_name
            if total_objs_in_bucket != "":
                bucket_details["bucket"]["total_objs_in_bucket_before"] = total_objs_in_bucket
            if total_size != "":
                bucket_details["bucket"]["total_bytes_in_bucket_before"] = total_size

            if total_size == 0:
                bucket_utilization = 0
            else:
                size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
                i = int(math.floor(math.log(total_size, 1024)))
                p = math.pow(1024, i)
                s = round(total_size / p, 2)
                bucket_utilization = f"{s} {size_name[i]}"

            if minio_deployed_locally:
                minio_pvc = self.get_oc_resource_to_json(
                    resource_type="pvc", resource_name="minio-pvc", namespace="minio-bucket"
                )
                if not minio_pvc:
                    self.fail_test_run(
                        " Attempts to get bucket's pvc size details from local cluster returned empty "
                        "for namespace='minio-bucket' this occurred in " + self.get_current_function()
                    )
                else:
                    minio_pvc_capacity = minio_pvc["spec"]["resources"]["requests"]["storage"]
                    bucket_capacity_in_bytes = self.capacity_in_bytes(minio_pvc_capacity)
                    if bucket_capacity_in_bytes != "":
                        bucket_details["bucket"]["bucket_capacity_in_bytes"] = bucket_capacity_in_bytes
                        logger.info(
                            f" Minio bucket contains {bucket_utilization} of objs, "
                            f"total bucket capacity is {minio_pvc_capacity}"
                        )
                    bucket_details["bucket"]["total_bucket_size"] = minio_pvc["spec"]["resources"]["requests"]["storage"]
            self._OadpWorkloads__run_metadata["summary"]["env"]["storage"].update(bucket_details)

        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def oadp_get_version_info(self) -> None:
        """Collect OADP operator image, CSV, subscription, catalog, and cluster topology into metadata."""
        oadp_details = {"oadp": {}}
        try:
            ssh = self._OadpWorkloads__ssh
            test_env = self._OadpWorkloads__test_env
            run_metadata = self._OadpWorkloads__run_metadata

            jsonpath_oadp_operator_container_image = "'{.spec.template.spec.containers[0].image}'"
            oadp_operator_container_image = ssh.run(
                cmd=f"oc get deployment openshift-adp-controller-manager "
                f"--namespace={test_env['velero_ns']} -o jsonpath={jsonpath_oadp_operator_container_image}"
            )
            if oadp_operator_container_image != "":
                oadp_details["oadp"]["oadp_operator_container_image"] = oadp_operator_container_image

            jsonpath_oadp_csv = r"'{.items[0].metadata.labels.olm\.owner}'"
            oadp_csv = ssh.run(
                cmd=f"oc get deployments --all-namespaces "
                f"--field-selector='metadata.name={test_env['velero_ns']}-controller-manager' "
                f"-o jsonpath={jsonpath_oadp_csv}"
            )
            if oadp_csv != "":
                oadp_details["oadp"]["oadp_csv"] = oadp_csv

            jsonpath_oadp_csv_creation_time = "'{.items[0].metadata.annotations.createdAt}'"
            oadp_csv_creation_time = ssh.run(
                cmd=f"oc get -n {test_env['velero_ns']} csv -o jsonpath={jsonpath_oadp_csv_creation_time}"
            )
            if oadp_csv_creation_time != "":
                oadp_details["oadp"]["oadp_csv_creation_time"] = oadp_csv_creation_time.split(".")[0]

            jsonpath_oadp_subscription_used = "{.items[?(@.status.installedCSV == " + f'"{oadp_csv}"' + ")].metadata.name}"
            oadp_subscription_used = ssh.run(
                cmd=f"oc get subscription.operators.coreos.com --namespace {test_env['velero_ns']} "
                f"-o jsonpath='{jsonpath_oadp_subscription_used}'"
            )
            if oadp_subscription_used != "":
                oadp_details["oadp"]["subscription"] = oadp_subscription_used

            jsonpath_oadp_catalog_source = "'{.spec.source}'"
            oadp_catalog_source = ssh.run(
                cmd=f"oc get subscription.operators.coreos.com {oadp_subscription_used} "
                f"--namespace {test_env['velero_ns']} -o jsonpath={jsonpath_oadp_catalog_source}"
            )
            if oadp_catalog_source != "":
                oadp_details["oadp"]["catalog_source"] = oadp_catalog_source
                jsonpath_oadp_iib = "'{.spec.image}'"
                oadp_iib_cmd = ssh.run(
                    cmd=f"oc get catsrc {oadp_catalog_source} -n openshift-marketplace "
                    f"-o jsonpath={jsonpath_oadp_iib} --ignore-not-found | grep -Eo 'iib:[0-9]+'"
                )
                if oadp_iib_cmd != "":
                    oadp_internal_build = ssh.run(
                        cmd="curl -s -k https://datagrepper.engineering.redhat.com/raw"
                        r"\?topic\=/topic/VirtualTopic.eng.ci.redhat-container-image.index.built"
                        rf"\&contains\={oadp_iib_cmd}\&rows_per_page\=1\&delta\=15552000"
                        " | jq -r '.raw_messages[0].msg.artifact.nvr'"
                    )
                    if oadp_internal_build != "":
                        logger.info(f"DataGrepper Curl command returned {oadp_internal_build}")
                        if "oadp-operator-bundle-container-" in oadp_internal_build:
                            oadp_details["oadp"]["internal_build"] = oadp_internal_build.split(
                                "oadp-operator-bundle-container-"
                            )[1]
                        if "iib:" in oadp_iib_cmd:
                            oadp_details["oadp"]["iib"] = oadp_iib_cmd.split("iib:")[1]
            else:
                oadp_details["oadp"]["internal_build"] = re.search(r"\w+-\w+\.\w([\d\.]+)", oadp_csv).group(1)
                logger.info(
                    f"::: INFO :: OADP internal build not available will parse from operator csv "
                    f"for major version {oadp_details['oadp']['internal_build']}"
                )

            jsonpath_cluster_name = "'{print $2}'"
            cluster_name = ssh.run(
                cmd=f"oc get route/console -n openshift-console | grep -v NAME | awk {jsonpath_cluster_name}"
            )
            if cluster_name != "":
                run_metadata["summary"]["env"]["ocp"]["cluster"] = cluster_name

            get_ocp_version_cmd = ssh.run(cmd="oc version | grep 'Server Version'")
            ocp_version = get_ocp_version_cmd.split("Version:")[1]
            if ocp_version != "":
                run_metadata["summary"]["env"]["ocp"]["version"] = ocp_version

            get_masters_cmd = ssh.run(
                cmd="""oc get nodes -l node-role.kubernetes.io/master -o jsonpath='{.items[*].metadata.name}'"""
            )
            num_of_masters = len(get_masters_cmd.split(" "))
            if num_of_masters != "":
                run_metadata["summary"]["env"]["ocp"]["num_of_masters"] = num_of_masters

            get_workers_cmd = ssh.run(
                cmd="""oc get nodes -l node-role.kubernetes.io/worker -o jsonpath='{.items[*].metadata.name}'"""
            )
            num_of_workers = len(get_workers_cmd.split(" "))
            if num_of_workers != "":
                run_metadata["summary"]["env"]["ocp"]["num_of_workers"] = num_of_workers

            run_metadata["summary"]["env"].update(oadp_details)
            self._OadpWorkloads__result_dicts.append(run_metadata["summary"]["env"])

        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

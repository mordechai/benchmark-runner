"""
Mixin for collecting OADP diagnostic artifacts.

Replaces the external ``log_collector.sh`` dependency with native Python
methods that gather Velero describe output, BSL/DPA/CR YAML, pod logs,
backup repositories, bucket content, pod distribution, plugin objects,
and cluster events into the run artifacts directory.
"""

from __future__ import annotations

import json
import os
from datetime import datetime

from minio import Minio

from benchmark_runner.common.logger.logger_time_stamp import logger, logger_time_stamp
from benchmark_runner.oadp.constants import (
    CR_TIMESTAMP_FORMAT,
    DEFAULT_DPA_NAME,
    PLUGIN_CSI,
    PLUGIN_KOPIA,
    PLUGIN_VBD,
    SOURCE_UPSTREAM,
    VM_DATASET_ROLE,
)


class OadpLogCollectionMixin:
    """Diagnostic artifact collection for OADP workload runs."""

    def _ensure_log_dirs(self, logs_folder: str, scenario: dict) -> None:
        """Create the subdirectory tree needed by the collection methods."""
        plugin = scenario["args"]["plugin"]
        testtype = scenario["args"]["OADP_CR_TYPE"]

        subdirs = [
            "Backupstoragelocations",
            "DPA",
            "bucket_content",
            "events",
        ]
        if plugin != PLUGIN_CSI:
            subdirs.append("Backuprepositories")

        plugin_dir = self._plugin_artifact_dir(plugin, testtype)
        if plugin_dir:
            subdirs.append(plugin_dir)
            subdirs.append(os.path.join(plugin_dir, "YamlObjects"))

        for subdir in subdirs:
            os.makedirs(os.path.join(logs_folder, subdir), exist_ok=True)

    @staticmethod
    def _plugin_artifact_dir(plugin: str, testtype: str) -> str | None:
        """Return uppercase plugin dir name for kopia/vbd, or None otherwise."""
        if plugin == PLUGIN_KOPIA and testtype in ("backup", "restore"):
            return "KOPIA"
        if plugin == PLUGIN_VBD and testtype in ("backup", "restore"):
            return "VBD"
        return None

    # ------------------------------------------------------------------
    # Individual artifact collectors (best-effort: log and continue)
    # ------------------------------------------------------------------

    @logger_time_stamp
    def collect_scenario_summary(self, logs_folder: str, scenario: dict, velero_ns: str) -> None:
        """Write CR status and full scenario info to ``scenario_summary.log``."""
        try:
            ssh = self._OadpWorkloads__ssh
            cr_name = scenario["args"]["OADP_CR_NAME"]
            testtype = scenario["args"]["OADP_CR_TYPE"]
            testcase = scenario.get("testcase", "")
            summary_file = os.path.join(logs_folder, "scenario_summary.log")

            cr_status = ssh.run(cmd=f"oc get {testtype}.velero.io {cr_name} -n {velero_ns} -o jsonpath='{{.status.phase}}'")

            timestamp = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
            source = self._OadpWorkloads__test_env["source"]

            lines = [
                f"Results summary - {timestamp}",
                f"Current OADP stream version is: {source}",
                f"Testcase number:{testcase} on Operation:{testtype} on CR name:{cr_name} CR status is:{cr_status}",
                "Total testcase info : ",
                json.dumps(scenario, indent=4, sort_keys=True, default=str),
            ]

            with open(summary_file, "w", encoding="utf-8") as fh:
                fh.write("\n".join(lines) + "\n")

            if cr_status != "Completed":
                logger.warning(f"CR {cr_name} status is {cr_status} (not Completed) during log collection")
        except Exception as err:
            logger.warning(f"collect_scenario_summary failed: {err}")

    @logger_time_stamp
    def collect_velero_describe(self, logs_folder: str, scenario: dict, velero_ns: str) -> None:
        """Collect ``velero describe`` output as text (and JSON for backups)."""
        try:
            ssh = self._OadpWorkloads__ssh
            test_env = self._OadpWorkloads__test_env
            cr_name = scenario["args"]["OADP_CR_NAME"]
            testtype = scenario["args"]["OADP_CR_TYPE"]
            describe_txt = os.path.join(logs_folder, f"{cr_name}.txt")
            describe_json = os.path.join(logs_folder, f"{cr_name}.json")

            if test_env["source"] == SOURCE_UPSTREAM:
                cli = f"cd {test_env['velero_cli_path']}/velero/cmd/velero; ./velero"
                ssh.run(cmd=f"{cli} {testtype} describe {cr_name} -n {velero_ns} --details >> {describe_txt}")
                if testtype == "backup":
                    ssh.run(cmd=f"{cli} {testtype} describe {cr_name} -n {velero_ns} --details -ojson >> {describe_json}")
            else:
                exec_prefix = f"oc -n {velero_ns} exec deployment/velero -c velero -it -- ./velero"
                ssh.run(cmd=f"{exec_prefix} {testtype} describe {cr_name} --details >> {describe_txt}")
                if testtype == "backup":
                    ssh.run(cmd=f"{exec_prefix} {testtype} describe {cr_name} --details -ojson >> {describe_json}")
        except Exception as err:
            logger.warning(f"collect_velero_describe failed: {err}")

    @logger_time_stamp
    def collect_bsl_yaml(self, logs_folder: str, velero_ns: str) -> None:
        """Collect BackupStorageLocations YAML."""
        try:
            bsl_file = os.path.join(logs_folder, "Backupstoragelocations", "Backupstoragelocations.yml")
            self._OadpWorkloads__ssh.run(cmd=f"oc get Backupstoragelocations -n {velero_ns} -o yaml >> {bsl_file}")
        except Exception as err:
            logger.warning(f"collect_bsl_yaml failed: {err}")

    @logger_time_stamp
    def collect_dpa_yaml(self, logs_folder: str, scenario: dict, velero_ns: str) -> None:
        """Collect DataProtectionApplication YAML (downstream only)."""
        try:
            testtype = scenario["args"]["OADP_CR_TYPE"]
            plugin = scenario["args"]["plugin"]
            dpa_file = os.path.join(logs_folder, "DPA", f"dpa_{testtype}_{plugin}.yml")
            self._OadpWorkloads__ssh.run(cmd=f"oc get dpa/{DEFAULT_DPA_NAME} -n {velero_ns} -o yaml >> {dpa_file}")
        except Exception as err:
            logger.warning(f"collect_dpa_yaml failed: {err}")

    @logger_time_stamp
    def collect_cr_yaml(self, logs_folder: str, scenario: dict, velero_ns: str) -> None:
        """Collect the backup/restore CR as YAML (downstream only)."""
        try:
            testtype = scenario["args"]["OADP_CR_TYPE"]
            cr_name = scenario["args"]["OADP_CR_NAME"]
            cr_file = os.path.join(logs_folder, f"{testtype}_CR.yml")
            self._OadpWorkloads__ssh.run(cmd=f"oc get {testtype}.velero.io {cr_name} -n {velero_ns} -o yaml >> {cr_file}")
        except Exception as err:
            logger.warning(f"collect_cr_yaml failed: {err}")

    @logger_time_stamp
    def collect_velero_ns_pod_logs(self, logs_folder: str, velero_ns: str) -> None:
        """Collect ``oc logs`` for every pod in the Velero namespace."""
        try:
            ssh = self._OadpWorkloads__ssh
            pod_list = ssh.run(cmd=f'oc get pods -n {velero_ns} --no-headers -o custom-columns=":metadata.name"')
            if not pod_list.strip():
                logger.warning(f"No pods found in namespace {velero_ns}")
                return
            for pod_name in pod_list.strip().splitlines():
                pod_name = pod_name.strip()
                if pod_name:
                    log_file = os.path.join(logs_folder, f"{pod_name}.log")
                    ssh.run(cmd=f"oc logs {pod_name} -n {velero_ns} >> {log_file} 2>&1")
        except Exception as err:
            logger.warning(f"collect_velero_ns_pod_logs failed: {err}")

    @logger_time_stamp
    def collect_backup_repositories(self, logs_folder: str, scenario: dict, velero_ns: str) -> None:
        """Collect BackupRepositories YAML (non-CSI plugins only)."""
        try:
            testtype = scenario["args"]["OADP_CR_TYPE"]
            plugin = scenario["args"]["plugin"]
            repo_file = os.path.join(
                logs_folder,
                "Backuprepositories",
                f"Backuprepositories_{testtype}_{plugin}.yml",
            )
            self._OadpWorkloads__ssh.run(cmd=f"oc get Backuprepositories -n {velero_ns} -o yaml >> {repo_file}")
        except Exception as err:
            logger.warning(f"collect_backup_repositories failed: {err}")

    @logger_time_stamp
    def collect_bucket_content(self, logs_folder: str) -> None:
        """List S3 bucket objects via the Minio client and write artifact files."""
        try:
            minio_bucket = self._OadpWorkloads__oadp_minio_bucket
            _, endpoint, bucket_name = self.get_s3_details_from_dpa()

            client = Minio(
                endpoint,
                access_key=minio_bucket["access_key"],
                secret_key=minio_bucket["secret_key"],
                secure=False,
            )

            bucket_dir = os.path.join(logs_folder, "bucket_content")
            objects_list = []
            total_size = 0

            for obj in client.list_objects(bucket_name, recursive=True):
                objects_list.append(
                    {
                        "key": obj.object_name,
                        "size": obj.size,
                        "last_modified": str(obj.last_modified),
                        "etag": obj.etag,
                    }
                )
                total_size += obj.size

            summary = {
                "bucket_name": bucket_name,
                "endpoint": endpoint,
                "total_objects": len(objects_list),
                "total_size_bytes": total_size,
                "objects": objects_list,
            }

            json_file = os.path.join(bucket_dir, "list_objects.json")
            with open(json_file, "w", encoding="utf-8") as fh:
                json.dump(summary, fh, indent=4, default=str)

            txt_file = os.path.join(bucket_dir, "list_objects_summarize.txt")
            with open(txt_file, "w", encoding="utf-8") as fh:
                for entry in objects_list:
                    fh.write(f"{entry['last_modified']}  {entry['size']:>12}  {entry['key']}\n")
                fh.write(f"\nTotal Objects: {len(objects_list)}\n")
                fh.write(f"Total Size: {total_size} bytes\n")

            logger.info(f"Bucket content collected: {len(objects_list)} objects, {total_size} bytes")
        except Exception as err:
            logger.warning(f"collect_bucket_content failed: {err}")

    @logger_time_stamp
    def collect_pod_distribution(self, logs_folder: str, scenario: dict) -> None:
        """Record running-pod counts per worker node for the backup namespace."""
        try:
            ssh = self._OadpWorkloads__ssh
            cr_namespace = scenario["args"].get("namespaces_to_backup", "")
            if not cr_namespace:
                logger.warning("No namespaces_to_backup in scenario, skipping pod distribution")
                return

            dist_file = os.path.join(logs_folder, "pods_distribution_per_worker.txt")

            cluster_info = ssh.run(cmd="oc cluster-info | awk -F ' ' '/https/ {print $7; exit}'")
            worker_nodes = ssh.run(
                cmd='oc get nodes -l node-role.kubernetes.io/worker --no-headers -o custom-columns=":metadata.name"'
            )

            if not worker_nodes.strip():
                logger.warning("No worker nodes found for pod distribution")
                return

            lines = [
                f"Pod distribution on all worker nodes for cluster: {cluster_info.strip()}",
                f"For namespace: {cr_namespace}",
                "As follows:",
            ]
            total_running = 0
            node_num = 1

            for node in worker_nodes.strip().splitlines():
                node = node.strip()
                if not node:
                    continue
                count_str = ssh.run(
                    cmd=f"oc get pod -n {cr_namespace} "
                    f"--field-selector=spec.nodeName={node} "
                    f"--no-headers --ignore-not-found=true 2>/dev/null "
                    f"| grep -c Running"
                )
                try:
                    count = int(count_str.strip())
                except (ValueError, AttributeError):
                    count = 0
                lines.append(f"worker node-{node_num}: {node} has {count} running pods")
                total_running += count
                node_num += 1

            lines.append("===========================================")
            lines.append(f"Total running pods on all worker nodes: {total_running}")

            with open(dist_file, "w", encoding="utf-8") as fh:
                fh.write("\n".join(lines) + "\n")
        except Exception as err:
            logger.warning(f"collect_pod_distribution failed: {err}")

    @logger_time_stamp
    def collect_plugin_objects(self, logs_folder: str, scenario: dict, velero_ns: str) -> None:
        """Collect YAML and duration data for kopia PVB/PVR or vbd DU/DD."""
        plugin = scenario["args"]["plugin"]
        testtype = scenario["args"]["OADP_CR_TYPE"]

        if plugin == PLUGIN_KOPIA:
            resource = "podvolumebackups" if testtype == "backup" else "podvolumerestores"
            self._collect_objects(logs_folder, "KOPIA", resource, velero_ns)
        elif plugin == PLUGIN_VBD:
            resource = "datauploads" if testtype == "backup" else "datadownloads"
            self._collect_objects(logs_folder, "VBD", resource, velero_ns)

    def _collect_objects(
        self,
        logs_folder: str,
        plugin_dir: str,
        resource: str,
        velero_ns: str,
    ) -> None:
        """Fetch individual YAML and timing for each object of the resource type."""
        try:
            ssh = self._OadpWorkloads__ssh
            base_dir = os.path.join(logs_folder, plugin_dir)
            yaml_dir = os.path.join(base_dir, "YamlObjects")
            os.makedirs(yaml_dir, exist_ok=True)

            if plugin_dir == "VBD":
                self._collect_vbd_distribution(ssh, base_dir, plugin_dir, resource, velero_ns)

            objects_file = os.path.join(base_dir, f"{plugin_dir}-Objects.txt")
            ssh.run(cmd=f"oc get {resource} -n {velero_ns} > {objects_file}")

            object_names = ssh.run(cmd=f'oc get {resource} -n {velero_ns} --no-headers -o custom-columns=":metadata.name"')
            if not object_names.strip():
                return

            duration_lines: list[str] = []
            for obj_name in object_names.strip().splitlines():
                obj_name = obj_name.strip()
                if not obj_name:
                    continue

                yaml_file = os.path.join(yaml_dir, f"{obj_name}.yaml")
                ssh.run(cmd=f"oc get {resource} {obj_name} -n {velero_ns} -o yaml > {yaml_file}")

                start_ts = ssh.run(cmd=f"oc get {resource} {obj_name} -n {velero_ns} -o jsonpath='{{.status.startTimestamp}}'")
                end_ts = ssh.run(
                    cmd=f"oc get {resource} {obj_name} -n {velero_ns} -o jsonpath='{{.status.completionTimestamp}}'"
                )

                duration_str = self._calc_timestamp_diff(start_ts.strip(), end_ts.strip())
                duration_lines.append(f"{obj_name} {duration_str}")

            if duration_lines:
                duration_file = os.path.join(base_dir, f"{plugin_dir}-Objects-Duration.txt")
                with open(duration_file, "w", encoding="utf-8") as fh:
                    fh.write("\n".join(duration_lines) + "\n")
        except Exception as err:
            logger.warning(f"_collect_objects failed for {resource}: {err}")

    @staticmethod
    def _collect_vbd_distribution(
        ssh: object,
        base_dir: str,
        plugin_dir: str,
        resource: str,
        velero_ns: str,
    ) -> None:
        """Write per-worker-node object distribution for VBD resources."""
        dist_file = os.path.join(base_dir, f"{plugin_dir}-Objects-Distribution.txt")
        worker_nodes = ssh.run(
            cmd='oc get nodes -l node-role.kubernetes.io/worker --no-headers -o custom-columns=":metadata.name"'
        )
        with open(dist_file, "w", encoding="utf-8") as fh:
            for node in worker_nodes.strip().splitlines():
                node = node.strip()
                if node:
                    count = ssh.run(cmd=f"oc get {resource} -n {velero_ns} --no-headers 2>/dev/null | grep -c {node}")
                    fh.write(f"{node} : {count.strip()}\n")

    @staticmethod
    def _calc_timestamp_diff(start_ts: str, end_ts: str) -> str:
        """Return ``HH:MM:SS`` duration between two ISO-8601 timestamps."""
        try:
            start = datetime.strptime(start_ts, CR_TIMESTAMP_FORMAT)
            end = datetime.strptime(end_ts, CR_TIMESTAMP_FORMAT)
            total_seconds = int((end - start).total_seconds())
            hours, remainder = divmod(abs(total_seconds), 3600)
            minutes, seconds = divmod(remainder, 60)
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        except (ValueError, TypeError):
            return "00:00:00"

    @logger_time_stamp
    def collect_cluster_events(self, logs_folder: str, scenario_name: str) -> None:
        """Collect cluster-wide events sorted by creation timestamp."""
        try:
            events_dir = os.path.join(logs_folder, "events")
            os.makedirs(events_dir, exist_ok=True)
            events_file = os.path.join(events_dir, f"events_logs_{scenario_name}.json")
            self._OadpWorkloads__ssh.run(
                cmd=f"oc get events --all-namespaces --sort-by=.metadata.creationTimestamp -o wide >> {events_file}"
            )
        except Exception as err:
            logger.warning(f"collect_cluster_events failed: {err}")

    # ------------------------------------------------------------------
    # KubeVirt VM artifact collectors
    # ------------------------------------------------------------------

    @logger_time_stamp
    def collect_vm_describe(self, logs_folder: str, scenario: dict) -> None:
        """Collect ``oc describe vm`` for every VM in the backup namespace."""
        try:
            namespace = scenario["args"].get("namespaces_to_backup", "")
            if not namespace:
                return
            ssh = self._OadpWorkloads__ssh
            vm_dir = os.path.join(logs_folder, "VirtualMachines")
            os.makedirs(vm_dir, exist_ok=True)
            vm_list = ssh.run(cmd=f'oc get vm -n {namespace} --no-headers -o custom-columns=":metadata.name" 2>/dev/null')
            if not vm_list.strip():
                return
            for vm_name in vm_list.strip().splitlines():
                vm_name = vm_name.strip()
                if vm_name:
                    out_file = os.path.join(vm_dir, f"{vm_name}-describe.txt")
                    ssh.run(cmd=f"oc describe vm {vm_name} -n {namespace} > {out_file} 2>&1")
        except Exception as err:
            logger.warning(f"collect_vm_describe failed: {err}")

    @logger_time_stamp
    def collect_dv_status(self, logs_folder: str, scenario: dict) -> None:
        """Collect DataVolume status for the backup namespace."""
        try:
            namespace = scenario["args"].get("namespaces_to_backup", "")
            if not namespace:
                return
            dv_file = os.path.join(logs_folder, "datavolumes_status.yaml")
            self._OadpWorkloads__ssh.run(cmd=f"oc get dv -n {namespace} -o yaml > {dv_file} 2>&1")
        except Exception as err:
            logger.warning(f"collect_dv_status failed: {err}")

    @logger_time_stamp
    def collect_vm_events(self, logs_folder: str, scenario: dict) -> None:
        """Collect events from the backup namespace filtered for VM resources."""
        try:
            namespace = scenario["args"].get("namespaces_to_backup", "")
            if not namespace:
                return
            events_file = os.path.join(logs_folder, "vm_events.txt")
            self._OadpWorkloads__ssh.run(
                cmd=f"oc get events -n {namespace} --sort-by=.metadata.creationTimestamp -o wide > {events_file} 2>&1"
            )
        except Exception as err:
            logger.warning(f"collect_vm_events failed: {err}")

    @logger_time_stamp
    def collect_vm_pvc_status(self, logs_folder: str, scenario: dict) -> None:
        """Collect PVC status for the backup namespace (VM-associated PVCs)."""
        try:
            namespace = scenario["args"].get("namespaces_to_backup", "")
            if not namespace:
                return
            pvc_file = os.path.join(logs_folder, "vm_pvc_status.yaml")
            self._OadpWorkloads__ssh.run(cmd=f"oc get pvc -n {namespace} -o yaml > {pvc_file} 2>&1")
        except Exception as err:
            logger.warning(f"collect_vm_pvc_status failed: {err}")

    def _scenario_has_vm_datasets(self, scenario: dict) -> bool:
        """Return True when any dataset in the scenario uses role 'kubevirt'."""
        dataset_value = scenario.get("dataset")
        if isinstance(dataset_value, list):
            return any(d.get("role") == VM_DATASET_ROLE for d in dataset_value)
        if isinstance(dataset_value, dict):
            return dataset_value.get("role") == VM_DATASET_ROLE
        return False

    @logger_time_stamp
    def invoke_vm_log_collection(
        self,
        logs_folder: str,
        scenario: dict,
        velero_ns: str,
    ) -> None:
        """Orchestrate all VM artifact collectors if scenario uses VM datasets."""
        if not self._scenario_has_vm_datasets(scenario):
            return
        self.collect_vm_describe(logs_folder, scenario)
        self.collect_dv_status(logs_folder, scenario)
        self.collect_vm_events(logs_folder, scenario)
        self.collect_vm_pvc_status(logs_folder, scenario)

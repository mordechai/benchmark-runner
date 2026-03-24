"""
Mixin for collecting and comparing node/pod resource utilisation.

Wraps ``oc adm top`` queries and maintains per-pod and per-node
resource snapshots inside ``__run_metadata``.
"""

from __future__ import annotations

import json
import re

from benchmark_runner.common.logger.logger_time_stamp import logger, logger_time_stamp


class OadpResourcesMixin:
    """Node and pod resource collection, diff calculations."""

    @logger_time_stamp
    def get_resources_per_pod(self, podname: str, namespace: str, label: str = "") -> None:
        """Record CPU/memory and container resources for one pod into run metadata."""
        cmd_adm_top_pod_output = self._OadpWorkloads__ssh.run(cmd=f"oc adm top pod {podname} -n {namespace} --no-headers=true")
        if len(cmd_adm_top_pod_output.split(" ")) == 0:
            logger.warning(f"resulting query for {podname} resources failed 0 lines returned")
        else:
            response = list(filter(None, cmd_adm_top_pod_output.split(" ")))
            get_pod_json_output = self._OadpWorkloads__ssh.run(cmd=f"oc get pod {response[0]} -n {namespace} -o json")
            data = json.loads(get_pod_json_output)
            pod_resources = data["spec"]["containers"][0]["resources"]
            pod_details = [
                {"name": response[0], "cores": response[1], "mem": response[2], "resources": pod_resources, "label": label}
            ]
            self._OadpWorkloads__run_metadata["summary"]["resources"]["run_time_pods"].append(pod_details)

    @logger_time_stamp
    def calc_resource_diff(self, old_value: str, new_value: str) -> int | float:
        """Compute percent change from old_value to new_value using digits only."""
        old_value = int(re.sub("[^0-9]", "", old_value))
        new_value = int(re.sub("[^0-9]", "", new_value))
        if old_value == 0:
            return 0
        return (new_value - old_value) / old_value * 100

    @logger_time_stamp
    def calc_pod_basename(self, pod_name: str) -> str:
        """Derive a stable base name from a Kubernetes pod name by stripping ordinal suffixes."""
        pod_parts = pod_name.split("-")
        base_name = ""
        for part in pod_parts:
            if any(map(str.isdigit, part)):
                return base_name[:-1]
            if pod_parts[-1] != part:
                base_name += part + "-"
            else:
                return base_name[:-1]
        return base_name[:-2]

    @logger_time_stamp
    def initialize_pod_resources_by_base_name(self, pod_name: str) -> None:
        """Register pod_name under a base key in oadp_resources and runtime mapping."""
        base_pod_name = self.calc_pod_basename(pod_name)
        if base_pod_name not in self._OadpWorkloads__oadp_resources:
            self._OadpWorkloads__oadp_resources[base_pod_name] = {}
            self._OadpWorkloads__oadp_runtime_resource_mapping[pod_name] = f"{base_pod_name}"
        else:
            count = 1
            for key, _value in self._OadpWorkloads__oadp_resources.items():
                short_name = key.rsplit("-", 1)[0]
                if base_pod_name.lower() == short_name.lower():
                    count += 1
            self._OadpWorkloads__oadp_resources[f"{base_pod_name}-{count - 1}"] = {}
            self._OadpWorkloads__oadp_runtime_resource_mapping[pod_name] = f"{base_pod_name}-{count - 1}"

    @logger_time_stamp
    def find_metadata_index_for_pods(self, target: str) -> int | None:
        """Return index in run_time_pods for the pod named target, or None."""
        run_time_pods = self._OadpWorkloads__run_metadata["summary"]["resources"]["run_time_pods"]
        for index in range(len(run_time_pods)):
            if run_time_pods[index][0]["name"] == target:
                return index
        return None

    @logger_time_stamp
    def get_node_generic_name(self, host: str) -> str:
        """Normalize worker node hostnames to a shorter generic name when applicable."""
        if "worker" in host and host.count("-") == 1:
            updated_host = host.split("-")[0]
            logger.info(f"get_node_generic_name converting {host} to {updated_host}")
            return updated_host
        logger.info(f"get_node_generic_name no need to convert host: {host} as its not a worker")
        return host

    @logger_time_stamp
    def get_node_resource_avail_adm(self, ocp_node: str) -> None:
        """Snapshot or update node CPU/memory from oc adm top into run metadata."""
        cmd_output = self._OadpWorkloads__ssh.run(cmd=f"oc adm top node {ocp_node} --no-headers")
        if cmd_output != "":
            node_adm_result = list(filter(None, cmd_output.split(" ")))
            worker_name = self.get_node_generic_name(node_adm_result[0])
            nodes = self._OadpWorkloads__run_metadata["summary"]["resources"]["nodes"]
            if worker_name not in nodes:
                nodes[worker_name] = {
                    "name": node_adm_result[0],
                    "cores": node_adm_result[1],
                    "cpu_per": node_adm_result[2],
                    "mem_bytes": node_adm_result[3],
                    "mem_per": node_adm_result[4],
                    "label": "",
                }
            else:
                self.calc_node_mem_and_cpu_resources_diff(node_adm_result)

    @logger_time_stamp
    def calc_node_mem_and_cpu_resources_diff(self, adm_node_info: list[str]) -> None:
        """Store CPU and memory percentage deltas versus the previous node snapshot."""
        node = self.get_node_generic_name(adm_node_info[0])
        nodes = self._OadpWorkloads__run_metadata["summary"]["resources"]["nodes"]

        prev_core_total = nodes[node]["cpu_per"].replace("%", "")
        current_core = adm_node_info[2].replace("%", "")
        diff_core = int(current_core) - int(prev_core_total)
        nodes[node]["core_diff"] = f"{diff_core}%"

        prev_mem_total = nodes[node]["mem_per"].replace("%", "")
        current_mem = adm_node_info[4].replace("%", "")
        diff_mem = int(current_mem) - int(prev_mem_total)
        nodes[node]["mem_diff"] = f"{diff_mem}%"

    @logger_time_stamp
    def collect_all_node_resource(self) -> None:
        """Collect oc adm top data for all Ready nodes and merge into run metadata."""
        try:
            get_all_ready_state_nodes = (
                """oc get nodes -o=json | jq -r '.items[] | """
                """select(.status.conditions[] | select(.type=="Ready" and .status=="True")) """
                """| .metadata.name'"""
            )
            get_node_names = self._OadpWorkloads__ssh.run(cmd=get_all_ready_state_nodes)
            if len(get_node_names.splitlines()) > 0 and "error" not in get_node_names:
                for bm in get_node_names.splitlines():
                    self.get_node_resource_avail_adm(ocp_node=bm)
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def get_resources_per_ns(self, namespace: str, label: str = "") -> None:
        """Enumerate pod resources in a namespace and update run_time_pods or final diffs."""
        try:
            cmd_adm_top_ns_output = self._OadpWorkloads__ssh.run(cmd=f"oc adm top pods -n {namespace} --no-headers=true")
            if len(cmd_adm_top_ns_output.splitlines()) == 0:
                logger.warning(f"resulting query for get_resources_per_ns {namespace} resources failed 0 lines returned")
                return

            cmd_adm_top_ns_output_list = list(filter(None, cmd_adm_top_ns_output.split("\n")))
            if cmd_adm_top_ns_output_list is None:
                return

            for val in cmd_adm_top_ns_output_list:
                adm_stdout_response = list(filter(None, val.split(" ")))
                if label == "end":
                    pod_index = self.find_metadata_index_for_pods(target=adm_stdout_response[0])
                    if pod_index is not None:
                        run_time_pods = self._OadpWorkloads__run_metadata["summary"]["resources"]["run_time_pods"]
                        original_sample = run_time_pods[pod_index][0]["mem"]
                        diff_mem = self.calc_resource_diff(original_sample, adm_stdout_response[2])
                        original_sample = run_time_pods[pod_index][0]["cores"]
                        diff_core = self.calc_resource_diff(original_sample, adm_stdout_response[1])
                        pod_name_by_role = self._OadpWorkloads__oadp_runtime_resource_mapping[adm_stdout_response[0]]
                        pod_details = {
                            f"{label}_cores": adm_stdout_response[1],
                            f"{label}_mem": adm_stdout_response[2],
                            "diff_core_percent": f"{diff_core:.1f}",
                            "diff_mem_percent": f"{diff_mem:.1f}",
                            "label": label,
                        }
                        original_dict = run_time_pods[pod_index][0]
                        run_time_pods[pod_index][0] = {**original_dict, **pod_details}
                        pods = self._OadpWorkloads__run_metadata["summary"]["resources"]["pods"]
                        pods[pod_name_by_role] = run_time_pods[pod_index][0]
                        del run_time_pods[pod_index]
                else:
                    self.initialize_pod_resources_by_base_name(pod_name=adm_stdout_response[0])
                    get_pod_json_output = self._OadpWorkloads__ssh.run(
                        cmd=f"oc get pod {adm_stdout_response[0]} -n {namespace} -o json"
                    )
                    data = json.loads(get_pod_json_output)
                    pod_resources = data["spec"]["containers"][0]["resources"]
                    pod_details = [
                        {
                            "name": adm_stdout_response[0],
                            "cores": adm_stdout_response[1],
                            "mem": adm_stdout_response[2],
                            "resources": pod_resources,
                            "label": label,
                        }
                    ]
                    self._OadpWorkloads__run_metadata["summary"]["resources"]["run_time_pods"].append(pod_details)

            if label == "end":
                self.get_percentage_of_limit_utilized(ns=namespace)
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def get_percentage_of_limit_utilized(self, ns: str) -> None:
        """Augment per-pod metadata with limit utilization from oc describe node."""
        get_node_names = self._OadpWorkloads__ssh.run(cmd=f'oc describe node | grep -w "{ns}"')
        for line in get_node_names.splitlines():
            matches_expected_line_ouput = re.findall(f"{ns}    ", line)
            if len(matches_expected_line_ouput) > 0:
                x = list(filter(None, line.split(" ")))
                logger.info(f"get_percentage_of_limit_utilized working with x: {x} whose length is {len(x)}")
                pod_name_by_role = self._OadpWorkloads__oadp_runtime_resource_mapping[f"{x[1]}"]
                pods = self._OadpWorkloads__run_metadata["summary"]["resources"]["pods"]
                cpu_limit_value = re.findall(r"\d+", x[5])
                mem_limit_value = re.findall(r"\d+", x[9])
                updated_pod_details = {
                    "ns": x[0],
                    "cpu_limit_percentage": cpu_limit_value,
                    "mem_limit_percentage": mem_limit_value,
                    "pod_uptime": x[10],
                }
                pods[pod_name_by_role] = {**pods[pod_name_by_role], **updated_pod_details}

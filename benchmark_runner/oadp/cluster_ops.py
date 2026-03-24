"""
Mixin providing thin wrappers around OpenShift CLI (oc) operations.

Includes login, resource queries, patching, pod listing, and
generic retry helpers used across the OADP workload.
"""

from __future__ import annotations

import inspect
import json
import time

from benchmark_runner.common.logger.logger_time_stamp import logger, logger_time_stamp


class OadpClusterOpsMixin:
    """Methods for low-level OpenShift cluster interaction."""

    def get_current_function(self) -> str:
        """Return a label string naming the immediate caller function."""
        caller_frame = inspect.currentframe().f_back
        function_name = inspect.getframeinfo(caller_frame).function
        return f" function executing: {function_name}"

    @logger_time_stamp
    def oc_log_in(self) -> bool:
        """Log in to OpenShift with kubeadmin if not already authenticated."""
        import os

        kubeconfig_path = f"/home/{os.getenv('USER')}/.kube/config"
        kubeadmin_password_path = f"/home/{os.getenv('USER')}/clusterconfigs/auth/kubeadmin-password"
        if not (os.path.exists(kubeconfig_path) and os.path.exists(kubeadmin_password_path)):
            logger.error(
                f"## ERROR ## Attempting to log in to Openshift but {kubeconfig_path} or "
                f"{kubeadmin_password_path} is not present"
            )
            raise FileNotFoundError("Kubeconfig file or kubeadmin password file not found.")

        oc_status = self._OadpWorkloads__ssh.run(cmd="oc whoami")

        if "kube:admin" in oc_status:
            logger.info("### INFO ### You are already logged in with authenticated session")
            return True

        logger.info("### INFO ### You are not currently logged into Openshift will now attempt log in")
        login_attempt = self._OadpWorkloads__ssh.run(
            cmd=f"export KUBECONFIG={kubeconfig_path} && oc login -u kubeadmin -p $(cat {kubeadmin_password_path}) -n default"
        )
        if "Login successful" not in login_attempt:
            logger.error("## ERROR ## oc_log_in: Unable to log in to Openshift.")
            return False
        logger.info("### INFO ### You are now logged in with authenticated session")
        return True

    @logger_time_stamp
    def get_oc_resource_to_json(self, resource_type: str, resource_name: str, namespace: str) -> dict:
        """Fetch a single OpenShift resource as JSON, or empty dict if missing."""
        logger.info(f":: INFO :: Attempting  oc get {resource_type} {resource_name} -n {namespace} ")
        resources_returned = self._OadpWorkloads__ssh.run(cmd=f"oc get {resource_type} {resource_name} -n {namespace} -o json")
        if "Error from server" in resources_returned:
            logger.info(f"::info:: oc get {resource_type} {resource_name} -n {namespace} => Not Found")
            return {}
        logger.info(f":: INFO :: oc get {resource_type} {resource_name} -n {namespace} returned {resources_returned} ")
        return json.loads(resources_returned)

    @logger_time_stamp
    def get_list_of_pods(self, namespace: str) -> list[str] | bool:
        """Return Running pod names in namespace, or False if none."""
        running_pods = self._OadpWorkloads__ssh.run(
            cmd=f"oc get pods -n {namespace} --field-selector status.phase=Running "
            f'--no-headers -o custom-columns=":metadata.name"'
        )
        if running_pods != "":
            return running_pods.split("\n")
        return False

    @logger_time_stamp
    def get_list_of_pods_by_status(self, namespace: str, query_operator: str, status: str) -> list[str]:
        """List pod names in namespace matching the given phase field-selector."""
        pods = self._OadpWorkloads__ssh.run(
            cmd=f"oc get pods -n {namespace} --field-selector "
            f"status.phase{query_operator}{status} --no-headers "
            f'-o custom-columns=":metadata.name"'
        )
        if pods != "":
            return pods.split("\n")
        return []

    @logger_time_stamp
    def patch_oc_resource(
        self, resource_type: str, resource_name: str, namespace: str, patch_type: str, patch_json: str
    ) -> None:
        """Patch an OpenShift resource in place via oc patch."""
        if patch_type == "json":
            oc_patch_cmd = f"oc patch {resource_type} {resource_name} -n {namespace} --type={patch_type} -p='{patch_json}'"
        else:
            oc_patch_cmd = f"oc patch {resource_type} {resource_name} -n {namespace} --type {patch_type} -p '{patch_json}'"
        logger.info(f":: INFO :: Attempting OC PATCH => {oc_patch_cmd}")
        oc_patch_response = self._OadpWorkloads__ssh.run(cmd=oc_patch_cmd)
        expected_result_output = ["patched", "unchanged", "configured", "no change"]
        if not any(ext in oc_patch_response for ext in expected_result_output):
            logger.exception(f":: ERROR :: Unable to process patch command: {oc_patch_cmd}  resulted in {oc_patch_response}")
        else:
            logger.info(f":: INFO :: Succesful patch command: {oc_patch_cmd}  resulted in {oc_patch_response}")

    @logger_time_stamp
    def is_cr_present(self, ns: str, cr_type: str, cr_name: str) -> bool:
        """Return True if the named custom resource exists in the namespace."""
        try:
            list_of_crs = self.get_custom_resources(cr_type, ns)
            return cr_name in list_of_crs
        except Exception as err:
            self.fail_test_run(f" {err} occurred in " + self.get_current_function())
            raise err

    @logger_time_stamp
    def get_custom_resources(self, cr_type: str, ns: str) -> list[str]:
        """Return names of all custom resources of the given type in the namespace."""
        cmd_output = self._OadpWorkloads__ssh.run(cmd=f'oc get {cr_type} -n {ns} -o jsonpath="{{.items[*].metadata.name}}"')
        return list(filter(None, cmd_output.split(" ")))

    def execute_with_retries(self, cmd: str, retries: int = 3, delay: int = 2) -> str | None:
        """Run a shell command up to retries times, returning the first non-empty output or None."""
        for attempt in range(1, retries + 1):
            result = self._OadpWorkloads__ssh.run(cmd)
            if result is not None and result != "":
                return result
            logger.warning(f"Attempt {attempt} failed for command: {cmd}")
            if attempt < retries:
                time.sleep(delay)
        logger.error(f"### ERROR ### execute_with_retries: command failed after {retries} attempts: {cmd}")
        return None

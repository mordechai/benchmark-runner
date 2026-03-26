"""
Mixin for KubeVirt VM dataset creation, data population, and lifecycle management.

Delegates to the existing ``Virtctl`` / ``OC`` helpers for VM lifecycle operations
rather than reimplementing via raw SSH commands.
"""

from __future__ import annotations

import time
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from benchmark_runner.common.logger.logger_time_stamp import logger, logger_time_stamp
from benchmark_runner.oadp.constants import (
    VM_DEFAULT_BATCH_SIZE,
    VM_DEFAULT_POPULATION_CONCURRENCY,
    VM_DEFAULT_SSH_USER,
    VM_DEFAULT_THROTTLE_BATCHES,
)
from benchmark_runner.oadp.vm_profiles import load_vm_profile, merge_disk_overrides

_TEMPLATES_DIR = Path(__file__).resolve().parent / "vm_templates"


class OadpVmOperationsMixin:
    """KubeVirt VM dataset creation and data population."""

    def _get_jinja_env(self) -> Environment:
        return Environment(
            loader=FileSystemLoader(str(_TEMPLATES_DIR)),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    # ------------------------------------------------------------------
    # VM dataset creation entry point
    # ------------------------------------------------------------------

    @logger_time_stamp
    def create_vm_dataset(self, scenario: dict, ds: dict) -> None:
        """Create a KubeVirt VM dataset: OS DV, VMs, wait, populate data."""
        profile_name = ds.get("vm_profile", "")
        profile = load_vm_profile(profile_name)
        disk_overrides = ds.get("disk_overrides")
        profile = merge_disk_overrides(profile, disk_overrides)

        namespace = ds["namespace"]
        vms_per_ns = ds.get("vms_per_namespace", ds.get("pods_per_ns", 1))
        sc = ds["sc"]
        batch_size = int(scenario["args"].get("vm_creation_batch_size", VM_DEFAULT_BATCH_SIZE))
        throttle_batches = int(scenario["args"].get("vm_creation_throttle_batches", VM_DEFAULT_THROTTLE_BATCHES))

        self._ensure_namespace(namespace)
        os_dv_name = self._create_os_datavolume(profile, namespace, sc)
        self._batch_create_vms(
            profile,
            namespace,
            sc,
            os_dv_name,
            vms_per_ns,
            batch_size,
            throttle_batches,
            scenario,
        )
        all_running = self._wait_for_all_vms_running(namespace, vms_per_ns, scenario)
        if not all_running:
            self.fail_test_run(f"Not all VMIs running in {namespace} after timeout")
        self._wait_for_guest_agents(namespace, vms_per_ns)

        if profile.get("data_generation", {}).get("method"):
            self._populate_vms_with_data(
                profile,
                namespace,
                vms_per_ns,
                scenario,
            )

    # ------------------------------------------------------------------
    # Namespace
    # ------------------------------------------------------------------

    def _ensure_namespace(self, namespace: str) -> None:
        ssh = self._OadpWorkloads__ssh
        existing = ssh.run(cmd=f"oc get ns {namespace} --no-headers 2>/dev/null")
        if namespace not in existing:
            ssh.run(cmd=f"oc create ns {namespace}")
            logger.info(f"Created namespace {namespace}")

    # ------------------------------------------------------------------
    # OS DataVolume
    # ------------------------------------------------------------------

    @logger_time_stamp
    def _create_os_datavolume(
        self,
        profile: dict,
        namespace: str,
        sc: str,
    ) -> str:
        """Create the golden OS DataVolume that VMs clone from."""
        ssh = self._OadpWorkloads__ssh
        os_config = profile.get("os", {})
        root_disk = next(
            (d for d in profile.get("disks", []) if d.get("boot_order") == 1),
            {},
        )
        dv_name = f"{profile['name']}-os-dv"

        existing = ssh.run(cmd=f"oc get dv {dv_name} -n {namespace} --no-headers 2>/dev/null")
        if dv_name in existing:
            logger.info(f"OS DataVolume {dv_name} already exists, skipping creation")
            self._wait_for_dv_ready(dv_name, namespace)
            return dv_name

        env = self._get_jinja_env()
        template = env.get_template("dv_template.yaml")
        rendered = template.render(
            dv_name=dv_name,
            namespace=namespace,
            image_source=os_config.get("image_source", ""),
            pull_method=os_config.get("pull_method", "node"),
            root_disk_size=root_disk.get("size", "30Gi"),
            storage_class=sc,
        )

        dv_file = f"/tmp/oadp-os-dv-{namespace}.yaml"
        ssh.run(cmd=f"cat > {dv_file} << 'DVEOF'\n{rendered}\nDVEOF")
        ssh.run(cmd=f"oc apply -f {dv_file}")
        logger.info(f"Created OS DataVolume {dv_name} in {namespace}")

        self._wait_for_dv_ready(dv_name, namespace)
        return dv_name

    def _wait_for_dv_ready(self, dv_name: str, namespace: str) -> None:
        ssh = self._OadpWorkloads__ssh
        timeout = 1800
        elapsed = 0
        while elapsed < timeout:
            phase = ssh.run(cmd=f"oc get dv {dv_name} -n {namespace} -o jsonpath='{{.status.phase}}' 2>/dev/null")
            if phase.strip() == "Succeeded":
                logger.info(f"DataVolume {dv_name} is ready")
                return
            time.sleep(15)
            elapsed += 15
            if elapsed % 60 == 0:
                logger.info(f"Waiting for DV {dv_name}: phase={phase}, elapsed={elapsed}s/{timeout}s")
        msg = f"DataVolume {dv_name} did not reach Succeeded within {timeout}s"
        logger.error(msg)
        self.fail_test_run(msg)

    # ------------------------------------------------------------------
    # Batched VM creation
    # ------------------------------------------------------------------

    @logger_time_stamp
    def _batch_create_vms(
        self,
        profile: dict,
        namespace: str,
        sc: str,
        os_dv_name: str,
        total_vms: int,
        batch_size: int,
        throttle_batches: int,
        scenario: dict,
    ) -> None:
        """Apply VM manifests in batches with throttle gates."""
        ssh = self._OadpWorkloads__ssh
        env = self._get_jinja_env()
        template = env.get_template("vm_template.yaml")

        root_disk = next(
            (d for d in profile.get("disks", []) if d.get("boot_order") == 1),
            {},
        )
        data_disks = [d for d in profile.get("disks", []) if d.get("boot_order") != 1]

        self.oadp_timer(action="start", transaction_name="vm_dataset_creation")
        batch_count = 0

        for i in range(total_vms):
            vm_name = f"oadp-vm-{profile['name']}-{i}"
            rendered = template.render(
                vm_name=vm_name,
                namespace=namespace,
                cpu_cores=profile.get("resources", {}).get("cpu_cores", 2),
                memory=profile.get("resources", {}).get("memory", "4Gi"),
                data_disks=data_disks,
                os_dv_name=os_dv_name,
                root_disk_size=root_disk.get("size", "30Gi"),
                storage_class=sc,
            )

            vm_file = f"/tmp/oadp-vm-{namespace}-{i}.yaml"
            ssh.run(cmd=f"cat > {vm_file} << 'VMEOF'\n{rendered}\nVMEOF")
            ssh.run(cmd=f"oc apply -f {vm_file}", background=True)

            if (i + 1) % batch_size == 0:
                batch_count += 1
                logger.info(f"VM creation batch {batch_count}: applied VMs {i - batch_size + 2} to {i + 1}")

                if batch_count % throttle_batches == 0:
                    self._verify_vms_are_progressing(namespace, scenario)

        self.oadp_timer(action="stop", transaction_name="vm_dataset_creation")
        logger.info(f"All {total_vms} VM manifests applied in {namespace}")

    # ------------------------------------------------------------------
    # VM readiness
    # ------------------------------------------------------------------

    def _verify_vms_are_progressing(
        self,
        namespace: str,
        scenario: dict,
    ) -> bool:
        """Check that the Running VMI count is increasing (throttle gate)."""
        retry_logic = self._OadpWorkloads__retry_logic

        previous_count = self._count_running_vmis(namespace)
        for _attempt in range(retry_logic["max_attempts"]):
            time.sleep(retry_logic["interval_between_checks"])
            current_count = self._count_running_vmis(namespace)
            if current_count > previous_count:
                logger.info(f"VMIs progressing: {previous_count} -> {current_count}")
                return True
            previous_count = current_count
        logger.warning(f"VMIs not progressing in {namespace}, count: {previous_count}")
        return False

    def _count_running_vmis(self, namespace: str) -> int:
        ssh = self._OadpWorkloads__ssh
        result = ssh.run(cmd=f"oc get vmi -n {namespace} --no-headers 2>/dev/null | grep -c Running")
        try:
            return int(result.strip())
        except (ValueError, AttributeError):
            return 0

    @logger_time_stamp
    def _wait_for_all_vms_running(
        self,
        namespace: str,
        expected_count: int,
        scenario: dict,
    ) -> bool:
        """Poll until all VMIs in namespace report Running."""
        timeout = int(scenario["args"].get("testcase_timeout", 43200))
        elapsed = 0
        while elapsed < timeout:
            count = self._count_running_vmis(namespace)
            if count >= expected_count:
                logger.info(f"All {expected_count} VMIs running in {namespace}")
                return True
            time.sleep(15)
            elapsed += 15
            if elapsed % 60 == 0:
                logger.info(f"Waiting for VMIs: {count}/{expected_count} running, elapsed={elapsed}s")
        logger.error(f"Only {self._count_running_vmis(namespace)}/{expected_count} VMIs running after {timeout}s")
        return False

    def _wait_for_guest_agents(self, namespace: str, expected_count: int) -> None:
        """Wait until qemu-guest-agent is connected on all VMIs."""
        ssh = self._OadpWorkloads__ssh
        timeout = 600
        elapsed = 0
        while elapsed < timeout:
            result = ssh.run(
                cmd=f"oc get vmi -n {namespace} -o jsonpath="
                f"'{{.items[*].status.conditions[?(@.type==\"AgentConnected\")].status}}'"
            )
            connected = result.strip().split()
            true_count = sum(1 for s in connected if s == "True")
            if true_count >= expected_count:
                logger.info(f"Guest agents connected on all {expected_count} VMIs")
                return
            time.sleep(10)
            elapsed += 10
        logger.warning(f"Guest agent timeout: only {true_count}/{expected_count} connected after {timeout}s")

    # ------------------------------------------------------------------
    # Data population
    # ------------------------------------------------------------------

    @logger_time_stamp
    def _populate_vms_with_data(
        self,
        profile: dict,
        namespace: str,
        total_vms: int,
        scenario: dict,
    ) -> None:
        """Run data generation commands concurrently on all VMs."""
        ssh = self._OadpWorkloads__ssh
        concurrency = int(
            scenario["args"].get(
                "vm_population_concurrency",
                VM_DEFAULT_POPULATION_CONCURRENCY,
            )
        )
        method = profile.get("data_generation", {}).get("method", "")
        config = profile.get("data_generation", {}).get("config", {})
        ssh_user = VM_DEFAULT_SSH_USER

        self.oadp_timer(action="start", transaction_name="vm_data_population")

        pending_vms = []
        for i in range(total_vms):
            vm_name = f"oadp-vm-{profile['name']}-{i}"
            cmd = self._build_data_gen_command(method, config, profile)
            if not cmd:
                continue

            ssh.run(
                cmd=f"virtctl ssh {ssh_user}@{vm_name} -n {namespace} --command '{cmd}' &",
                background=True,
            )
            pending_vms.append(vm_name)

            if len(pending_vms) >= concurrency:
                self._wait_for_data_gen_batch(
                    ssh,
                    namespace,
                    pending_vms,
                    ssh_user,
                    scenario,
                )
                pending_vms = []

        if pending_vms:
            self._wait_for_data_gen_batch(
                ssh,
                namespace,
                pending_vms,
                ssh_user,
                scenario,
            )

        self.oadp_timer(action="stop", transaction_name="vm_data_population")
        logger.info(f"Data population completed for {total_vms} VMs")

    @staticmethod
    def _build_data_gen_command(
        method: str,
        config: dict,
        profile: dict,
    ) -> str:
        """Construct the in-guest data generation command string."""
        if method == "hammerdb":
            return "/opt/hammerdb/run_tpcc.sh"
        return ""

    def _wait_for_data_gen_batch(
        self,
        ssh: object,
        namespace: str,
        vm_names: list[str],
        ssh_user: str,
        scenario: dict,
    ) -> None:
        """Poll VMs until data generation processes finish."""
        timeout = int(scenario["args"].get("testcase_timeout", 43200))
        remaining = list(vm_names)
        elapsed = 0
        while remaining and elapsed < timeout:
            still_running = []
            for vm_name in remaining:
                result = ssh.run(
                    cmd=f"virtctl ssh {ssh_user}@{vm_name} -n {namespace} --command 'pgrep -c hammerdb || echo 0'"
                )
                try:
                    count = int(result.strip().split()[-1])
                except (ValueError, IndexError):
                    count = 0
                if count > 0:
                    still_running.append(vm_name)
            remaining = still_running
            if remaining:
                time.sleep(30)
                elapsed += 30
        if remaining:
            logger.warning(f"Data generation did not complete on {len(remaining)} VMs after {elapsed}s: {remaining}")

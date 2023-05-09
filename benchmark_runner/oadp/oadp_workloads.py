import logging
import os
import json
import time
import random
import string


import yaml
import re
from pathlib import Path

from benchmark_runner.common.ssh.ssh import SSH
from benchmark_runner.oadp.oadp_exceptions import MissingResultReport, MissingElasticSearch
from benchmark_runner.common.logger.logger_time_stamp import logger_time_stamp, logger
from benchmark_runner.workloads.workloads_operations import WorkloadsOperations
from benchmark_runner.common.prometheus.prometheus_metrics import prometheus_metrics
from datetime import datetime, timedelta
from dateutil import parser


class OadpWorkloads(WorkloadsOperations):
    """
    This class is responsible for all Oadp workloads
    """

    def __init__(self):
        super().__init__()
        self.__oadp_path = '/tmp/mpqe-scale-scripts/mtc-helpers/busybox'
        self.__oadp_base_dir = '/tmp/mpqe-scale-scripts/oadp-helpers'
        self.__oadp_scenario_data = '/tmp/mpqe-scale-scripts/oadp-helpers/templates/internal_data/tests.yaml'
        self.__oadp_promql_queries = '/tmp/mpqe-scale-scripts/oadp-helpers/templates/metrics/metrics-oadp.yaml'
        # environment variables
        self.__namespace = self._environment_variables_dict.get('namespace', '')
        self.__oadp_workload = self._environment_variables_dict.get('oadp', '')
        self.__oadp_uuid = self._environment_variables_dict.get('oadp_uuid', '')
        #  To set test scenario variable for 'backup-csi-busybox-perf-single-100-pods-rbd' for  self.__oadp_scenario_name you'll need to  manually set the default value as shown below
        #  for example:   self.__oadp_scenario_name = self._environment_variables_dict.get('oadp_scenario', 'backup-csi-busybox-perf-single-100-pods-rbd')
        self.__oadp_scenario_name = 'backup-100pod-backup-vsm-pvc-util-4-1-0-cephrbd-6g' #self._environment_variables_dict.get('oadp_scenario','')
        # self.__oadp_scenario_name  = 'backup-csi-pvc-util-2-1-5-rbd-swift-1.5t' # backup-100pod-backup-vsm-pvc-util-4-1-0-cephrbd-6g' #self._environment_variables_dict.get('oadp_scenario', '')
        self.__oadp_cleanup_cr_post_run = self._environment_variables_dict.get('oadp_cleanup_cr', False)
        self.__oadp_cleanup_dataset_post_run = self._environment_variables_dict.get('oadp_cleanup_dataset', False)
        self.__result_report = '/tmp/oadp-report.json'
        self.__artifactdir = os.path.join(self._run_artifacts_path, 'oadp-ci')
        self._run_artifacts_path = self._environment_variables_dict.get('run_artifacts_path', '')
        self.__oadp_log = os.path.join(self._run_artifacts_path, 'oadp.log')
        self.__ssh = SSH()
        self.__oadp_resources = {}
        self.__oadp_runtime_resource_mapping = {}
        self.__oadp_dpa = 'example-velero'
        self.__result_dicts = []
        self.__run_metadata = {
            "index": '',
            "metadata": {},
            "status": '',
            "summary": {
                "env": {
                    "ocp": {},
                    "storage": {}
                },
                "runtime": {},
                "results": {},
                "resources": {
                    "nodes": {},
                    "run_time_pods": [],
                    "pods": {}
                },
                "transactions": []
            }
        }

    @logger_time_stamp
    def create_json_summary(self):
        """
        method json dumps __run_metadata to file
        """
        with open('/tmp/oadp-report.json', 'w', encoding='utf-8') as f:
            json.dump(self.__run_metadata, f, indent=4, sort_keys=True, default=str)


    @logger_time_stamp
    def upload_oadp_result_to_elasticsearch(self):
        """
        This method upload to ElasticSearch the results
        :return:
        """
        datetime_format = '%Y-%m-%d %H:%M:%S'
        result_report_json_file = open(self.__result_report)
        result_report_json_str = result_report_json_file.read()
        result_report_json_data = json.loads(result_report_json_str)
        index = self.__run_metadata['index']
        logger.info(f'upload index: {index}')
        metadata_details = {'uuid': self._environment_variables_dict['uuid'], 'upload_date': datetime.now().strftime(datetime_format), 'run_artifacts_url': os.path.join(self._run_artifacts_url,f'{self._get_run_artifacts_hierarchy(workload_name=self._workload, is_file=True)}-{self._time_stamp_format}.tar.gz'), 'scenario': self.__run_metadata['summary']['runtime']['name']}
        # run artifacts data
        result_report_json_data['metadata'] = {}
        result_report_json_data['metadata'].update(metadata_details)

        self._es_operations.upload_to_elasticsearch(index=index, data=result_report_json_data)

    @logger_time_stamp
    def get_logs_by_pod_ns(self, namespace):
        """
        This method gets list of pods in ns and saves pod log in self.__run_artifacts podname
        will be invoked with get_oadp_velero_and_cr_log(self, cr_name, cr_type)
        """
        list_of_pods_found = self.get_list_of_pods(namespace=namespace)
        if type(list_of_pods_found) == list:
            for p in list_of_pods_found:
                output_filename = os.path.join(self._run_artifacts_path,f'{p}.log')
                logger.info(
                    f'performing oc logs {p} -n {namespace} redirected to {output_filename} ')
                self.__ssh.run(cmd=f'oc logs {p} -n {namespace} > {output_filename}')


    @logger_time_stamp
    def delete_all(self):
        """
        This method delete all resource that related to oadp resource
        :return:
        """
        self.__ssh.run(cmd=f'oc delete pod -A -l {self.__namespace}')

    def initialize_workload(self):
        """
        This method includes all the initialization of oadp workload
        :return:
        """
        # self.delete_all()
        # self.clear_nodes_cache()
        if self._enable_prometheus_snapshot:
            self.start_prometheus()

    def finalize_workload(self):
        """
        This method includes all the finalization of oadp workload
        :return:
        """
        # Result file exist and not empty
        if os.path.exists(os.path.join(self.__result_report)) and not os.stat(self.__result_report).st_size == 0:
            self.upload_oadp_result_to_elasticsearch()

        if self._enable_prometheus_snapshot:
            self.end_prometheus()
        if self._endpoint_url:
            self.upload_run_artifacts_to_s3()
        if not self._save_artifacts_local:
            self.delete_local_artifacts()
        self.delete_all()

    @logger_time_stamp
    def get_storage_details(self):
        """
        method gets ceph version
        """
        storage_details = {}
        get_sc_cmd = self.__ssh.run(cmd="oc get sc --no-headers | awk '{print $1}'")
        if len(get_sc_cmd.splitlines()) > 0:
            storage_details['storage_class'] = get_sc_cmd.splitlines()
        ceph_pod = self.__ssh.run(cmd=f'oc get pods -n openshift-storage --field-selector status.phase=Running --no-headers -o custom-columns=":metadata.name" | grep tools')
        if ceph_pod != '':
            ceph_version_cmd = self.__ssh.run(cmd=f'oc -n openshift-storage rsh {ceph_pod} ceph version')
            if ceph_version_cmd.split('ceph version ')[1] != '':
                storage_details['ceph'] = ceph_version_cmd.split('ceph version ')[1]
                # self.__run_metadata['summary'].update(storage_details)
        # storage info
        default_storage_class = self.get_default_storage_class()
        storage_details['default_storage_class'] = default_storage_class
        openshift_storage_version_cmd = self.__ssh.run(
            cmd=f"oc -n openshift-storage get csv -o yaml | grep full_version | tail -n 1")
        openshift_storage_version = openshift_storage_version_cmd.split('full_version: ')[1]
        storage_details['openshift_storage_version'] = openshift_storage_version
        self.__result_dicts.append(storage_details)
        self.__run_metadata['summary']['env']['storage'].update(storage_details)
        self.__result_dicts.append(self.__run_metadata['summary']['env']['storage'])
        self.get_noobaa_version_details()

    @logger_time_stamp
    def get_velero_details(self):
        """
        method gets oadp relevant pod details
        """
        velero_pod_name = self.__ssh.run(
            cmd='oc get pods -n openshift-adp --field-selector status.phase=Running --no-headers -o custom-columns=":metadata.name" | grep velero')
        if velero_pod_name != '':
            get_velero_pod_details = self.__ssh.run(cmd=f"oc get pods {velero_pod_name} -n openshift-adp -o json")
            data = json.loads(get_velero_pod_details)
            velero_details = {
                "velero": {}
            }
            velero_pod_on_worker = data['spec']['nodeName']
            for container in data['spec']['containers']:
                velero_details['velero']['name'] = container['name']
                velero_details['velero']['args'] = container['args']
                velero_details['velero']['image'] = container['image']
                velero_details['velero']['resources'] = container['resources']
            get_velero_version = self.__ssh.run(
                cmd=f"oc -n openshift-adp exec deployment/velero -c velero -it -- ./velero version | grep Version: | tail -n -1")
            velero_version = get_velero_version.split('Version:')[1]
            if velero_version != '':
                velero_details['velero']['velero_version'] = velero_version
            self.__run_metadata['summary']['env'].update(velero_details)
            self.__result_dicts.append(self.__run_metadata['summary']['env'])

    @logger_time_stamp
    def get_noobaa_version_details(self):
        """
        method get noobaa details
        """
        get_noobaa_details = self.__ssh.run(cmd="oc get noobaa -n openshift-storage -o json")

        # Parse the JSON string into a Python object
        data = json.loads(get_noobaa_details)
        noobaa_details = {
            "noobaa": {}
        }
        # Extract the values for the keys "coreResources" and "dbImage"
        dbimage = data['items'][0]['spec']['dbImage']
        noobaa_details['noobaa']['db_image'] = dbimage
        noobaa_details['noobaa']['db_storage_class'] = data['items'][0]['spec']['dbStorageClass']
        noobaa_details['noobaa']['db_volume_resource'] = data['items'][0]['spec']['dbVolumeResources']
        noobaa_details['noobaa']['endpoints'] = data['items'][0]['spec']['endpoints']
        noobaa_details['noobaa']['core_resources'] = data['items'][0]['spec']['coreResources']
        for line in data['items'][0]['status']['readme'].split("\n"):
            if 'NooBaa Core Version:' in line:
                noobaa_details['noobaa']['noobaa_core_version'] = line.split('NooBaa Core Version:')[1].strip()
            if 'NooBaa Operator Version:' in line:
                noobaa_details['noobaa']['noobaa_operator_version'] = line.split('NooBaa Operator Version:')[1].strip()

        self.__run_metadata['summary']['env']['storage'].update(noobaa_details)
        self.__result_dicts.append(noobaa_details)

    @logger_time_stamp
    def verify_pod_pv_size_and_sc(self, podName, expected_ns, expected_sc, expected_size ):
        '''
        method used to verify pod volume is the size, and sc intended is correct
                "volumes": [
            {
                "name": "busybox-perf-single-ns-10-pods-1",
                "persistentVolumeClaim": {
                    "claimName": "pvc-busybox-perf-single-ns-10-pods-1"
                }

        '''
        # get pod's
        query = '"{.spec.volumes[0].persistentVolumeClaim.claimName}"'
        pvc_name = self.__ssh.run(cmd=f'oc get pod {podName} -n {expected_ns} -o jsonpath={query}')
        if (pvc_name.find('Error') > 0 and  pvc_name != ''):
            return False
        else:
            # get pvc sc and size and compare to what was expected
            query = "'{.spec.storageClassName} {.spec.resources.requests.storage}'"
            cmd_get_pvc_sc_and_size = self.__ssh.run(cmd=f'oc get pvc {pvc_name} -n {expected_ns} -o jsonpath={query}')
            current_sc = cmd_get_pvc_sc_and_size.split(' ')[0]
            current_size = cmd_get_pvc_sc_and_size.split(' ')[1]
            if current_sc != expected_sc:
                logger.warning(f"current pv storage class used: {current_sc} doesnt match expected storage class of: {expected_sc}")
                return False
            if current_size != expected_size:
                logger.warning(f"current pv size: {current_size} doesnt match expected pv size: {expected_size}")
                return False
            logger.info(f"pod: {podName} in ns: {expected_ns} matches desired storage and pv size")
            return True

    @logger_time_stamp
    def verify_pod_restarts(self, target_namespace):
        """
        This method verifies number of pods restarts in namespace are not greater than 0
        sets ['summary']['results']['pod_restarts_post_run_validation']['status'] to true/false and lists pods with restart values
        :return:
        """
        restart_query = "'.items[] | select(.status.containerStatuses[].restartCount > 0) | .metadata.name'"
        pods_restarted_cmd = self.__ssh.run(
            cmd=f'oc get pods -n {target_namespace} -o json | jq -r {restart_query}')
        if pods_restarted_cmd != '':
            self.__run_metadata['summary']['results']['pod_restarts_post_run_validation'] = {}
            self.__run_metadata['summary']['results']['pod_restarts_post_run_validation']['status'] = False
            get_pod_details = self.__ssh.run(cmd=f'oc get pods -n {target_namespace} -o json')
            data = json.loads(get_pod_details)
            pods_that_restarted = {}
            for pod in data['items']:
                if pod['status']['containerStatuses'][0]['restartCount'] > 0:
                    name = pod['metadata']['name']
                    pods_that_restarted[f'{name}'] = pod['status']['containerStatuses'][0]['restartCount']
            self.__run_metadata['summary']['results']['pod_restarts_post_run_validation']['restarted_pods'] = {}
            self.__run_metadata['summary']['results']['pod_restarts_post_run_validation']['restarted_pods'].update(pods_that_restarted)
        else:
            self.__run_metadata['summary']['results']['pod_restarts_post_run_validation'] = {}
            self.__run_metadata['summary']['results']['pod_restarts_post_run_validation']['status'] = True
            # Saving empty dict for query consistency in ELK querying
            self.__run_metadata['summary']['results']['pod_restarts_post_run_validation']['restarted_pods'] = {}

    @logger_time_stamp
    def verify_cluster_operators_status(self):
        """
        This method verifies status of cluster operators
        it checks avaiblity and status of degraded

        sets ['summary']['results']['co_status_post_run_validation']['status'] to true/false
        lists co that arent available/degraded only when relevant.
        :return:
        """
        get_co_status = self.__ssh.run(cmd=f'oc get co -o json')
        if get_co_status != '':
            self.__run_metadata['summary']['results']['cluster_operator_post_run_validation'] = {}
            data = json.loads(get_co_status)
            co_degraded = {}
            co_that_are_not_available = {}
            for co in data['items']:
                for s in co['status']['conditions']:
                    if (s['type'] == 'Available' and s['status'] == 'False'):
                        co_name = co['metadata']['name']
                        co_that_are_not_available[f'{co_name}'] = s['status']
                    if (s['type'] == 'Degraded' and s['status'] == 'True'):
                        co_name = co['metadata']['name']
                        co_degraded[f'{co_name}'] = s['status']
            self.__run_metadata['summary']['results']['cluster_operator_post_run_validation']['unavailable'] = {}
            if bool(co_that_are_not_available):
                self.__run_metadata['summary']['results']['cluster_operator_post_run_validation']['unavailable'].update(co_that_are_not_available)
            self.__run_metadata['summary']['results']['cluster_operator_post_run_validation']['degraded'] = {}
            if bool(co_degraded):
                self.__run_metadata['summary']['results']['cluster_operator_post_run_validation']['degraded'].update(co_degraded)
            if (bool(co_that_are_not_available) == False) and (bool(co_degraded) == False):
                self.__run_metadata['summary']['results']['cluster_operator_post_run_validation']['status'] = True
            else:
                self.__run_metadata['summary']['results']['cluster_operator_post_run_validation']['status'] = False

    @logger_time_stamp
    def verify_running_pods(self, num_of_pods_expected, target_namespace):
        """
        This method verifies number of pods in namespace are in running state
        :return:
        """
        running_pods = self.get_list_of_pods_by_status(namespace=target_namespace, query_operator='=',  status='Running')
        logger.info(f':: INFO :: verify_running_pods: {target_namespace} has  {len(running_pods)} in running state, out of total desired: {num_of_pods_expected}')
        if len(running_pods) == num_of_pods_expected:
            return True
        else:
            pods_not_yet_in_run_status = self.get_list_of_pods_by_status(namespace=target_namespace, query_operator='!=',  status='Running')
            if len(pods_not_yet_in_run_status) > 0:
                for pod in pods_not_yet_in_run_status:
                    logger.info(f"verify_running_pods: waiting for {pod} in ns {target_namespace} to go to Running status out of {len(pods_not_yet_in_run_status)} ")
                    self._oc.wait_for_pod_ready(pod, target_namespace)
                running_pods = self.get_list_of_pods_by_status(namespace=target_namespace, query_operator='=', status='Running')
                logger.info(f':: INFO :: verify_running_pods: {target_namespace} has  {len(running_pods)} in running state, out of total desired: {num_of_pods_expected}')
                if len(running_pods) == num_of_pods_expected:
                    return True
                else:
                    logger.warn(f':: WARNING :: verify_running_pods: {target_namespace} has  {len(running_pods)} in running state, execpted total desired: {num_of_pods_expected}')
                    return False
            else:
                # Necessary to re-cehck for pods in run state incase it changed after the initial check for pods in run status
                running_pods = self.get_list_of_pods_by_status(namespace=target_namespace, query_operator='=',status='Running')
                logger.info(f':: INFO :: verify_running_pods: {target_namespace} has  {len(running_pods)} in running state, out of total desired: {num_of_pods_expected}')
                if len(running_pods) == num_of_pods_expected:
                    return True
                else:
                    logger.warn(f':: WARNING :: verify_running_pods: {target_namespace} has  {len(running_pods)} in running state, execpted total desired: {num_of_pods_expected}')
                    return False

    @logger_time_stamp
    def delete_oadp_source_dataset(self, target_namespace):
        """
        method deletes namespaces used for original backup
        """
        self.oadp_timer(action="start", transaction_name='delete_oadp_source_dataset')
        for n in target_namespace.split(','):
            del_ns_cmd = self.__ssh.run(cmd=f'oc delete ns {n}')
            if del_ns_cmd.find('deleted') < 0:
                print(f"attempt to delete namespace {n} failed")
        self.oadp_timer(action="stop", transaction_name='delete_oadp_source_dataset')

    @logger_time_stamp
    def  verify_pod_presence_and_storage(self, num_of_pods_expected, target_namespace, expected_sc, expected_size, skip_dataset_validation):
        '''
        checks pod presence via func verify_running_pods
        checks pod's pv storage class used, and pv size via func verify_pod_pv_size_and_sc
        '''
        if skip_dataset_validation == True:
            logger.warn('You are skipping dataset validations - verify_pod_presence_and_storage will return True with out checks')
            return True
        check_num_of_pods_and_state = self.verify_running_pods(num_of_pods_expected,target_namespace)
        if check_num_of_pods_and_state == False:
            logger.warn(f"verify_pod_presence_and_storage: Dataset not as expected - did not find {num_of_pods_expected} of pods in namespace {target_namespace}")
            return False
        list_of_pods = self.get_list_of_pods(namespace=target_namespace)
        for p in list_of_pods:
            pod_storage_status = self.verify_pod_pv_size_and_sc(podName=p, expected_ns=target_namespace, expected_sc=expected_sc, expected_size=expected_size)
            if pod_storage_status == False:
                return False
        return True

    @logger_time_stamp
    def busybox_dataset_creation(self, scenario):
        """
        method scales up single NS with busybox pods and oc assets
        """
        num_of_assets_desired = scenario['dataset']['pods_per_ns']
        target_namespace = scenario['args']['namespaces_to_backup']
        pv_size =  scenario['dataset']['pv_size']
        storage = scenario['dataset']['sc']
        role = scenario['dataset']['role']
        self.oadp_timer(action="start", transaction_name='dataset_creation')
        logger.info(f'{role} {num_of_assets_desired} {pv_size} {storage}')
        self.__ssh.run(
            cmd=f'{self.__oadp_path}/{role} {num_of_assets_desired} {pv_size} {storage} > /tmp/dataset-creation.log')
        for i in range(1, num_of_assets_desired + 1):
            print(f'checking for {target_namespace}-{i} in namespace= {target_namespace}')
            self._oc.wait_for_pod_ready(pod_name=f'{target_namespace}-{i}', namespace=target_namespace)
        self.oadp_timer(action="stop", transaction_name='dataset_creation')


    @logger_time_stamp
    def create_oadp_source_dataset(self, scenario):
        """
        This method creates dataset for oadp to work against
        :return:
        """
        num_of_assets_desired = scenario['dataset']['pods_per_ns']
        target_namespace = scenario['args']['namespaces_to_backup']
        pv_size = scenario['dataset']['pv_size']
        storage = scenario['dataset']['sc']
        role = scenario['dataset']['role']
        #
        # # check whether current NS already exists
        # check_ns_presence = self.__ssh.run(cmd=f'oc get ns {target_namespace}')
        # if check_ns_presence.find('not found') < 0:
        #     # delete ns with same name
        #     logger.warn(f"NS {target_namespace} with same name already exists and will be remoed")
        #     check_ns_presence = self.__ssh.run(cmd=f'oc delete ns {target_namespace}')
        #     if check_ns_presence.find('deleted') < 0:
        #         logger.exception("Unable to remove NS {ns} when attempting to clean up before populating NS likely same ns exists on different storage")
        if role == 'BusyBoxPodSingleNS.sh':
            self.busybox_dataset_creation(scenario)
        if role == 'generator':
            self.create_multi_pvutil_dataset(scenario)


    @logger_time_stamp
    def wait_until_process_inside_pod_completes(self, pod_name, namespace, process_text_to_monitor, timeout_value):
        """
        function will run rsh command and check for presence of running process and sleep until its not present or timeoutval exceeded
        """
        try:
            self._oc.wait_for_pod_ready(pod_name=pod_name, namespace=namespace)
            current_wait_time = 0
            while current_wait_time <= int(timeout_value):
                status_cmd = self.__ssh.run(cmd=f"oc  exec -n{namespace} {pod_name} -- /bin/bash -c 'pgrep -flc {process_text_to_monitor}'")
                status_cmd = status_cmd.split('\n')[0]
                if int(status_cmd) == 0:
                    logger.info('process not found in container')
                    return True
                else:
                    logger.info('process is still running in container')
                    time.sleep(3)
            current_wait_time += 3
        except Exception as err:
            logger.info(f'Error in wait_until_process_inside_pod_completes pod {pod_name} timeout waiting for command {process_text_to_monitor} to not be found so raised an exception')

    @logger_time_stamp
    def create_multi_pvutil_dataset(self, test_scenario):
        # common vars between roles
        active_role = test_scenario['dataset']['role']
        playbook_path = test_scenario['dataset']['playbook_path']
        pvc_size = test_scenario['dataset']['pv_size']
        dataset_path = test_scenario['dataset']['dataset_path']
        num_of_pods_expected = test_scenario['dataset']['pods_per_ns']
        create_data_ansible_output_responses = []
        mount_point = test_scenario['dataset']['dataset_path']
        namespace = test_scenario['args']['namespaces_to_backup']
        sc = test_scenario['dataset']['sc']
        testcase_timeout = int(test_scenario['args']['testcase_timeout'])
        # generated_name for pv or namespaces
        generated_name = test_scenario['testcase']
        generated_name = generated_name.replace('.', '-')
        generated_name = 'perf-datagen-' + generated_name.lower() + '-' + sc[-3:]

        # Create Pods via Ansible population flow
        for i in range(num_of_pods_expected):
            pvc_name = 'pvc-' + generated_name + '-' + str(i)
            deployment_name = 'deploy-' + generated_name + '-' + str(i)
            if active_role == 'generator':
                dir_count = test_scenario['dataset']['dir_count']
                files_count = test_scenario['dataset']['files_count']
                file_size = test_scenario['dataset']['files_size']
                dept_count = test_scenario['dataset']['dept_count']
                playbook_extra_var = (
                    f"dir_count={dir_count}  files_count={files_count}  files_size={file_size}  dept_count={dept_count}  pvc_size={pvc_size} deployment_name={deployment_name} dataset_path={dataset_path} namespace={namespace} pvc_name={pvc_name}")

            if active_role == 'dd_generator':
                bs = test_scenario['dataset']['bs']
                count = test_scenario['dataset']['count']
                playbook_extra_var = (
                    f"bs={bs} count={count}  pvc_size={pvc_size}  deployment_name={deployment_name} dataset_path={dataset_path} namespace={namespace} pvc_name={pvc_name}")

            create_data_ansible_output = self.__ssh.run(cmd=f"ansible-playbook {playbook_path}  --extra-vars  '{playbook_extra_var}' -vvv")
            create_data_ansible_output_responses.append(create_data_ansible_output)

        # Validate Ansible Play execution is as expected
        # sample expected output ok=4    changed=2    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0
        num_of_validated_ansible_plays = 0
        for response in create_data_ansible_output_responses:
            ansible_play_invoked_successfully = self.validate_ansible_play(response)
            if not ansible_play_invoked_successfully:
                logger.warning(f"Ansible playbook validation failed for: {response}")
            else:
                num_of_validated_ansible_plays = num_of_validated_ansible_plays + 1
        logger.info(f"::INFO:: Total ansible plays verified to have executed without runtime issues are: {num_of_validated_ansible_plays} out of expected {num_of_pods_expected} ")

        # Verify pods in run state that are executing population on their pvs match expected numbers
        # Previous check was for execution of ansible this check is for the pods which are in run state
        pods_ready_for_pv_util_validation = self.verify_running_pods(num_of_pods_expected=num_of_pods_expected, target_namespace=namespace)
        if not pods_ready_for_pv_util_validation:
            logger.error(f"Number of created pods running: {len(created_pods)} expected pods running should be {num_of_pods_expected}")
        else:
            running_pods = self.get_list_of_pods(namespace=namespace)
            for pod in running_pods:
                self.wait_until_process_inside_pod_completes(pod_name=pod, namespace=namespace, process_text_to_monitor=mount_point,timeout_value=testcase_timeout)
                logger.info(f"::INFO:: Population process inside pod {pod} in ns {namespace} has completed")


    @logger_time_stamp
    def validate_ansible_play(self, playbook_output):
        """
        parses ansible playbook output to verify stdout cotians unreachable=0  failed=0
        """
        if playbook_output == '':
            logger.exception(f"ansible-playbook stdout was empty and should not have been")
        # parse data to get value for changed, unreachable, failed
        failed_count = re.search(r'failed=(\d+)', playbook_output)
        unreachable_count = re.search(r'unreachable=(\d+)', playbook_output)
        if ((failed_count == None) or (unreachable_count == None)):
            logger.error(f"ansible-playbook stdout did not contain expected values with regards to failed or changed or unreachable see: {playbook_output}")
            return False
        else:
            if (int(failed_count.group(1)) == 0) and (int(unreachable_count.group(1)) == 0):
                logger.info(f'ansible-playbook output ran without failures or unreachable errors')
                return True
            else:
                logger.warn(f"ansible-playbook stdout did not contain expected values with regards to number of failures and unreachable related tasks : {playbook_output}")
                return False

    @logger_time_stamp
    def get_pod_pv_utilization_info_by_podname(self, test_scenario, podname):
        results_capacity_usage = {}
        active_role = test_scenario['dataset']['role']
        mount_point = test_scenario['dataset']['dataset_path']
        namespace = test_scenario['args']['namespaces_to_backup']
        disk_capacity = self.__ssh.run(
            cmd=f"oc  exec -it -n{namespace} {podname} -- /bin/bash -c \"du -sh {mount_point}\"")
        current_disk_capacity = disk_capacity.split('\n')[-1].split('\t')[0]
        unit_disk_capacity = current_disk_capacity[-1]
        results_capacity_usage['disk_capacity'] = current_disk_capacity
        files_count = self.__ssh.run(
            cmd=f"oc  exec -it -n{namespace} {podname} -- /bin/bash -c \"find {mount_point}* -type f -name \"my-random-file-*\" -o -name \"dd_file\" |wc -l\"")
        current_files_count = files_count.split('\n')[-1].split('\t')[0]
        results_capacity_usage['files_count'] = current_files_count
        folders_count = self.__ssh.run(
            cmd=f"oc  exec -it -n{namespace} {podname} -- /bin/bash -c \"find {mount_point}python/* -type d  |wc -l\"")
        current_folders_count = folders_count.split('\n')[-1].split('\t')[0]
        results_capacity_usage['folders_count'] = current_folders_count
        results_capacity_usage['active_role'] = active_role
        logger.info(f"get_pod_pv_utilization_info saw pv contained: {results_capacity_usage}")

    @logger_time_stamp
    def get_pod_pv_utilization_info_by_podname(self, test_scenario, podname):
        results_capacity_usage = {}
        active_role = test_scenario['dataset']['role']
        mount_point = test_scenario['dataset']['dataset_path']
        namespace = test_scenario['args']['namespaces_to_backup']
        disk_capacity = self.__ssh.run(
            cmd=f"oc  exec -it -n{namespace} {podname} -- /bin/bash -c \"du -sh {mount_point}\"")
        current_disk_capacity = disk_capacity.split('\n')[-1].split('\t')[0]
        unit_disk_capacity = current_disk_capacity[-1]
        results_capacity_usage['disk_capacity'] = current_disk_capacity
        files_count = self.__ssh.run(
            cmd=f"oc  exec -it -n{namespace} {podname} -- /bin/bash -c \"find {mount_point}* -type f -name \"my-random-file-*\" -o -name \"dd_file\" |wc -l\"")
        current_files_count = files_count.split('\n')[-1].split('\t')[0]
        results_capacity_usage['files_count'] = current_files_count
        folders_count = self.__ssh.run(
            cmd=f"oc  exec -it -n{namespace} {podname} -- /bin/bash -c \"find {mount_point}python/* -type d  |wc -l\"")
        current_folders_count = folders_count.split('\n')[-1].split('\t')[0]
        results_capacity_usage['folders_count'] = current_folders_count
        results_capacity_usage['active_role'] = active_role
        logger.info(f"get_pod_pv_utilization_info saw pv contained: {results_capacity_usage}")
        return results_capacity_usage

    @logger_time_stamp
    def get_pod_pv_utilization_info(self, test_scenario):
        results_capacity_usage = {}
        active_role = test_scenario['dataset']['role']
        mount_point = test_scenario['dataset']['dataset_path']
        namespace = test_scenario['args']['namespaces_to_backup']
        podname = self.__ssh.run(cmd=f"oc get pods -o custom-columns=POD:.metadata.name --no-headers -n{namespace}")
        disk_capacity = self.__ssh.run(cmd=f"oc  exec -it -n{namespace} {podname} -- /bin/bash -c \"du -sh {mount_point}\"")
        current_disk_capacity = disk_capacity.split('\n')[-1].split('\t')[0]
        unit_disk_capacity = current_disk_capacity[-1]
        results_capacity_usage['disk_capacity'] = current_disk_capacity
        files_count = self.__ssh.run(cmd=f"oc  exec -it -n{namespace} {podname} -- /bin/bash -c \"find {mount_point}* -type f -name \"my-random-file-*\" -o -name \"dd_file\" |wc -l\"")
        current_files_count = files_count.split('\n')[-1].split('\t')[0]
        results_capacity_usage['files_count'] = current_files_count
        folders_count = self.__ssh.run(cmd=f"oc  exec -it -n{namespace} {podname} -- /bin/bash -c \"find {mount_point}python/* -type d  |wc -l\"")
        current_folders_count = folders_count.split('\n')[-1].split('\t')[0]
        results_capacity_usage['folders_count'] = current_folders_count
        results_capacity_usage['active_role'] = active_role
        logger.info(f"get_pod_pv_utilization_info saw pv contained: {results_capacity_usage}")
        return results_capacity_usage

    @logger_time_stamp
    def pv_contains_expected_data(self, test_scenario, pv_util_details_returned_by_pod):
        """
        method compares data returned from get_pod_pv_utilization_info against data from the yaml
        """
        if (pv_util_details_returned_by_pod['disk_capacity'] != test_scenario['dataset']['expected_capacity']):
            logger.warning(f":: ERROR :: pv_contains_expected_data disk_capacity FAILED comparison pod returned: {pv_util_details_returned_by_pod['disk_capacity']} yaml expected: {test_scenario['dataset']['expected_capacity']}")
            return False
        else:
            logger.info(f":: INFO :: pv_contains_expected_data: disk_capacity returned by pod: {pv_util_details_returned_by_pod['disk_capacity']} matches what yaml expected: {test_scenario['dataset']['expected_capacity']}")

        total_expected_files_assuming_its_per_by_folder = (int(test_scenario['dataset']['files_count']) * test_scenario['dataset']['dir_count'])
        total_expected_files_assuming_its_already_the_raw_total = int(test_scenario['dataset']['files_count'])
        if not (total_expected_files_assuming_its_per_by_folder == int(pv_util_details_returned_by_pod['files_count']) ) and not (total_expected_files_assuming_its_already_the_raw_total == int(pv_util_details_returned_by_pod['files_count'])):
            logger.warning(f":: ERROR :: pv_contains_expected_data files_count FAILED comparison pod returned: {pv_util_details_returned_by_pod['files_count']} yaml expected: {test_scenario['dataset']['files_count']} when total_expected_files_assuming_its_per_by_folder: {total_expected_files_assuming_its_per_by_folder}")
            return False
        else:
            logger.info(f":: INFO :: pv_contains_expected_data: files_count comparison succeeded pod returned: {pv_util_details_returned_by_pod['files_count']} yaml expected: {test_scenario['dataset']['files_count']} or when total_expected_files_assuming_its_per_by_folder: {total_expected_files_assuming_its_per_by_folder}")
        if (int(pv_util_details_returned_by_pod['folders_count']) != test_scenario['dataset']['dir_count']):
            logger.warning(f":: ERROR :: pv_contains_expected_data folders_count failed comparison pod returned: {pv_util_details_returned_by_pod['folders_count']} yaml expected: {test_scenario['dataset']['dir_count']}")
            return False
        else:
            logger.info(f":: INFO :: pv_contains_expected_data folders_count comparison succeeded: pod returned: {pv_util_details_returned_by_pod['folders_count']} yaml expected: {test_scenario['dataset']['dir_count']}")
        logger.info(f':: INFO :: pv_contains_expected_data is returning True based on pv_util_details_returned_by_pod')
        return True



    def get_expected_files_count(self, test_scenario):
        results_capacity_expected = {}
        import math
        active_role = test_scenario['dataset']['role']
        dir_count = test_scenario['dataset']['dir_count'] # -n
        files_count = test_scenario['dataset']['files_count'] # -f
        dept_count = test_scenario['dataset']['dept_count'] # -d
        file_size = test_scenario['dataset']['files_size'] # -s
        if active_role == 'generator':
            countdir = 0
            for k in range(dept_count):
                countdir += math.pow(dept_count, k)
            countfile = int(math.pow(dept_count, (dept_count - 1)) * files_count)
            total_files = int(countfile * dir_count)
            total_folders = int(countdir * dir_count)
            disk_capacity = round(int(total_files * file_size) / 1024 / 1024)
            results_capacity_expected['disk_capacity'] = disk_capacity
            results_capacity_expected['files_count'] = total_files
            results_capacity_expected['folders_count'] = total_folders
            logger.info(f"{total_folders} {total_files}")
            logger.info(results_capacity_expected)
        elif active_role == 'dd_generator':
             bs = test_scenario['dataset']['bs']
             count = test_scenario['dataset']['count']
             current_file_size = bs * count
        return results_capacity_expected

    def capacity_usage_and_expected_comparison(self, results_capacity_expected, results_capacity_usage):
        if len(results_capacity_expected) != len(results_capacity_usage):
            print("Not Equal")
        else:
            flag = 0
            for i in results_capacity_expected:
                if results_capacity_expected.get(i) != results_capacity_usage:
                    flag = 1
                    break
            if flag == 0:
                print("Equal")
            else:
                print("Not equal")



    @logger_time_stamp
    def get_oadp_custom_resources(self, cr_type, ns='openshift-adp'):
        """
        This method return backups as list
        :return: list of crs like backups or restore
        """
        cmd_output = self.__ssh.run(cmd=f'oc get {cr_type} -n {ns} -o jsonpath="{{.items[*].metadata.name}}"')
        list_of_crs = list(filter(None, cmd_output.split(' ')))
        return list_of_crs

    @logger_time_stamp
    def oadp_timer(self, action, transaction_name):
        """
        this method is for marking start / stop for oadp actions
        """
        if action == 'start':
            time_start = datetime.now()
            transaction = {"transaction_name": transaction_name, "start_at": time_start, "stopped_at": [], "duration": []}
            self.__run_metadata['summary']['transactions'].append(transaction)
        elif action == 'stop':
            time_end = datetime.now()
            for trans in range(len(self.__run_metadata['summary']['transactions'])):
                if self.__run_metadata['summary']['transactions'][trans]['transaction_name'] == transaction_name:
                    print(f"{self.__run_metadata['summary']['transactions'][trans]}")
                    self.__run_metadata['summary']['transactions'][trans]['stopped_at'] = time_end
                    self.__run_metadata['summary']['transactions'][trans]['duration'] = str(
                        time_end - self.__run_metadata['summary']['transactions'][trans]['start_at'])


    @logger_time_stamp
    def oadp_restore(self, plugin, restore_name, backup_name):
        """
        this method is for restoring oadp backups
      os  """
        #              cmd: "oc -n openshift-adp exec deployment/velero -c velero -it -- ./velero restore create {{restore_name}}  --from-backup {{backup_name}}"
        restore_cmd = self.__ssh.run(cmd=f'oc -n openshift-adp exec deployment/velero -c velero -it -- ./velero restore create {restore_name} --from-backup {backup_name}')
        if restore_cmd.find('submitted successfully') == 0:
            print("Error restore was not successfully started")
            logging.error(f'Error restore did not execut stdout {restore_cmd}')

    @logger_time_stamp
    def oadp_create_backup(self, plugin, backup_name, namespaces_to_backup):
        """
        this method is for testing oadp backup
        """
        if plugin == 'restic':
            backup_cmd = self.__ssh.run(
                cmd=f'oc -n openshift-adp exec deployment/velero -c velero -it -- ./velero backup create {backup_name} --include-namespaces {namespaces_to_backup} --default-volumes-to-fs-backup=true --snapshot-volumes=false')
            if backup_cmd.find('submitted successfully') == 0:
                print("Error backup attempt failed !!! ")
                logger.error(f"Error backup attempt failed stdout from command: {backup_cmd}")
        if plugin == 'csi':
            backup_cmd = self.__ssh.run(
                cmd=f'oc -n openshift-adp exec deployment/velero -c velero -it -- ./velero backup create {backup_name} --include-namespaces {namespaces_to_backup}')
            if backup_cmd.find('submitted successfully') == 0:
                print("Error backup attempt failed !!! ")
                logger.error(f"Error backup attempt failed stdout from command: {backup_cmd}")

    @logger_time_stamp
    def wait_for_condition_of_oadp_cr(self, cr_type, cr_name, testcase_timeout=43200):
        """
        method polls for condition of OADP CR
        """
        if not self.is_oadp_cr_present(ns='openshift-adp', cr_type=cr_type, cr_name=cr_name):
            logger.info(f'{cr_name} OADPWaitForConditionTimeout raised an exception in is_oadp_cr_present returned false')
        jsonpath = "'{.status.phase}'"
        get_state = self.__ssh.run(cmd=f"oc get {cr_type}/{cr_name} -n openshift-adp -o jsonpath={jsonpath}")
        try:
            current_wait_time = 0
            while current_wait_time <= testcase_timeout:
                state = self.__ssh.run(
                    cmd=f"oc get {cr_type}/{cr_name} -n openshift-adp -o jsonpath={jsonpath}")
                if state in ['Completed', 'Failed', 'PartiallyFailed']:
                    print(f"current status: CR {cr_name} state: {state} in ['Completed', 'Failed', 'PartiallyFailed']")
                    return True
                    # sleep for x
                else:
                    print(f"current cr state is: {state} meaning its still running as its NOT in 'Completed', 'Failed', 'PartiallyFailed'")
                    time.sleep(3)
            current_wait_time += 3
        except Exception as err:
            logger.info(f'{cr_name} OADPWaitForConditionTimeout raised an exception')


    @logger_time_stamp
    def is_oadp_cr_present(self, ns, cr_type, cr_name):
        """
        This method returns true or false regarding CR presence
        """
        list_of_crs = self.get_oadp_custom_resources(cr_type, ns)
        if cr_name in list_of_crs:
            return True
        else:
            return False

    def verify_volsync_present(self):
        """
        return true or false if volsync present
        """
        cmd_volsync_status = self.__ssh.run(
            cmd="oc get csv -n openshift-operators | grep VolSync | awk {'print $5'}")
        if 'error' in cmd_volsync_status or cmd_volsync_status == '':
            logger.error(
                f':: ERROR :: Volsync not installed {cmd_volsync_status}')
            return False
        if cmd_volsync_status == 'Succeeded':
            logger.info(':: INFO :: Volsync is present')
            return True

    def setup_ocs_cephfs_shallow(self):
        """
        sets up ocs-storagecluster-cephfs-shallow for 4.12
        """
        cmd_create_cephfs_shallow_sc = self.__ssh.run(cmd=f"oc apply -f {self.__oadp_base_dir}/templates/cephfs-shallow.yaml")
        if (not 'created' in cmd_create_cephfs_shallow_sc and not 'configured' in cmd_create_cephfs_shallow_sc):
            logger.error(f':: ERROR :: cephfs-shallow not deployed correctly - following output returned: {cmd_create_cephfs_shallow_sc}')
        else:
            logger.info(':: INFO :: cephfs-shallow sc is present')

    def config_dpa_for_cephfs_shallow(self, enable, oadp_namespace):
        """
        method adds or removes cephfs-shallow to dpa
        """
        if enable:
            query = '{"spec": {"features": {"dataMover": {"volumeOptions": {"destinationVolumeOptions": {"accessMode": "ReadOnlyMany","cacheAccessMode": "ReadWriteOnce","storageClassName": "ocs-storagecluster-cephfs-shallow","moverSecurityContext": true}, "sourceVolumeOptions": {"accessMode": "ReadOnlyMany","cacheAccessMode": "ReadWriteMany","cacheStorageClassName": "ocs-storagecluster-cephfs","moverSecurityContext": true,"storageClassName": "ocs-storagecluster-cephfs-shallow"}}}}}}'
            self.patch_oc_resource(resource_type='dpa', resource_name='example-velero', namespace=oadp_namespace,
                                   patch_type='merge',
                                   patch_json=query)
        if not enable:
            query = '[{"op":"remove", "path": "/spec/features/dataMover/volumeOptions"}]'
            self.patch_oc_resource(resource_type='dpa', resource_name='example-velero', namespace=oadp_namespace,
                                   patch_type='json',
                                   patch_json=query)


    def config_datamover(self, oadp_namespace, scenario):
        """
        method for customizing datamover concurrency and timeout
        eg: # setting values in dpa from yaml
            datamover:
                maxConcurrentBackupVolumes": 30
                maxConcurrentRestoreVolumes": 10
                timeout": 5m
            or
            # removing values from DPA
            dataMover:
               maxConcurrentBackupVolumes: remove
               maxConcurrentRestoreVolumes: remove
               timeout: remove
        if any of value of these key pairs =='remove'
        """
        set_maxConcurrentBackupVolumes = scenario['config']['dataMover'].get('maxConcurrentBackupVolumes', False)
        set_maxConcurrentRestoreVolumes = scenario['config']['dataMover'].get('maxConcurrentRestoreVolumes', False)
        set_timeout = scenario['config']['dataMover'].get('timeout', False)

        if set_maxConcurrentBackupVolumes != False:
            value = scenario['config']['dataMover']['maxConcurrentBackupVolumes']
            if value != 'remove':
                query = '{"spec": {"features": {"dataMover": {"maxConcurrentBackupVolumes": ' + f'"{value}"' + '}}}}'
                self.patch_oc_resource(resource_type='dpa', resource_name='example-velero', namespace=oadp_namespace,
                                   patch_type='merge',
                                   patch_json=query)
            else:
                query = '[{"op":"remove", "path": "/spec/features/dataMover/maxConcurrentBackupVolumes"}]'
                self.patch_oc_resource(resource_type='dpa', resource_name='example-velero', namespace=oadp_namespace,
                                       patch_type='json',
                                       patch_json=query)

        if set_maxConcurrentRestoreVolumes != False:
            value = scenario['config']['dataMover']['maxConcurrentRestoreVolumes']
            if value != 'remove':
                query = '{"spec": {"features": {"dataMover": {"maxConcurrentRestoreVolumes": ' + f'"{value}"' + '}}}}'
                self.patch_oc_resource(resource_type='dpa', resource_name='example-velero', namespace=oadp_namespace,
                                   patch_type='merge',
                                   patch_json=query)
            else:
                query = '[{"op":"remove", "path": "/spec/features/dataMover/maxConcurrentRestoreVolumes"}]'
                self.patch_oc_resource(resource_type='dpa', resource_name='example-velero', namespace=oadp_namespace,
                                       patch_type='json',
                                       patch_json=query)

        if set_timeout != False:
            value = scenario['config']['dataMover']['timeout']
            if value != 'remove':
                query = '{"spec": {"features": {"dataMover": {"timeout": ' + f'"{value}"' + '}}}}'
                self.patch_oc_resource(resource_type='dpa', resource_name='example-velero', namespace=oadp_namespace,
                                   patch_type='merge',
                                   patch_json=query)
            else:
                query = '[{"op":"remove", "path": "/spec/features/dataMover/timeout"}]'
                self.patch_oc_resource(resource_type='dpa', resource_name='example-velero', namespace=oadp_namespace,
                                       patch_type='json',
                                       patch_json=query)

    def enable_datamover(self, oadp_namespace, scenario):
        """
        # can handle sc and vsc defaults before this invoked.
        method enables vsm / datamover
        0) Get DPA values
        1) enable restic secret via oc apply -f yaml
        2) Set SC (in this case rbd per scenario details)
        3) disabple restic plugin
        4) Set SC
        5) enable_vsm_plugin
        6) patch dataprotectionapplication example-velero -n openshift-adp --type=json -p='[{"op": "replace", "path": "/spec/backupLocations/0/velero/config/s3Url", "value": "http://s3-openshift-storage.apps.cloud20.rdu2.scalelab.redhat.com"}]'
        """
        # Get DPA contents
        dpa_data = self.get_oc_resource_to_json(resource_type='dpa', resource_name=self.__oadp_dpa, namespace=oadp_namespace)
        if bool(dpa_data) == False:
            logger.error(':: ERROR :: DPA is not present command to get dpa as json resulted in empty dict')

        dpa_name = dpa_data['metadata']['name']
        is_restic_enabled = dpa_data['spec']['configuration']['restic']['enable']
        velero_enabled_plugins = dpa_data['spec']['configuration']['velero']['defaultPlugins']
        # check for restic secret
        restic_secret = self.get_oc_resource_to_json(resource_type='secret', resource_name='restic-secret', namespace=oadp_namespace)
        if bool(restic_secret) == False:
            logger.info(':: INFO :: Restic Secret not found - will deploy it now')
            cmd_restic_secret_apply = self.__ssh.run(cmd=f"oc apply -f {self.__oadp_base_dir}/templates/restic_secret.yaml")
            if (not 'created'  in  cmd_restic_secret_apply and not 'configured' in cmd_restic_secret_apply):
                logger.error(':: ERROR :: Restic Secret not deployed correctly - following output returned: {}')
            else:
                logger.info(':: INFO :: Restic Secret created successfully')
        # patch restic-secret to dpa
        self.patch_oc_resource(resource_type='dpa', resource_name='example-velero', namespace='openshift-adp', patch_type='merge', patch_json='{"spec": {"features": {"dataMover": {"credentialName": "restic-secret"}}}}')
        #  Enable datamover / VSM
        if is_restic_enabled:
            # disabple restic plugin
            json_query = '{"spec": {"configuration": {"restic": {"enable": false}}}}'
            disable_restic_plugin = self.__ssh.run(cmd=f"oc patch dpa {dpa_name} -n {oadp_namespace} --type merge -p '{json_query}'")
            if 'error' in disable_restic_plugin:
                logger.error(f':: ERROR :: Attemmpted to disable restic - following output returned: {disable_restic_plugin}')
            else:
                logger.info(':: INFO :: Restic Secret disabled successfully')
        # Get S3 URL
        json_query = '{.spec.host}'
        cmd_get_s3_url = self.__ssh.run(cmd=f" oc get route s3 -n openshift-storage -o jsonpath='{json_query}'")
        if (cmd_get_s3_url != '') and (not 'error' in cmd_get_s3_url):
            logger.info(f':: INFO :: Setting S3 {cmd_get_s3_url} to {dpa_name}')
            json_query = f"""[{{"op": "replace", "path": "/spec/backupLocations/0/velero/config/s3Url", "value": "http://{cmd_get_s3_url}"}}]"""
            cmd_setting_s3_in_dpa = self.__ssh.run( cmd=f"oc patch dataprotectionapplication {dpa_name} -n {oadp_namespace} --type=json -p='{json_query}'")
            if (cmd_setting_s3_in_dpa != '') and (not 'error' in cmd_setting_s3_in_dpa):
                logger.info(f':: INFO :: S3 set sucessfully {cmd_setting_s3_in_dpa} to {dpa_name}')
        self.verify_volsync_present()
        # enable vsm
        if 'vsm' not in velero_enabled_plugins:
            logger.info(f':: INFO :: Setting VSM to defaultplugins in {dpa_name}')
            json_query = """[{"op": "add", "path": "/spec/configuration/velero/defaultPlugins/-", "value": "vsm"}]"""
            cmd_setting_vsm_in_defaultplugins = self.__ssh.run(
                cmd=f"oc patch dataprotectionapplication {dpa_name} -n {oadp_namespace} --type=json -p='{json_query}'")
            if (cmd_setting_vsm_in_defaultplugins != '') and (not 'error' in cmd_setting_vsm_in_defaultplugins):
                logger.info(f':: INFO :: VSM to defaultplugins in {dpa_name}')

        json_query = '{"spec": {"features": {"dataMover": {"enable": true}}}}'
        enable_dataMover = self.__ssh.run(cmd=f"oc patch dpa {dpa_name} -n {oadp_namespace} --type merge -p '{json_query}'")
        if (enable_dataMover != '') and (not 'error' in enable_dataMover):
            logger.info(f':: INFO :: Datamover is now enabled for {dpa_name}')

    def disable_datamover(self, oadp_namespace):
        """
        method disables datamover from dpa
        """
        query = '{"spec": {"features": {"dataMover": {"enable": false}}}}'
        self.patch_oc_resource(resource_type='dpa', resource_name='example-velero', namespace=oadp_namespace,
                               patch_type='merge',
                               patch_json=query)

    @logger_time_stamp
    def set_volume_snapshot_class(self, sc):
        """
        Sets volume snapshot class per storage
        """
        if sc == 'ocs-storagecluster-ceph-rbd':
            cmd_set_volume_snapshot_class = self.__ssh.run(cmd=f"oc apply -f {self.__oadp_base_dir}/vsc-cephRBD.yaml")
        if sc == 'ocs-storagecluster-cephfs' or 'ocs-storagecluster-cephfs-shallow':
            cmd_set_volume_snapshot_class = self.__ssh.run(cmd=f"oc apply -f {self.__oadp_base_dir}/vsc-cephFS.yaml")
        expected_result_output = ['created', 'unchanged', 'configured']
        if any(ext in cmd_set_volume_snapshot_class for ext in expected_result_output) == False:
            print(f"Unable to set volume-snapshot-class {sc} ")
            logger.exception(f"Unable to set volume-snapshot-class {sc}")

    @logger_time_stamp
    def set_default_storage_class(self,sc):
        """
        method returns default sc if not set then empy string returned
        """
        current_sc = self.get_default_storage_class()
        if current_sc != sc:
            if current_sc == 'ocs-storagecluster-cephfs-shallow':
                current_oc_version = packaging.version.parse(self._oc.get_ocp_server_version())
                minimal_supported_oc_version = packaging.version.parse('4.12.0')
                if current_oc_version < minimal_supported_oc_version:
                    logger.error(f":: ERROR :: CEPHFS-Shallow set to desired sc on unsupported oc version.  Please check your yaml scenario {self.__oadp_scenario_name}  for the storage class is set {sc} which requires {minimal_supported_oc_version}, this env is: {current_oc_version} ")
            #set desired sc as default
            json_sc = '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
            set_def_sc_cmd = self.__ssh.run(cmd=f"oc patch storageclass {sc} -p '{json_sc}'")
            if set_def_sc_cmd.find('patched') < 0:
                print(f"Unable to set {sc} as default storage class")
                logger.exception(f"Unable to set {sc} as default storage class")
        # Verify other storage classes present are not set as default storage
        cmd_output = self.__ssh.run(cmd=f'oc get sc -o jsonpath="{{.items[*].metadata.name}}"')
        list_of_storage_class_present = list(filter(None, cmd_output.split(' ')))
        for storage in list_of_storage_class_present:
            if storage != sc:
                # set default storage class to false
                json_sc = '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}'
                set_sc_as_non_default_cmd = self.__ssh.run(cmd=f"oc patch storageclass {storage} -p '{json_sc}'")
                if set_sc_as_non_default_cmd.find('patched') < 0:
                    logger.warn(f"Note that storage {storage} was set  is-default-class:false as its not desired sc of {sc} ")

    def patch_oc_resource(self, resource_type, resource_name, namespace, patch_type, patch_json):
        """
        method returns oc resource info
        """
        oc_patch_cmd = ''
        if patch_type == 'json':
            oc_patch_cmd = f"oc patch {resource_type} {resource_name} -n {namespace} --type={patch_type} -p='{patch_json}'"
        else:
            oc_patch_cmd = f"oc patch {resource_type} {resource_name} -n {namespace} --type {patch_type} -p '{patch_json}'"
        logger.info (f":: INFO :: Attempting OC PATCH => {oc_patch_cmd}")
        oc_patch_response = self.__ssh.run(cmd=f"{oc_patch_cmd}")
        expected_result_output = ['patched', 'unchanged', 'configured', 'no change']
        # negative_result_output = ['not found', 'invalid', 'conflict', 'unauthorized', 'forbidden']
        if any(ext in oc_patch_response for ext in expected_result_output) == False:
            logger.exception(f":: ERROR :: Unable to process patch command: {oc_patch_cmd}  resulted in {oc_patch_response}")
        else:
            logger.info(
                f":: INFO :: Succesful patch command: {oc_patch_cmd}  resulted in {oc_patch_response}")

    @logger_time_stamp
    def get_default_storage_class(self):
        """
        method returns default sc if not set then empy string returned
        """
        jsonpath_check_default_sc = """'{.items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")].metadata.name}'"""
        default_sc_cmd = self.__ssh.run(cmd=f"oc get storageclass -o=jsonpath={jsonpath_check_default_sc}")
        print(f'get_default_storage_class returned: {default_sc_cmd}')
        return default_sc_cmd

    @logger_time_stamp
    def oadp_get_version_info(self):
        """
        method returns relevant oadp version info
        oadp:
            oadp_operator_container_image: oc get deployment openshift-adp-controller-manager --namespace=openshift-adp --output=jsonpath='{.spec.template.spec.containers[0].image}'
            oadp_csv: oc get deployments --all-namespaces --field-selector='metadata.name=openshift-adp-controller-manager' --output=jsonpath='{.items[0].metadata.labels.olm\\.owner}'
            oadp_csv_creation_on: oc get -n openshift-adp csv -o=jsonpath='{.items[0].metadata.annotations.createdAt}'
                # note the following var requires output of oadp_csv to be inserted at oadp-operator-v1.1.1 value
            oadp_subscription_used: oc get subscription.operators.coreos.com --namespace='openshift-adp' --output=jsonpath='{.items[?(@.status.installedCSV == "oadp-operator.v1.1.1")].metadata.name}'
        """
        oadp_details = {
            "oadp": {}
        }

        jsonpath_oadp_operator_container_image = "'{.spec.template.spec.containers[0].image}'"
        oadp_operator_container_image = self.__ssh.run(
            cmd=f"oc get deployment openshift-adp-controller-manager --namespace=openshift-adp -o jsonpath={jsonpath_oadp_operator_container_image}")
        if oadp_operator_container_image != '':
            oadp_details['oadp']['oadp_operator_container_image'] = oadp_operator_container_image

        jsonpath_oadp_csv = "'{.items[0].metadata.labels.olm\.owner}'"
        oadp_csv = self.__ssh.run(
            cmd=f"oc get deployments --all-namespaces --field-selector='metadata.name=openshift-adp-controller-manager' -o jsonpath={jsonpath_oadp_csv}")
        if oadp_csv != '':
            oadp_details['oadp']['oadp_csv'] = oadp_csv

        jsonpath_oadp_csv_creation_time = "'{.items[0].metadata.annotations.createdAt}'"
        oadp_csv_creation_time = self.__ssh.run(
            cmd=f"oc get -n openshift-adp csv  -o jsonpath={jsonpath_oadp_csv_creation_time}")
        if oadp_csv_creation_time != '':
            oadp_details['oadp']['oadp_csv_creation_time'] = oadp_csv_creation_time.split('.')[0]

        jsonpath_oadp_subscription_used = "{.items[?(@.status.installedCSV == " + f'"{oadp_csv}"' + ")].metadata.name}"
        oadp_subscription_used = self.__ssh.run(
            cmd=f"oc get subscription.operators.coreos.com --namespace='openshift-adp' -o jsonpath='{jsonpath_oadp_subscription_used}'")
        if oadp_subscription_used != '':
            oadp_details['oadp']['subscription'] = oadp_subscription_used

        jsonpath_oadp_catalog_source = "'{.spec.source}'"
        oadp_catalog_source = self.__ssh.run(
            cmd=f"oc get subscription.operators.coreos.com {oadp_subscription_used} --namespace='openshift-adp' -o jsonpath={jsonpath_oadp_catalog_source}")
        if oadp_catalog_source != '':
            oadp_details['oadp']['catalog_source'] = oadp_catalog_source
            #get iib here
            jsonpath_oadp_iib = "'{.spec.image}'"
            oadp_iib_cmd =  self.__ssh.run(cmd=f"oc get catsrc {oadp_catalog_source} -n openshift-marketplace -o jsonpath={jsonpath_oadp_iib} --ignore-not-found | grep -Eo 'iib:[0-9]+'")
            if oadp_iib_cmd != '':
                oadp_internal_build = self.__ssh.run(cmd=f"curl -s -k https://datagrepper.engineering.redhat.com/raw\?topic\=/topic/VirtualTopic.eng.ci.redhat-container-image.index.built\&contains\={oadp_iib_cmd}\&rows_per_page\=1\&delta\=15552000 | jq -r '.raw_messages[0].msg.artifact.nvr'")
                if oadp_internal_build != '':
                    oadp_details['oadp']['internal_build'] = oadp_internal_build.split('oadp-operator-bundle-container-')[1]
                    oadp_details['oadp']['iib'] = oadp_iib_cmd.split('iib:')[1]

        jsonpath_cluster_name = "'{print $2}'"
        cluster_name = self.__ssh.run(
            cmd=f"oc get route/console -n openshift-console | grep -v NAME | awk {jsonpath_cluster_name}")
        if cluster_name != '':
            self.__run_metadata['summary']['env']['ocp']['cluster'] = cluster_name

        get_ocp_version_cmd = self.__ssh.run(cmd=f"oc version | grep 'Server Version'")
        ocp_version = get_ocp_version_cmd.split('Version:')[1]
        if ocp_version != '':
            self.__run_metadata['summary']['env']['ocp']['version'] = ocp_version

        # get number of masters
        get_masters_cmd = self.__ssh.run(
            cmd="""oc get nodes -l node-role.kubernetes.io/master -o jsonpath='{.items[*].metadata.name}'""")
        num_of_masters = len(get_masters_cmd.split(' '))
        if num_of_masters != '':
            self.__run_metadata['summary']['env']['ocp']['num_of_masters'] = num_of_masters

        get_workers_cmd = self.__ssh.run(
            cmd="""oc get nodes -l node-role.kubernetes.io/worker -o jsonpath='{.items[*].metadata.name}'""")
        num_of_workers = len(get_workers_cmd.split(' '))
        if num_of_workers != '':
            self.__run_metadata['summary']['env']['ocp']['num_of_workers'] = num_of_workers

        self.__run_metadata['summary']['env'].update(oadp_details)
        self.__result_dicts.append(self.__run_metadata['summary']['env'])

    @logger_time_stamp
    def delete_oadp_custom_resources(self, ns, cr_type, cr_name):
        """
        This method can delete backup or delete cr
        cr_name allows for specifying specific CR or '*' will delete all CRs
        """
        self.oadp_timer(action="start", transaction_name='Delete existing OADP CR')
        if cr_name == '*':
            list_of_crs_to_delete = self.get_oadp_custom_resources(cr_type, ns)
            if len(list_of_crs_to_delete) > 0:
                for i in range(len(list_of_crs_to_delete)):
                    del_cmd = self.__ssh.run(
                        cmd=f'oc -n {ns} exec deployment/velero -c velero -it -- ./velero {cr_type} delete {list_of_crs_to_delete[i]} --confirm')
                    if del_cmd.find('submitted successfully') < 0:
                        print("Error did not delete successfully")
        else:
            if cr_name != '*' and cr_name != '':
                del_cmd = self.__ssh.run(
                    cmd=f'oc -n {ns} exec deployment/velero -c velero -it -- ./velero {cr_type} delete {cr_name} --confirm')
                if del_cmd != None and ('submitted successfully' in del_cmd or 'deleted' in del_cmd):
                    logger.info("Error delete completed successfully")
                elif del_cmd != None and f"No {cr_type}s found" in del_cmd:
                    logger.info('CR not found so delete failed as it doesnt exist')
                else:
                    logger.info(f"=== Attempt to delete was not successful the output was: {del_cmd}")
        self.oadp_timer(action="stop", transaction_name='Delete existing OADP CR')

    @logger_time_stamp
    def find_test_scenario_index(self, scenario_name):
        """
        This method return index of which test scenarios
        to supply details of oadp test scenario to run
        """
        if os.path.exists(os.path.join(self.__oadp_scenario_data)) and not os.stat(
                self.__oadp_scenario_data).st_size == 0:
            test_data = yaml.safe_load(Path(self.__oadp_scenario_data).read_text())
            for index in range(len(test_data['scenarios'])):
                if test_data['scenarios'][index]['name'] == scenario_name:
                    print(f"{test_data['scenarios'][index]['name']}  == f{scenario_name}")
                    return index
                else:
                    print(f" no match found on try {index}")
        else:
            print('Yaml for test scenarios is not found or empty!!')
            logger.error('Test Scenario index is not found')
            logger.exception(f'Test Scenario {scenario_name} index is not found')

    @logger_time_stamp
    def parse_oadp_cr(self, ns, cr_type, cr_name):
        """
        this method parse CR for
        CR status, kind, itemsBackedUp, itemsRestored, totalItems, Cloud, startTimestamp, completionTimestamp, dyration
        """
        # verify CR exists
        oadp_cr_already_present = self.is_oadp_cr_present(ns=ns, cr_type=cr_type, cr_name=cr_name)
        if not oadp_cr_already_present:
            # todo Throw exception and fail test
            print(f"Warning no matching cr {cr_name} of type: {cr_type} was found")
        else:
            # todo Avoid Multi OC cmds use single call by working against json output directly
            cr_info = {}
            jsonpath_cr_status = "'{.status.phase}'"
            cr_status = self.__ssh.run(
                cmd=f"oc get {cr_type}/{cr_name} -n {ns} -o jsonpath={jsonpath_cr_status}")
            if cr_status != '':
                cr_info['cr_status'] = cr_status

            jsonpath_cr_kind = "'{.kind}'"
            cr_kind = self.__ssh.run(
                cmd=f"oc get {cr_type}/{cr_name} -n {ns} -o jsonpath={jsonpath_cr_kind}")
            if cr_kind != '':
                cr_info['cr_kind'] = cr_kind

            if cr_type == 'backup':
                jsonpath_cr_items_backedup = "'{.status.progress.itemsBackedUp}'"
                cr_items_backedup = self.__ssh.run(
                    cmd=f"oc get {cr_type}/{cr_name} -n {ns} -o jsonpath={jsonpath_cr_items_backedup}")
                if cr_items_backedup != '':
                    cr_info['cr_items_backedup'] = cr_items_backedup

            if cr_type == 'restore':
                jsonpath_cr_items_restored = "'{.status.progress.itemsRestored}'"
                cr_items_restored = self.__ssh.run(
                    cmd=f"oc get {cr_type}/{cr_name} -n {ns} -o jsonpath={jsonpath_cr_items_restored}")
                if cr_items_restored != '':
                    cr_info['cr_items_restored'] = cr_items_restored

            jsonpath_cr_items_total = "'{.status.progress.totalItems}'"
            cr_items_total = self.__ssh.run(
                cmd=f"oc get {cr_type}/{cr_name} -n {ns} -o jsonpath={jsonpath_cr_items_total}")
            if cr_items_total != '':
                cr_info['cr_items_total'] = cr_items_total

            jsonpath_cr_errors = "'{.status.errors}'"
            cr_errors = self.__ssh.run(
                cmd=f"oc get {cr_type}/{cr_name} -n {ns} -o jsonpath={jsonpath_cr_items_total}")
            if cr_items_total != '':
                cr_info['cr_errors'] = cr_errors

            jsonpath_cr_start_timestamp = "'{.status.startTimestamp}'"
            cr_start_timestamp = self.__ssh.run(
                cmd=f"oc get {cr_type}/{cr_name} -n {ns} -o jsonpath={jsonpath_cr_start_timestamp}")
            if cr_start_timestamp != '':
                cr_info['cr_start_timestamp'] = cr_start_timestamp

            jsonpath_cr_completion_timestamp = "'{.status.completionTimestamp}'"
            cr_completion_timestamp = self.__ssh.run(
                cmd=f"oc get {cr_type}/{cr_name} -n {ns} -o jsonpath={jsonpath_cr_completion_timestamp}")
            if cr_completion_timestamp != '':
                cr_info['cr_completion_timestamp'] = cr_completion_timestamp
                if cr_start_timestamp != '':
                    cr_info['total_duration'] = (parser.parse(cr_info['cr_completion_timestamp']) - parser.parse(
                        cr_info['cr_start_timestamp']))
            awk_cr_cluster = "'{print $2}'"
            cr_cluster = self.__ssh.run(
                cmd=f"oc get route/console -n openshift-console | grep -v NAME | awk {awk_cr_cluster}")
            if cr_cluster != '':
                cr_info['cr_cluster'] = cr_cluster

            print(f'cr_info is {cr_info}')
            self.__run_metadata['summary']['runtime']['results'] = {}
            self.__run_metadata['summary']['runtime']['results'].update(cr_info)
            self.__result_dicts.append(self.__run_metadata['summary']['runtime']['results'])

    @logger_time_stamp
    def set_run_status(self, msg={}):
        """
        method for setting status of run
        """
        if msg:
            self.__run_metadata['summary']['results'].update(msg)
        self.__run_metadata['status'] = self.__run_metadata['summary']['runtime']['results'].get('cr_status', 'error')

    @logger_time_stamp
    def get_resources_per_pod(self, podname, namespace, label=''):
        """
        method returns oc adm top pods value
        """
        cmd_adm_top_pod_output = self.__ssh.run(cmd=f"oc adm top pod {podname} -n {namespace} --no-headers=true")
        if len(cmd_adm_top_pod_output.split(' ')) == 0:
            print(f'resulting query for {podname} resources failed 0 lines returned')
            # todo add failure logic for oc adm top pod query in get_resources_per_pod()
        else:
            response = list(filter(None, cmd_adm_top_pod_output.split(' ')))
            get_pod_json_output = self.__ssh.run(cmd=f"oc get pod {response[0]} -n {namespace} -o json")
            # Parse the JSON string into a Python object
            data = json.loads(get_pod_json_output)
            # Get resources req & limits of specific pod
            pod_resources = data['spec']['containers'][0]['resources']
            pod_details = [
                {'name': response[0], 'cores': response[1], 'mem': response[2],
                 'resources': pod_resources, 'label': label }]
            self.__run_metadata['summary']['resources']['run_time_pods'].append(pod_details)

    @logger_time_stamp
    def calc_resource_diff(self, old_value, new_value):
        """
        method calculates % of difference between resources samples
        """
        old_value = int(re.sub("[^0-9]", "", old_value))
        new_value = int(re.sub("[^0-9]", "", new_value))
        if old_value == 0:
            # avoid division by zero
            return 0
        diff = (new_value - old_value) / old_value * 100
        return diff

    @logger_time_stamp
    def calc_pod_basename(self, pod_name):
        """
        method gets pod base name
        """
        base_names = {}
        pod_parts = pod_name.split("-")
        base_name = ""
        for part in pod_parts:
            if any(map(str.isdigit, part)):
                fullname = base_name[:-1]
                return fullname
            else:
                # Handle non-digit based pod names eg: node-agent-vzbgg note how vzbgg doesnt contain digit
                if pod_parts[-1] != part:
                    base_name += part + "-"
                else:
                    fullname = base_name[:-1]
                    return fullname
        # if no return has already happened then podname doesnt contain digit
        # so we parse on - and assume last part of name is unique char string that we dont care about
        # csi-cephfsplugin-provisioner-asdf ==> return csi-cephfsplugin-provisioner
        fullname = base_name[:-2]
        return fullname


    @logger_time_stamp
    def get_resources_per_ns(self, namespace, label=''):
        """
        method returns oc adm top pods values for ns, when label is set to 'end' diffs of values are stored
        additional % of cpu/mem limit, pod uptime stored when limit is set to 'end'
        """
        cmd_adm_top_ns_output = self.__ssh.run(cmd=f"oc adm top pods -n {namespace} --no-headers=true")
        if len(cmd_adm_top_ns_output.splitlines()) == 0:
            logger.warn(f'resulting query for get_resources_per_ns {namespace} resources failed 0 lines returned')
        else:
            cmd_adm_top_ns_output_list = list(filter(None, cmd_adm_top_ns_output.split('\n')))
            if cmd_adm_top_ns_output_list is not None:
                for val in cmd_adm_top_ns_output_list:
                    adm_stdout_response = (list(filter(None, val.split(' '))))
                    if label == 'end':
                        pod_index = self.find_metadata_index_for_pods(target=adm_stdout_response[0])
                        if pod_index is not None:
                            # pod_details = [{'cores': adm_stdout_response[1], 'mem': adm_stdout_response[2],'label': label}]
                            # Diff of memory between pod samples
                            original_sample = self.__run_metadata['summary']['resources']['run_time_pods'][pod_index][0]['mem']
                            latest_sample =  adm_stdout_response[2]
                            diff_mem = self.calc_resource_diff(original_sample, latest_sample)
                            # Diff of milicores between samples
                            original_sample = self.__run_metadata['summary']['resources']['run_time_pods'][pod_index][0]['cores']
                            latest_sample = adm_stdout_response[1]
                            diff_core = self.calc_resource_diff(original_sample, latest_sample)
                            # Get pod base name per pod run time name
                            pod_name_by_role = self.__oadp_runtime_resource_mapping[f'{adm_stdout_response[0]}']
                            # Persist latest changes to hash indexed per pod run time name
                            pod_details = {f'{label}_cores': adm_stdout_response[1], f'{label}_mem': adm_stdout_response[2], 'diff_core_percent': f'{diff_core:.1f}', 'diff_mem_percent': f'{diff_mem:.1f}', 'label': label}
                            original_dict = self.__run_metadata['summary']['resources']['run_time_pods'][pod_index][0]
                            self.__run_metadata['summary']['resources']['run_time_pods'][pod_index][0] = {**original_dict, **pod_details}
                            # Set run time data to dict indexed by pod_role_name to allow for easy querying post run
                            self.__run_metadata['summary']['resources']['pods'][pod_name_by_role] = self.__run_metadata['summary']['resources']['run_time_pods'][pod_index][0]
                            del self.__run_metadata['summary']['resources']['run_time_pods'][pod_index]
                    else:
                        # Initalize key:value runtime pod name to base pod name for updating upon result collection
                        self.initialize_pod_resources_by_base_name(pod_name=adm_stdout_response[0])
                        get_pod_json_output = self.__ssh.run(cmd=f"oc get pod {adm_stdout_response[0]} -n {namespace} -o json")
                        # Parse the JSON string into a Python object
                        data = json.loads(get_pod_json_output)
                        # Get resources req & limits of specific pod
                        pod_resources = data['spec']['containers'][0]['resources']
                        pod_details = [{'name': adm_stdout_response[0], 'cores': adm_stdout_response[1], 'mem': adm_stdout_response[2], 'resources': pod_resources, 'label': label}]
                        self.__run_metadata['summary']['resources']['run_time_pods'].append(pod_details)
                if label == 'end':
                    # updates existing dict of utlized limits for cpu/mem and uptime
                    self.get_percentage_of_limit_utilized(ns=namespace)



    @logger_time_stamp
    def find_metadata_index_for_pods(self, target):
        """
        searches through list of dictionaries to return the right index
        self.__run_metadata['summary']['resources']['run_time_pods']
        """
        for index in range(len(self.__run_metadata['summary']['resources']['run_time_pods'])):
            print (f"{self.__run_metadata['summary']['resources']['run_time_pods'][index][0]['name']}  == {target}")
            if self.__run_metadata['summary']['resources']['run_time_pods'][index][0]['name'] == target:
                return index

    @logger_time_stamp
    def get_list_of_pods_by_status(self, namespace, query_operator,  status):
        """
        This method returns list of pods in namespace
        :return:
        """
        pods = self.__ssh.run(
            cmd=f'oc get pods -n {namespace} --field-selector status.phase{query_operator}{status} --no-headers -o custom-columns=":metadata.name"')
        if pods != '':
            list_of_pods = pods.split('\n')
            return list_of_pods
        else:
            return []

    def get_oc_resource_to_json(self, resource_type, resource_name, namespace):
        """
        method returns oc resource info
        """
        resources_returned = self.__ssh.run(cmd=f'oc get {resource_type} {resource_name} -n {namespace} -o json')
        if 'not found' in resources_returned:
            logger.info(f"::info:: oc get {resource_type} {resource_name} -n {namespace} => Not Found")
            return {}
        else:
            return json.loads(resources_returned)

    @logger_time_stamp
    def get_list_of_pods(self, namespace):
        """
        This method returns list of pods in namespace
        :return:
        """
        running_pods = self.__ssh.run(
            cmd=f'oc get pods -n {namespace} --field-selector status.phase=Running --no-headers -o custom-columns=":metadata.name"')
        if running_pods != '':
            list_of_running_pods = running_pods.split('\n')
            return list_of_running_pods
        else:
            return False

    @logger_time_stamp
    def get_percentage_of_limit_utilized(self, ns):
        """
        method collects cpu/mem limits and pod uptime
        values are appened to existing dict  self.__run_metadata['summary']['resources']['pods'][pod_name_by_role]
        """
        get_node_names = self.__ssh.run(cmd=f'oc describe node | grep -w "{ns}"')
        adm_result = {}
        for line in get_node_names.splitlines():
            # necessary to check if line contains the substring needed to parse
            matches_expected_line_ouput = re.findall(f'{ns}    ', line)
            if len(matches_expected_line_ouput) > 0:
                x = line.split(' ')
                x = list(filter(None, x))
                logger.info(f'get_percentage_of_limit_utilized  working with x: {x} whose length is {len(x)}')
                pod_name_by_role = self.__oadp_runtime_resource_mapping[f'{x[1]}']
                original_dict = self.__run_metadata['summary']['resources']['pods'][pod_name_by_role]
                # x[5] cpulimit_percentage x[9] memlimit_percentage x[10] pod_uptime
                # extract raw digit value from eg: (3%) => 3
                cpu_limit_value = re.findall(r'\d+', x[5])
                mem_limit_value = re.findall(r'\d+', x[9])
                updated_pod_details = {'ns': x[0], 'cpu_limit_percentage': cpu_limit_value, 'mem_limit_percentage': mem_limit_value, 'pod_uptime': x[10] }
                original_dict =  self.__run_metadata['summary']['resources']['pods'][pod_name_by_role]
                self.__run_metadata['summary']['resources']['pods'][pod_name_by_role] = {**original_dict, **updated_pod_details}

    @logger_time_stamp
    def get_node_generic_name(self, host):
        """
        method returns base name of host
        eg: worker000-r640 ==> worker000 or master-0 => master-0
        """
        if 'worker' in host and host.count('-') == 1:
            #'worker000-r640' or 'worker-000-r650
            updated_host = host.split('-')[0]
            logger.info(f"get_node_generic_name converting {host} to {updated_host}")
            return updated_host
        else:
            # master-0
            logger.info(f"get_node_generic_name no need to convert host: {host} as its not a worker")
            return host


    @logger_time_stamp
    def get_node_resource_avail_adm(self, ocp_node):
        """
        method returns CPU(cores), CPU%, Mem, Mem%
        """
        cmd_output = self.__ssh.run(cmd=f'oc adm top node {ocp_node} --no-headers')
        if cmd_output != '':
            node_adm_result = (list(filter(None, cmd_output.split(' '))))
            worker_name = self.get_node_generic_name(node_adm_result[0])
            # worker_name = node_adm_result[0]
            if worker_name not in self.__run_metadata['summary']['resources']['nodes'].keys():
                self.__run_metadata['summary']['resources']['nodes'][worker_name] = {'name': node_adm_result[0], 'cores': node_adm_result[1], 'cpu_per': node_adm_result[2],
                             'mem_bytes': node_adm_result[3], 'mem_per': node_adm_result[4], "label": '' }
            else:
                # worker_name exists and need compare func for values
                self.calc_node_mem_and_cpu_resources_diff(node_adm_result)

    @logger_time_stamp
    def calc_node_mem_and_cpu_resources_diff(self, adm_node_info):
        """
        method calculates diff between node resources via adm and updates dict with results
        """
        node = self.get_node_generic_name(adm_node_info[0])
        # node = adm_node_info[0]
        prev_core_total = self.__run_metadata['summary']['resources']['nodes'][node]['cpu_per']
        prev_core_total = prev_core_total.replace('%', '')
        current_core = adm_node_info[2].replace('%', '')
        diff_core = int(current_core) - int(prev_core_total)
        self.__run_metadata['summary']['resources']['nodes'][node]['core_diff'] = f'{diff_core}%'

        prev_mem_total = self.__run_metadata['summary']['resources']['nodes'][node]['mem_per']
        prev_mem_total = prev_core_total.replace('%', '')
        current_mem = adm_node_info[4].replace('%', '')
        diff_mem = int(current_mem) - int(prev_mem_total)
        self.__run_metadata['summary']['resources']['nodes'][node]['mem_diff'] = f'{diff_mem}%'

    @logger_time_stamp
    def collect_all_node_resource(self):
        """
        method collects all node avail resources via get_node_resource_avail_adm(self, ocp_node):
        """
        get_node_names = self.__ssh.run(cmd=f'oc get nodes --no-headers -o custom-columns=":metadata.name"')
        if len(get_node_names.splitlines()) > 0 and 'error' not in get_node_names:
            for bm in get_node_names.splitlines():
                self.get_node_resource_avail_adm(ocp_node=bm)

    @logger_time_stamp
    def get_oadp_velero_and_cr_log(self, cr_name, cr_type):
        """
        method saves velero log to self.__artifactdir
        save CR, velero logs
        """
        oadp_cr_log = os.path.join(self._run_artifacts_path, 'oadp-cr.json')
        oadp_velero_log = os.path.join(self._run_artifacts_path, 'oadp-velero.log')
        self.__ssh.run(cmd=f'oc -n openshift-adp exec deployment/velero -c velero -it -- ./velero {cr_type} logs {cr_name} --insecure-skip-tls-verify >> {oadp_velero_log}')
        if os.path.exists(os.path.join(oadp_velero_log)) or os.stat(oadp_velero_log).st_size == 0:
            #todo warn file artifact creation had an issue
            logger.warn(f'oadp_velero_log is either not present or empty check file path: {oadp_velero_log}')
        self.__ssh.run(cmd=f"oc get {cr_type} {cr_name} -n openshift-adp -o json >> {oadp_cr_log}")

        if os.path.exists(os.path.join(oadp_cr_log)) or os.stat(oadp_cr_log).st_size == 0:
            #todo warn file artifact creation had an issue
            logger.warn(f'oadp_cr_log is either not present or empty check file path: {oadp_cr_log}')

    @logger_time_stamp
    def oadp_execute_scenario(self, test_scenario, run_method):
        """
        method executes workload
        """
        if run_method == 'ansible':
            print("invoking via ansible")
            ansible_args = f"test=1 testcase={test_scenario['testcase']} plugin={test_scenario['args']['plugin']} use_cli={test_scenario['args']['use_cli']} OADP_CR_TYPE={test_scenario['args']['OADP_CR_TYPE']} OADP_CR_NAME={test_scenario['args']['OADP_CR_NAME']} backup_name={test_scenario['args']['backup_name']} namespaces_to_backup={test_scenario['args']['namespaces_to_backup']} result_dir_base_path={test_scenario['result_dir_base_path']}"
            self.__ssh.run(cmd=f'ansible-playbook {self.__oadp_base_dir}/test-oadp.yaml -e "{ansible_args}" -vv')

        if run_method == 'python':
            if test_scenario['args']['OADP_CR_TYPE'] == 'backup':
                self.oadp_timer(action="start", transaction_name=f"{test_scenario['args']['OADP_CR_NAME']}")
                self.oadp_create_backup(plugin=test_scenario['args']['plugin'], backup_name=test_scenario['args']['backup_name'], namespaces_to_backup=test_scenario['args']['namespaces_to_backup'])
                self.wait_for_condition_of_oadp_cr(cr_type=test_scenario['args']['OADP_CR_TYPE'],
                                                   cr_name=test_scenario['args']['OADP_CR_NAME'],
                                                   testcase_timeout=test_scenario['args']['testcase_timeout'])
                self.oadp_timer(action="stop", transaction_name=f"{test_scenario['args']['OADP_CR_NAME']}")
            if test_scenario['args']['OADP_CR_TYPE'] == 'restore':
                self.oadp_timer(action="start", transaction_name=f"{test_scenario['args']['OADP_CR_NAME']}")
                self.oadp_restore(plugin=test_scenario['args']['plugin'], restore_name=test_scenario['args']['OADP_CR_NAME'], backup_name=test_scenario['args']['backup_name'])
                self.wait_for_condition_of_oadp_cr(cr_type=test_scenario['args']['OADP_CR_TYPE'], cr_name=test_scenario['args']['OADP_CR_NAME'], testcase_timeout=test_scenario['args']['testcase_timeout'])
                self.oadp_timer(action="stop", transaction_name=f"{test_scenario['args']['OADP_CR_NAME']}")
                dataset_restored_as_expected = self.validate_dataset(test_scenario)
                self.__run_metadata['summary']['results']['dataset_post_run_validation'] = dataset_restored_as_expected
                if not dataset_restored_as_expected:
                    logger.error(f"Restored Dataset for {test_scenario['args']['OADP_CR_NAME']} did not pass post run validations on {test_scenario['args']['namespaces_to_backup']}")
                else:
                    logger.info('Restore passed post run validations')


    @logger_time_stamp
    def load_test_scenario(self):
        """
        method parses yaml which contains test scenario details
        """
        # parse test yaml to find desired scenario
        index = self.find_test_scenario_index(scenario_name=self.__oadp_scenario_name)

        # Read yaml which contains test scenario details
        # Load data from oadp-helpers/templates/internal_data/tests.yaml
        test_data = yaml.safe_load(Path(self.__oadp_scenario_data).read_text())
        return (test_data['scenarios'][index])

    @logger_time_stamp
    def validate_dataset(self, scenario):
        """
        method verifies dataset pod, pv size, sc, and pv contents (utilization of mount, file & folder counts where relevant.)
        returns boolean representing state of dataset present vs whats expected.
        """
        num_of_pods_expected = scenario['dataset']['pods_per_ns']
        target_namespace = scenario['args']['namespaces_to_backup']
        expected_size = scenario['dataset']['pv_size']
        expected_sc = scenario['dataset']['sc']
        role = scenario['dataset']['role']
        skip_dataset_validation = scenario['args'].get('skip_source_dataset_check', False)

        # all datasets are checked for if the pods are in the correct state with relevant pv in correct sc and pv size
        # datasets with role  generator require addition check of pv utilization in regards to utilized size, folders, and

        pod_presence_and_storage_as_expected = self.verify_pod_presence_and_storage(num_of_pods_expected, target_namespace, expected_sc, expected_size, skip_dataset_validation)
        if not pod_presence_and_storage_as_expected:
            logger.warn(f'validate_dataset returning false for pod_presence_and_storage_as_expected: value is: {pod_presence_and_storage_as_expected}')
            return False
        elif role == 'generator':
            # get list of running pods
            pods_to_validate =  self.get_list_of_pods(namespace=target_namespace)
            if pods_to_validate == 0:
                logger.error(f"No running pods were found in ns {target_namespace} expected {num_of_pods_expected}")
            else:
                for pod in pods_to_validate:
                    pv_util_details_returned_by_pod = self.get_pod_pv_utilization_info_by_podname(scenario, pod)
                    pv_contents_as_expected = self.pv_contains_expected_data(scenario, pv_util_details_returned_by_pod)
                    if not pv_contents_as_expected:
                        logger.warn(f'::: PV UTIL Contents check FAILURE:::  pv_contents_as_expected: value is: {pv_contents_as_expected} for pod: {pod} in ns {target_namespace} pod returned: {pv_util_details_returned_by_pod}')
                        return False
                    else:
                        logger.info(f'::: PV UTIL Contents check successful for pod: {pod} in ns {target_namespace}')
        return True

    @logger_time_stamp
    def initialize_pod_resources_by_base_name(self, pod_name):
        """
        method takes pod name and updates dict
        """
        base_pod_name = self.calc_pod_basename(pod_name)
        if base_pod_name not in self.__oadp_resources.keys():
            self.__oadp_resources[base_pod_name] = {}
            # self.__oadp_runtime_resource_mapping[pod_name] = f"{base_pod_name}-0"
            self.__oadp_runtime_resource_mapping[pod_name] = f"{base_pod_name}"
            # self.__oadp_resources[base_name] =  {"base_name": base_pod_name}
            print(f'key {base_pod_name} not in {self.__oadp_resources.keys} ')
        else:
            if base_pod_name in self.__oadp_resources.keys():
                count = 1
                for key, value in self.__oadp_resources.items():
                    short_name = key.rsplit('-', 1)
                    short_name = short_name[0]
                    if base_pod_name.lower() == short_name.lower():
                    # if base_pod_name.lower() in key.lower():
                        count += 1
                        print (f" base_pod_name: {base_pod_name} key: {key} value: {value}  and count value is : {count}" )
                        print ("here")
                self.__oadp_resources[f"{base_pod_name}-{count - 1}"] = {}
                self.__oadp_runtime_resource_mapping[pod_name] = f"{base_pod_name}-{count - 1}"

    @logger_time_stamp
    @prometheus_metrics(yaml_full_path='/tmp/mpqe-scale-scripts/oadp-helpers/templates/metrics/metrics-oadp.yaml')
    def run_workload(self):
        """
        This method run oadp workload
        :return:
        """
        # Load Scenario Details
        test_scenario = self.load_test_scenario()

        # Get OADP, Velero, Storage Details
        self.oadp_get_version_info()
        self.get_velero_details()
        self.get_storage_details()

        # Save test scenario run time settings run_metadata dict
        self.__run_metadata['summary']['runtime'].update(test_scenario)
        self.__result_dicts.append(test_scenario)
        self.generate_elastic_index(test_scenario)

        namespace = test_scenario['args']['namespaces_to_backup']
        target_namespace = test_scenario['args']['namespaces_to_backup']

        # Verify desired storage is default storage class and volumesnapshotclass
        expected_sc = test_scenario['dataset']['sc']
        expected_size = test_scenario['dataset']['pv_size']

        # Setup Default SC and Volume Snapshot Class
        self.set_default_storage_class(expected_sc)
        self.set_volume_snapshot_class(expected_sc)

        # Setup Datamover if needed
        if test_scenario['args']['plugin'] == 'vsm':
            self.enable_datamover(oadp_namespace='openshift-adp', scenario=test_scenario)
            self.config_datamover(oadp_namespace='openshift-adp', scenario=test_scenario)
            if expected_sc == 'ocs-storagecluster-cephfs-shallow':
                self.config_dpa_for_cephfs_shallow(enable=True,oadp_namespace='openshift-adp')
            else:
                self.config_dpa_for_cephfs_shallow(enable=False,oadp_namespace='openshift-adp')
        else:
            # disable datamover / verify cephfs-shallow isnt set in dpa
            self.config_dpa_for_cephfs_shallow(enable=False,oadp_namespace='openshift-adp')
            self.disable_datamover(oadp_namespace='openshift-adp')

        # when performing backup
        # Check if source namespace aka our dataset is preseent, if dataset not prsent then create it
        if test_scenario['args']['OADP_CR_TYPE'] == 'backup':
            dataset_already_present = self.validate_dataset(test_scenario)
            if not dataset_already_present:
                self.create_oadp_source_dataset(test_scenario)

        # when performing restore
        # source dataset will be removed before restore attempt unless dataset yaml contains ['args']['existingResourcePolicy'] set to 'Update'
        if test_scenario['args']['OADP_CR_TYPE'] == 'restore':
            remove_source_dataset = test_scenario['args'].get('existingResourcePolicy', False)
            if remove_source_dataset != 'Update':
                self.delete_oadp_source_dataset(target_namespace=test_scenario['args']['namespaces_to_backup'])
            elif remove_source_dataset == 'Update':
                print ('WIP: logic for existingResourcePolicy OADP-1184 wil go here')
                # todo Add logic for existingResourcePolicy OADP-1184 which requires existingResourcePolicy: Update to be set in Restore CR

        # Check if OADP CR name is present if so remove it
        oadp_cr_already_present = self.is_oadp_cr_present(ns='openshift-adp',
                                                          cr_type=test_scenario['args']['OADP_CR_TYPE'],
                                                          cr_name=test_scenario['args']['OADP_CR_NAME'])
        if oadp_cr_already_present:
            logger.warn(f"You are attempting to use CR name: {test_scenario['args']['OADP_CR_NAME']} which is already present so it will be deleted")
            self.delete_oadp_custom_resources(cr_type=test_scenario['args']['OADP_CR_TYPE'], ns='openshift-adp',
                                              cr_name=test_scenario['args']['OADP_CR_NAME'])

        # Get Node and Pod Resource prior to test
        self.collect_all_node_resource()
        self.get_resources_per_ns(namespace='openshift-adp', label="start")

        # Launch OADP scenario
        self.oadp_execute_scenario(test_scenario, run_method='python')

        # Get Pod Resource after the test
        self.collect_all_node_resource()
        self.get_resources_per_ns(namespace='openshift-adp', label="end")

        # Parse result CR for status, and timestamps
        self.parse_oadp_cr(ns='openshift-adp', cr_type=test_scenario['args']['OADP_CR_TYPE'],
                           cr_name=test_scenario['args']['OADP_CR_NAME'])
        self.get_oadp_velero_and_cr_log(cr_name=test_scenario['args']['OADP_CR_NAME'],
                                        cr_type=test_scenario['args']['OADP_CR_TYPE'])
        self.get_logs_by_pod_ns(namespace='openshift-adp')

        # Get OCP node info
        self.collect_all_node_resource()

        # Post Run Validations
        #    check for pod restarts / cluster operator status
        self.verify_pod_restarts('openshift-adp')
        self.verify_cluster_operators_status()

        # Set Run Status
        self.set_run_status()
        self.create_json_summary()

        #Post Run Cleanup
        self.cleaning_up_oadp_resources(test_scenario)


        if os.path.exists(os.path.join(self.__result_report)) and not os.stat(self.__result_report).st_size == 0:
            self.__ssh.run(cmd=f'cp {self.__result_report} {self._run_artifacts_path}')
            return True
        else:
            result_report_json_data = {}
            result_report_json_data['result'] = 'Failed'
            result_report_json_data['run_artifacts_url'] = os.path.join(self._run_artifacts_url,
                                                                        f'{self._get_run_artifacts_hierarchy(workload_name=self._workload, is_file=True)}-{self._time_stamp_format}.tar.gz')
            # if self._run_type == 'test_ci':
            #     index = f'oadp-metadata-test-ci-results'
            # elif self._run_type == 'release':
            #     index = f'oadp-metadata-release-results'
            # else:
            #     index = f'oadp-metadata-results'
            index = self.generate_elastic_index(test_scenario)
            logger.info(f'upload index: {index}')
            self._es_operations.upload_to_elasticsearch(index=index, data=result_report_json_data)
            raise MissingResultReport()

    @logger_time_stamp
    def cleaning_up_oadp_resources(self, scenario):
        """
        method removes oapd CR or dataset if set via CLI option
        """
        if self.__oadp_cleanup_cr_post_run:
            if 'restore' == scenario['testtype']:
                logger.info(f"*** Attempting post run: clean up for {scenario['args']['OADP_CR_NAME']} that is a {scenario['testtype']} relevant CRs to remove are: restore: {scenario['args']['OADP_CR_NAME']} & relevant CRs related backup CR: {scenario['args']['backup_name']} ")
                self.delete_oadp_custom_resources( 'openshift-adp', cr_type=scenario['args']['OADP_CR_TYPE'], cr_name=scenario['args']['OADP_CR_NAME'])
                self.delete_oadp_custom_resources('openshift-adp', cr_type='backup', cr_name=scenario['args']['backup_name'])
            if 'backup' == scenario['testtype']:
                logger.info(f"*** Attempting post run: clean up for {scenario['args']['OADP_CR_NAME']} that is a {scenario['testtype']} relevant CRs to remove are: {scenario['args']['OADP_CR_NAME']}")
                self.delete_oadp_custom_resources('openshift-adp',cr_type=scenario['args']['OADP_CR_TYPE'],cr_name=scenario['args']['OADP_CR_NAME'])
        else:
            logger.info(f'*** Skipping post run cleaning up of OADP CR *** as self.__oadp_cleanup_cr_post_run: {self.__oadp_cleanup_cr_post_run}')
        if self.__oadp_cleanup_dataset_post_run:
            logger.info(f'*** Attempting post run: clean up of OADP dataset *** as self.__oadp_cleanup_dataset_post_run: {self.__oadp_cleanup_dataset_post_run}')
            self.delete_oadp_source_dataset(target_namespace=scenario['args']['namespaces_to_backup'])
        else:
            logger.info(f'*** Skipping post run cleaning up of OADP dataset  *** as self.__oadp_cleanup_dataset_post_run: {self.__oadp_cleanup_dataset_post_run}')


    @logger_time_stamp
    def generate_elastic_index(self, scenario):
        '''
        method creates elastic index name based on test_scenario data
        '''
        if scenario['dataset']['total_namespaces'] == 1:
            index_name = 'oadp-' + scenario['testtype'] + '-' + 'single-namespace'
        elif scenario['dataset']['total_namespaces'] > 1:
                index_name = 'oadp-' + scenario['testtype'] + '-' + 'multi-namespace'
        logger.info(f'index_name {index_name}')
        self.__run_metadata['index'] = index_name
        return index_name


    @logger_time_stamp
    def run(self):
        """
        This method run oadp workloads
        :return:
        """
        try:
            # initialize workload
            self.initialize_workload()
            # Run workload
            if self.run_workload():
                # finalize workload
                self.finalize_workload()
        # when error raised finalize workload
        except Exception:
            logger.info(f'{self._workload} workload raised an exception')
            # finalize workload
            self.finalize_workload()
            return False

        return True

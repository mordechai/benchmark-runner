import os
import json
import time

import yaml
import re
from pathlib import Path

from benchmark_runner.common.ssh.ssh import SSH
from benchmark_runner.clusterbuster.clusterbuster_exceptions import MissingResultReport, MissingElasticSearch
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
        self.__oadp_scenario_data = '/tmp/mpqe-scale-scripts/oadp-helpers/templates/internal_data/pvc_utlization.yaml'
        self.__oadp_promql_queries = '/tmp/mpqe-scale-scripts/oadp-helpers/templates/metrics/metrics-oadp.yaml'
        # environment variables
        self.__namespace = self._environment_variables_dict.get('namespace', '')
        self.__oadp_workload = self._environment_variables_dict.get('oadp', '')
        self.__oadp_uuid = self._environment_variables_dict.get('oadp_uuid', '')
        self.__oadp_scenario_name = 'backup-restic-pvc_utlization-2-2-1-rbd'
        self.__result_report = '/tmp/oadp-report.json'
        self.__artifactdir = os.path.join(self._run_artifacts_path, 'oadp-ci')
        self._run_artifacts_path = self._environment_variables_dict.get('run_artifacts_path', '')
        self.__oadp_log = os.path.join(self._run_artifacts_path, 'oadp.log')
        self.__ssh = SSH()
        self.__oadp_resources = {}
        self.__oadp_runtime_resource_mapping = {}
        self.__result_dicts = []
        self.__run_metadata = {
            "index": '',
            "metadata": {},
            "summary": {
                "env": {
                    "ocp": {},
                    "storage": {}
                },
                "runtime": {},
                "results": {},
                "resources": {
                    "nodes": [],
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
        # self._es_operations.verify_elasticsearch_data_uploaded(index=index, uuid=result_report_json_data['metadata']['uuid'])

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
                logger.info(f"current pv storage class used: {current_sc} doesnt match expected storage class of: {expected_sc}")
                return False
            if current_size != expected_size:
                logger.info(f"current pv size: {current_size} doesnt match expected pv size: {expected_size}")
                return False
            logger.info(f"pod: {podName} in ns: {expected_ns} matches desired storage and pv size")
            return True


    @logger_time_stamp
    def verify_running_pods(self, num_of_pods_expected, target_namespace):
        """
        This method verifies number of pods in namespace are in running state
        :return:
        """
        running_pods = self.__ssh.run(
            cmd=f'oc get pods -n {target_namespace} --field-selector status.phase=Running --no-headers -o custom-columns=":metadata.name"')
        if running_pods != '':
            list_of_running_pods = running_pods.split('\n')
            print('running_pods detected in {namespace} are: {running_pods} expected {num_of_pods_expected}')
            if len(list_of_running_pods) == num_of_pods_expected:
                print("expected dataset is present")
                return True
            else:
                return False
        else:
            print('expected dataset NOT present returning false')
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
    def verify_pod_presence_and_storage(self, num_of_pods_expected, target_namespace, expected_sc, expected_size, skip_dataset_validation):
        '''
        checks pod presence via func verify_running_pods
        checks pod's pv storage class used, and pv size via func verify_pod_pv_size_and_sc
        '''
        if skip_dataset_validation == True:
            logger.warn('You are skipping dataset validations - verify_pod_presence_and_storage will return True with out checks')
            return True
        check_num_of_pods_and_state = self.verify_running_pods(num_of_pods_expected,target_namespace)
        if check_num_of_pods_and_state == False:
            logger.info(f"Dataset not as expected - did not find {num_of_pods_expected} of pods in namespace {target_namespace}")
            return False
        list_of_pods = self.get_list_of_pods(namespace=target_namespace)
        for p in list_of_pods:
            pod_storage_status = self.verify_pod_pv_size_and_sc(podName=p, expected_ns=target_namespace, expected_sc=expected_sc, expected_size=expected_size)
            if pod_storage_status == False:
                return False
        return True


    @logger_time_stamp
    def create_oadp_source_dataset(self, num_of_assets_desired, target_namespace, pv_size, storage):
        """
        This method creates dataset for oadp to work against
        :return:
        """
        # check whether current NS already exists
        check_ns_presence = self.__ssh.run(cmd=f'oc get ns {target_namespace}')
        if check_ns_presence.find('not found') < 0:
            # delete ns with same name
            logger.warn(f"NS {target_namespace} with same name already exists and will be remoed")
            check_ns_presence = self.__ssh.run(cmd=f'oc delete ns {target_namespace}')
            if check_ns_presence.find('deleted') < 0:
                logger.exception("Unable to remove NS {ns} when attempting to clean up before populating NS likely same ns exists on different storage")
        self.oadp_timer(action="start", transaction_name='dataset_creation')
        print(f'BusyBoxPodSingleNS.sh {num_of_assets_desired} {pv_size} {storage}')
        self.__ssh.run(cmd=f'{self.__oadp_path}/BusyBoxPodSingleNS.sh {num_of_assets_desired} {pv_size} {storage} > /tmp/dataset-creation.log')
        for i in range(1, num_of_assets_desired + 1):
            print(f'checking for {target_namespace}-{i} in namespace= {target_namespace}')
            self._oc.wait_for_pod_ready(pod_name=f'{target_namespace}-{i}', namespace=target_namespace)
        self.oadp_timer(action="stop", transaction_name='dataset_creation')

    @logger_time_stamp
    def create_pvutil_dataset(self, test_scenario):
        active_role = test_scenario['dataset']['role']
        playbook_path = test_scenario['dataset']['playbook_path']
        pvc_size = test_scenario['dataset']['pvc_size']
        dataset_path = test_scenario['dataset']['dataset_path']
        if active_role == 'generator':
            dir_count = test_scenario['dataset']['dir_count']
            files_count = test_scenario['dataset']['files_count']
            file_size = test_scenario['dataset']['files_size']
            dept_count = test_scenario['dataset']['dept_count']
            playbook_extra_var = (f"dir_count={dir_count}  files_count={files_count}  files_size={file_size}  dept_count={dept_count}  pvc_size={pvc_size}  dataset_path={dataset_path}")
            create_data_py = self.__ssh.run(cmd=f"ansible-playbook {playbook_path}  --extra-vars  '{playbook_extra_var}' -vvv")
            logger.info(create_data_py)
        elif active_role == 'dd_generator':
            bs = test_scenario['dataset']['bs']
            count = test_scenario['dataset']['count']
            playbook_extra_var = (f"bs={bs} count={count}  pvc_size={pvc_size}  dataset_path={dataset_path}")
            create_data_dd = self.__ssh.run(cmd=f"ansible-playbook {playbook_path}  --extra-vars  '{playbook_extra_var}' -vvv")
            logger.info(create_data_dd)
        else:
            logger.info("role doesnt define")
    @logger_time_stamp
    def get_capacity_usage(self, test_scenario):
        results_capacity_usage = {}
        active_role = test_scenario['dataset']['role']
        mount_point = test_scenario['dataset']['dataset_path']
        namespace = test_scenario['args']['namespaces_to_backup']
        podname = self.__ssh.run(cmd=f"oc get pods -o custom-columns=POD:.metadata.name --no-headers -n{namespace}")
        disk_capacity = self.__ssh.run(cmd=f"oc  exec -it -n{namespace} {podname} -- /bin/bash -c \"du -sh {mount_point}\"")
        current_disk_capacity = disk_capacity.split('\n')[-1].split('\t')[0]
        results_capacity_usage['disk_capacity'] = current_disk_capacity
        files_count = self.__ssh.run(cmd=f"oc  exec -it -n{namespace} {podname} -- /bin/bash -c \"find {mount_point}* -type f -name \"my-random-file-*\" -o -name \"dd_file\" |wc -l\"")
        current_files_count = files_count.split('\n')[-1].split('\t')[0]
        results_capacity_usage['files_count'] = current_files_count
        folders_count = self.__ssh.run(cmd=f"oc  exec -it -n{namespace} {podname} -- /bin/bash -c \"find {mount_point}* -type d  |wc -l\"")
        current_folders_count = folders_count.split('\n')[-1].split('\t')[0]
        results_capacity_usage['folders_count'] = current_folders_count
        logger.info(results_capacity_usage)

    def get_expected_files_count(self, test_scenario):
        results_capacity_expected = {}
        import math
        active_role = test_scenario['dataset']['role']
        dir_count = test_scenario['dataset']['dir_count'] # -n
        files_count = test_scenario['dataset']['files_count']
        dept_count = test_scenario['dataset']['dept_count'] # -d
        active_role = 'dd_generator'
        if active_role == 'generator':
            countdir = 0
            for k in range(dept_count):
                countdir += math.pow(dept_count, k)
            countfile = int(math.pow(dept_count, (dept_count - 1)) * files_count)
            total_files = int(countfile * dir_count)
            total_folders = int(countdir * dir_count)
            results_capacity_expected['expected_files'] = total_files
            results_capacity_expected['expected_folders'] = total_folders
            logger.info(total_folders, total_files)
            logger.info(results_capacity_expected)
        elif active_role == 'dd_generator':
             bs = test_scenario['dataset']['bs']
             count = test_scenario['dataset']['count']
             current_file_size = bs * count
    def capacity_usage_and_capacity_expected_comparison(self, results_capacity_expected, results_capacity_usage):


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
        restore_cmd = self.__ssh.run(
            cmd=f'oc -n openshift-adp exec deployment/velero -c velero -it -- ./velero restore create {restore_name} --from-backup {backup_name}')
        if restore_cmd.find('submitted successfully') < 0:
            print("Error restore was not successfully started")

    @logger_time_stamp
    def oadp_create_backup(self, plugin, backup_name, namespaces_to_backup):
        """
        this method is for testing oadp backup
        """
        if plugin == 'restic':
            backup_cmd = self.__ssh.run(
                cmd=f'oc -n openshift-adp exec deployment/velero -c velero -it -- ./velero backup create {backup_name} --include-namespaces {namespaces_to_backup} --default-volumes-to-fs-backup=true --snapshot-volumes=false')
        if plugin == 'csi':
            backup_cmd = self.__ssh.run(
                cmd=f'oc -n openshift-adp exec deployment/velero -c velero -it -- ./velero backup create {backup_name} --include-namespaces {namespaces_to_backup}')
        if backup_cmd.find('submitted successfully') < 0:
            print("Error backup was not successfully started")
            # todo Add failure flow to if backup cli command failed in func oadp_create_backup

    @logger_time_stamp
    def wait_for_condition_of_oadp_cr(self, cr_type, cr_name, testcase_timeout=3600):
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
                if state != 'InProgress':
                    print(f"current status: CR {cr_name} state: {state} !=InProgress")
                    return True
                    # sleep for x
                else:
                    print(f"current cr state is: {state} meaning its still showing InProgress")
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

    @logger_time_stamp
    def set_default_storage_class(self,sc):
        """
        method returns default sc if not set then empy string returned
        """
        current_sc = self.get_default_storage_class()
        if current_sc != sc:
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
                if del_cmd.find('submitted successfully') < 0:
                    print("Error did not delete successfully")
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
        print(f'old: {old_value}  new: {new_value}')
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
                print (f'correct name is {fullname}')
                return fullname
            else:
                # if len(pod_parts) == 2:
                base_name += part + "-"
                print (f"base_name patial is {base_name}")
        # if no return has already happened then podname doesnt contain digit
        # so we parse on - and assume last part of name is unique char string that we dont care about
        # csi-cephfsplugin-provisioner-asdf ==> return csi-cephfsplugin-provisioner
        fullname = base_name[:-2]
        print(f'correct name is {fullname}')
        return fullname


    @logger_time_stamp
    def get_resources_per_ns(self, namespace, label=''):
        """
        method returns oc adm top pods value
        """
        cmd_adm_top_ns_output = self.__ssh.run(cmd=f"oc adm top pods -n {namespace} --no-headers=true")
        if len(cmd_adm_top_ns_output.splitlines()) == 0:
            print(f'resulting query for get_resources_per_ns {namespace} resources failed 0 lines returned')
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
                            # Todo remove run time hash of pod resource info
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
    def get_list_of_pods(self, namespace):
        """
        This method returns list of pods in namespace
        :return:
        """
        running_pods = self.__ssh.run(
            cmd=f'oc get pods -n {namespace} --field-selector status.phase=Running --no-headers -o custom-columns=":metadata.name"')
        if running_pods != '':
            list_of_running_pods = running_pods.split('\n')
            print('running_pods detected in {namespace} are: {running_pods} expected {num_of_pods_expected}')
            return list_of_running_pods
        else:
            return False

    @logger_time_stamp
    def get_node_resource_avail_adm(self, ocp_node):
        """
        method returns CPU(cores), CPU%, Mem, Mem%
        """
        cmd_output = self.__ssh.run(cmd=f'oc adm top node {ocp_node} --no-headers')
        if cmd_output != '':
            node_adm_result = (list(filter(None, cmd_output.split(' '))))
            node_details = [{'name': node_adm_result[0], 'cores': node_adm_result[1], 'cpu_per': node_adm_result[2],
                             'mem_bytes': node_adm_result[3], 'mem_per': node_adm_result[4], "label": '' }]
            self.__run_metadata['summary']['resources']['nodes'].append(node_details)
            self.__result_dicts.append(node_details)
            print(f'node: {ocp_node} {node_adm_result}')

    @logger_time_stamp
    def collect_all_node_resource(self):
        """
        method collects all node avail resources via get_node_resource_avail_adm(self, ocp_node):
        """
        get_node_names = self.__ssh.run(cmd=f'oc get nodes --no-headers -o custom-columns=":metadata.name"')
        if len(get_node_names.splitlines()) > 0 and 'error' not in get_node_names:
            for bm in get_node_names.splitlines():
                self.get_node_resource_avail_adm(ocp_node=bm)
    # todo node resource pre and post run comparison with label=end not complete

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
    def initialize_pod_resources_by_base_name(self, pod_name):
        """
        method takes pod name and updates dict
        """
        base_pod_name = self.calc_pod_basename(pod_name)
        if base_pod_name not in self.__oadp_resources.keys():
            self.__oadp_resources[base_pod_name] = {}
            self.__oadp_runtime_resource_mapping[pod_name] = f"{base_pod_name}_0"
            # self.__oadp_resources[base_name] =  {"base_name": base_pod_name}
            print(f'key {base_pod_name} not in {self.__oadp_resources.keys} ')
        else:
            if base_pod_name in self.__oadp_resources.keys():
                count = 1
                for key, value in self.__oadp_resources.items():
                    if base_pod_name.lower() in key.lower():
                        count += 1
                        print (f" base_pod_name: {base_pod_name} key: {key} value: {value}  and count value is : {count}" )
                        print ("here")
                self.__oadp_resources[f"{base_pod_name}_{count - 1}"] = {}
                self.__oadp_runtime_resource_mapping[pod_name] = f"{base_pod_name}_{count - 1}"

    @logger_time_stamp
    @prometheus_metrics(yaml_full_path='/tmp/mpqe-scale-scripts/oadp-helpers/templates/metrics/metrics-oadp.yaml')
    def run_workload(self):
        """
        This method run oadp workload
        :return:
        """
        # Load Scenario Details
        test_scenario = self.load_test_scenario()
        self.create_pvutil_dataset(test_scenario)
        self.get_capacity_usage(test_scenario)
        self.get_expected_files_count(test_scenario)


        # Get OADP, Velero, Storage Details
        self.oadp_get_version_info()
        self.get_velero_details()
        self.get_storage_details()

        # Save test scenario run time settings run_metadata dict
        self.__run_metadata['summary']['runtime'].update(test_scenario)
        self.__result_dicts.append(test_scenario)
        self.generate_elastic_index(test_scenario)

        num_of_assets_desired: int = test_scenario['dataset']['pods_per_ns']
        namespace = test_scenario['args']['namespaces_to_backup']
        # namespace = f'busybox-perf-single-ns-{num_of_assets_desired}-pods'
        # Check if this is a single or multi name space scenario
        # if test_scenario['dataset']['total_namespaces'] == 1:
        #     num_of_assets_desired: int = test_scenario['dataset']['pods_per_ns']
        #     namespace = test_scenario['args']['namespaces_to_backup']
        #     # namespace = f'busybox-perf-single-ns-{num_of_assets_desired}-pods'
        # else:
        #     # todo: handle var loading for multi namespace
        #     print(' logic for mult var loading here - should only be seen if total_namespaces > 1')

        # Check if namespace containing dataset to be 'backed up' is present
        # if dataset is not present, create it as we intend to perform backup
        num_of_pods_expected = num_of_assets_desired
        target_namespace = test_scenario['args']['namespaces_to_backup']
        expected_sc = test_scenario['dataset']['sc']
        expected_size = test_scenario['dataset']['pv_size']

        # Verify desired storage is default storage class and others are non default
        self.set_default_storage_class(expected_sc)

        # when performing backup
        # Check if source namespace aka our dataset is preseent, if dataset not prsent then create it
        if test_scenario['args']['OADP_CR_TYPE'] == 'backup':
            skip_dataset_validation = test_scenario['args'].get('skip_source_dataset_check', False)
            dataset_already_present = self.verify_pod_presence_and_storage(num_of_pods_expected, target_namespace,
                                                                           expected_sc, expected_size, skip_dataset_validation)
            if not dataset_already_present:
                self.create_oadp_source_dataset(num_of_assets_desired, target_namespace=namespace, pv_size=test_scenario['dataset']['pv_size'], storage=test_scenario['dataset']['sc'] )

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

        # Get Pod Resource prior to test
        self.get_resources_per_ns(namespace='openshift-adp', label="start")
        # self.get_resources_per_ns(namespace='openshift-storage', label="start")

        # Launch OADP scenario
        self.oadp_execute_scenario(test_scenario, run_method='python')

        # Get Pod Resource after the test
        self.get_resources_per_ns(namespace='openshift-adp', label="end")
        # self.get_resources_per_ns(namespace='openshift-storage', label="end")

        # Parse result CR for status, and timestamps
        self.parse_oadp_cr(ns='openshift-adp', cr_type=test_scenario['args']['OADP_CR_TYPE'],
                           cr_name=test_scenario['args']['OADP_CR_NAME'])
        self.get_oadp_velero_and_cr_log(cr_name=test_scenario['args']['OADP_CR_NAME'],
                                        cr_type=test_scenario['args']['OADP_CR_TYPE'])
        self.get_logs_by_pod_ns(namespace='openshift-adp')

        # Get OCP node info
        self.collect_all_node_resource()
        self.create_json_summary()
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
    def generate_elastic_index(self, scenario):
        '''
        method creates elastic index name based on test_scenario data
        '''
        if scenario['testcase'] < 2.0:
            if scenario['dataset']['total_namespaces'] == 1:
                index_name = 'oadp-' + scenario['testtype'] + '-' + 'single-namespace'
            elif scenario['dataset']['total_namespaces'] > 1:
                index_name = 'oadp-' + scenario['testtype'] + '-' + 'multi-namespace'
        if (scenario['testcase'] >= 2.0 and scenario['testcase'] < 3.0):
            if scenario['dataset']['pods_per_ns'] == 1:
                index_name = 'oadp-' + scenario['testtype'] + '-' + 'single-pv-util'
        print(f'index_name {index_name}')
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

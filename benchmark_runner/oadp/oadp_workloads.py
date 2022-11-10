import os
import json

from benchmark_runner.common.ssh.ssh import SSH
from benchmark_runner.clusterbuster.clusterbuster_exceptions import MissingResultReport, MissingElasticSearch
from benchmark_runner.common.logger.logger_time_stamp import logger_time_stamp, logger
from benchmark_runner.workloads.workloads_operations import WorkloadsOperations


class OadpWorkloads(WorkloadsOperations):
    """
    This class is responsible for all Oadp workloads
    """

    def __init__(self):
        super().__init__()
        self.__oadp_path = '/tmp/mpqe-scale-scripts/mtc-helpers/busybox'
        # environment variables
        self.__namespace = self._environment_variables_dict.get('namespace', '')
        self.__oadp_workload = self._environment_variables_dict.get('oadp', '')
        self.__oadp_uuid = self._environment_variables_dict.get('oadp_uuid', '')
        self.__result_report = '/tmp/oadp-report.json'
        self.__artifactdir = os.path.join(self._run_artifacts_path, 'oadp-ci')
        self.__oadp_log = os.path.join(self._run_artifacts_path, 'oadp.log')
        self.__ssh = SSH()

    @logger_time_stamp
    def upload_oadp_result_to_elasticsearch(self):
        """
        This method upload to ElasticSearch the results
        :return:
        """
        result_report_json_file = open(self.__result_report)
        result_report_json_str = result_report_json_file.read()
        result_report_json_data = json.loads(result_report_json_str)
        for workload, oadp_tests in result_report_json_data.items():
            if self._run_type == 'test_ci':
                index = f'oadp-{workload}-test-ci-results'
            elif self._run_type == 'release':
                index = f'oadp-{workload}-release-results'
            else:
                index = f'oadp-{workload}-results'
            logger.info(f'upload index: {index}')
            if workload != 'metadata':
                for oadp_test in oadp_tests:
                    self._es_operations.upload_to_elasticsearch(index=index, data=oadp_test)
            # metadata
            elif workload == 'metadata':
                # run artifacts data
                result_report_json_data['metadata']['run_artifacts_url'] = os.path.join(self._run_artifacts_url,
                                                                                        f'{self._get_run_artifacts_hierarchy(workload_name=self._workload, is_file=True)}-{self._time_stamp_format}.tar.gz')
                self._es_operations.upload_to_elasticsearch(index=index, data=result_report_json_data['metadata'])
                self._es_operations.verify_elasticsearch_data_uploaded(index=index,
                                                                       uuid=result_report_json_data['metadata']['uuid'])

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
        self.delete_all()
        self.clear_nodes_cache()
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

    #  todo  Move to OC class and convert to use jq if possible
    def verify_running_pods(self, num_of_pods_expected, target_namespace):
        """
        This method verifies number of pods in namespace are in running state
        :return:
        """
        running_pods = self.__ssh.run(cmd=f'oc get pods -n {target_namespace} --field-selector status.phase=Running --no-headers -o custom-columns=":metadata.name"')
        if running_pods != '':
            list_of_running_pods = running_pods.split('\n')
            print('running_pods detected in {namespace} are: {running_pods} expected {num_of_pods_expected}')
            if len(list_of_running_pods) == num_of_pods_expected:
                print ("expected dataset is present")
                return True
            else:
                return False
        else:
            print('expected dataset NOT present returning false')
            return False

    def create_oadp_source_dataset(self,num_of_assets_desired, target_namespace, pv_size):
        """
        This method creates dataset for oadp to work against
        :return:
        """
        is_already_dataset_present=self.verify_running_pods(num_of_pods_expected=num_of_assets_desired, target_namespace=target_namespace)
        if not is_already_dataset_present:
            print('Warning did not find expected number of running pods in this namespace will create it now')
            self.__ssh.run(cmd=f'{self.__oadp_path}/BusyBoxPodSingleNS.sh {num_of_assets_desired} {pv_size}Mi')
            for i in range(1, num_of_assets_desired + 1):
                print('checking for f\'{namespace}-{i}\' in namespace= f\'{namespace}\'')
                self._oc.wait_for_pod_ready(pod_name=f'{target_namespace}-{i}', namespace=target_namespace)

    # todo wait_for_pod_create use this from OC class
    @logger_time_stamp
    def run_workload(self):
        """
        This method run oadp workload
        :return:
        """
        # release pass uuid and workload
        # Check if names pace
        # oc get pods -n busybox-perf-single-ns-3-pods -ojson | jq '.items[].status.containerStatuses | select(.[].ready == true) | .[].name '
        num_of_assets_desired: int = 3
        namespace = f'busybox-perf-single-ns-{num_of_assets_desired}-pods'
        dataset_already_present = self.verify_running_pods(num_of_pods_expected=num_of_assets_desired, target_namespace=namespace)
        if not dataset_already_present:
            self.create_oadp_source_dataset(num_of_assets_desired, target_namespace=namespace, pv_size=32)
    # todo need to queue workload
    # todo ansible-playbook test-oadp.yaml -e "test=1 testcase=1.1.1 plugin=csi use_cli=true OADP_CR_TYPE=backup OADP_CR_NAME=backup-csi-busybox-perf-single-ns-3-pods backup_name=backup-csi-busybox-perf-single-ns-3-pods namespaces_to_backup=busybox-perf-single-ns-3-pods result_dir_base_path=/tmp/results/" -vv
    # todo may need to verify before that CR is not present
    # todo convert csv to json file
    # todo test with prom on

        if os.path.exists(os.path.join(self.__result_report)) and not os.stat(self.__result_report).st_size == 0:
            self.__ssh.run(cmd=f'cp {self.__result_report} {self._run_artifacts_path}')
            return True
        else:
            result_report_json_data = {}
            result_report_json_data['result'] = 'Failed'
            result_report_json_data['run_artifacts_url'] = os.path.join(self._run_artifacts_url,
                                                                        f'{self._get_run_artifacts_hierarchy(workload_name=self._workload, is_file=True)}-{self._time_stamp_format}.tar.gz')
            if self._run_type == 'test_ci':
                index = f'oadp-metadata-test-ci-results'
            elif self._run_type == 'release':
                index = f'oadp-metadata-release-results'
            else:
                index = f'oadp-metadata-results'
            logger.info(f'upload index: {index}')
            self._es_operations.upload_to_elasticsearch(index=index, data=result_report_json_data)
            raise MissingResultReport()

    @logger_time_stamp
    def run(self):
        """
        This method run oadp workloads
        :return:
        """
        try:
            # initialize workload
            # self.initialize_workload()
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

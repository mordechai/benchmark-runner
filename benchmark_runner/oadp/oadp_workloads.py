
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
        self.__oadp_path = '/tmp/mpqe-scale-scripts/mtc-helpers/busybox/BusyBoxPodSingleNS.sh 1 32Mi'
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
                result_report_json_data['metadata']['run_artifacts_url'] = os.path.join(self._run_artifacts_url, f'{self._get_run_artifacts_hierarchy(workload_name=self._workload, is_file=True)}-{self._time_stamp_format}.tar.gz')
                self._es_operations.upload_to_elasticsearch(index=index, data=result_report_json_data['metadata'])
                self._es_operations.verify_elasticsearch_data_uploaded(index=index, uuid=result_report_json_data['metadata']['uuid'])

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

    @logger_time_stamp
    def run_workload(self):
        """
        This method run oadp workload
        :return:
        """
        # release pass uuid and workload
        self.__ssh.run(cmd=f'{self.__oadp_path}')
        if os.path.exists(os.path.join(self.__result_report)) and not os.stat(self.__result_report).st_size == 0:
            self.__ssh.run(cmd=f'cp {self.__result_report} {self._run_artifacts_path}')
            return True
        else:
            result_report_json_data = {}
            result_report_json_data['result'] = 'Failed'
            result_report_json_data['run_artifacts_url'] = os.path.join(self._run_artifacts_url, f'{self._get_run_artifacts_hierarchy(workload_name=self._workload, is_file=True)}-{self._time_stamp_format}.tar.gz')
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

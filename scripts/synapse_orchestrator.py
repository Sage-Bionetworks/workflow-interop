#!/usr/bin/env python
"""
Takes a given Synapse submission id prepare the workflow run
request, including retrieval and formatting of parameters,
if not provided; post the workflow run request to a given WES
implementation; monitor and report results of the workflow run.
"""
import logging

from scoring_harness.base_processor import (EvaluationQueueProcessor,
                                            _get_submission_submitter)
from scoring_harness import messages

# from wfinterop import config
from wfinterop import orchestrator

logging.basicConfig(format='%(asctime)s %(message)s')
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


# config.add_queue(queue_id='demo_queue',
#                  wf_type='CWL',
#                  wf_id='github.com/dockstore-testing/md5sum-checker',
#                  version_id='develop',
#                  trs_id='dockstore')


#  orchestrator.get_run_log(run_id='3a3a22027db24be2821c6ddce4d968cf',
#                           wes_id='local')

def run_workflow(workflow_input_json):
    job = orchestrator.run_job(queue_id='test_cwl_queue',
                               wes_id='local',
                               wf_jsonyaml="file://" + workflow_input_json)
    return job

class SynapseOrchestrator(EvaluationQueueProcessor):
    _status = "RECEIVED"
    _success_status = "ACCEPTED"

    def interaction_func(self, submission, **kwargs):
        # Download submission
        sub = self.syn.getSubmission(submission)
        job = run_workflow(sub.filePath)
        print(job)
        is_valid = job['status'] != "EXECUTOR_ERROR"
        # stderr, stdout = orchestrator.get_run_log(run_id=job['run_id'],
        #                                           wes_id='local')
        # print(stderr)
        # print(stdout)
        submission_info = {'valid': is_valid,
                           'error': job['status'],
                           'annotations': {'test': 'testing'},
                           'message': "INVALID"}
        print(submission_info)
        return submission_info

    def notify(self, submission, submission_info):
        pass


EVALUATION_QUEUES_CONFIG = [
    {'id': 9614423,
     'func': SynapseOrchestrator,
     'kwargs': {'goldstandard_path': 'path/to/sc1gold.txt'}}
]
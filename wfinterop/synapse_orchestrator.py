#!/usr/bin/env python
"""
Takes a given ID/URL for a workflow registered in a given TRS
implementation; prepare the workflow run request, including
retrieval and formatting of parameters, if not provided; post
the workflow run request to a given WES implementation;
monitor and report results of the workflow run.
"""
import datetime as dt
import json
import logging
import os
import shutil
import sys
import time

import chevron
from IPython.display import display, clear_output
from synapseclient import Synapse, Submission, SubmissionStatus
from synapseclient.exceptions import SynapseHTTPError
from synapseclient.annotations import from_submission_status_annotations

from wfinterop.config import add_queue, queue_config, wes_config
from wfinterop.util import ctime2datetime, convert_timedelta
from wfinterop.wes import WES
# from wfinterop.trs2wes import store_verification
from wfinterop.trs2wes import build_wes_request
from wfinterop.trs2wes import fetch_queue_workflow
from wfinterop.orchestrator import run_job
from wfinterop.synapse_queue import get_submission_bundle
from wfinterop.synapse_queue import get_submissions
# from wfinterop.synapse_queue import create_submission
from wfinterop.synapse_queue import update_submission

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

SCRIPT_PATH = os.path.abspath(os.path.dirname(__file__))
RUN_DOCKER_TEMPLATE = os.path.join(
    SCRIPT_PATH, '../templates/run_docker_template.cwl.mustache'
)
WORKFLOW_TEMPLATE = os.path.join(
    SCRIPT_PATH, '../templates/workflow.cwl.mustache',
)
VALIDATE_AND_SCORE = os.path.join(
    SCRIPT_PATH, '../testdata/validate_and_score.cwl',
)


def _get_docker_runjob_inputs(sub: Submission) -> dict:
    """Get run_job inputs for a docker submission.
    Create cwl tool with correct docker hint (via mustache) - subid.cwl.
    Create workflow that uses custom tool - subid_workflow.cwl.
    Create custom queue with submission id, configure it to run
    subid_workflow.cwl.

    Args:
        sub: Submission

    Returns:
        dict: queue_id: Queue id
              wf_jsonyaml: workflow inputs

    """
    repo_name = f"{sub.dockerRepositoryName}@{sub.dockerDigest}"
    # mustache template
    # Create docker tool with right docker hint
    cwl_input = {'docker_repository': repo_name,
                 'prediction_file': 'predictions.csv',
                 'training': False,
                 'scratch': False}
    with open(RUN_DOCKER_TEMPLATE, 'r') as mus_f:
        template = chevron.render(mus_f, cwl_input)
    with open(f"{sub.id}.cwl", "w") as sub_f:
        sub_f.write(template)

    # Create workflow with correct run docker step
    workflow_input = {'run_docker_tool': f"{sub.id}.cwl"}
    with open(WORKFLOW_TEMPLATE, 'r') as mus_f:
        template = chevron.render(mus_f, workflow_input)
    with open(f"{sub.id}_workflow.cwl", "w") as sub_f:
        sub_f.write(template)
    # TODO: This is a dummy value
    input_dict = {
        "input": {
            "class": "Directory",
            "location": "/home/tyu/sandbox"
        }
    }
    # TODO: The input can also be passed in.
    # Imagine the workfow + input scenario
    with open(f"{sub.id}.json", "w") as input_f:
        json.dump(input_dict, input_f)
    # This is to ensure validate_and_score.cwl lives in
    # home directory of CWL
    shutil.copy(VALIDATE_AND_SCORE, ".")
    attachments = ["file://" + os.path.abspath("validate_and_score.cwl"),
                   "file://" + os.path.abspath(f"{sub.id}.cwl")]
    add_queue(queue_id=sub.id,
              wf_type='CWL',
              wf_url=os.path.abspath(f"{sub.id}_workflow.cwl"),
              # TODO: need to fix this bug.  WF attachments shouldn't
              # be required
              wf_attachments=attachments)
    return {'queue_id': sub.id,
            'wf_jsonyaml': os.path.abspath(f"{sub.id}.json")}


def _get_workflow_runjob_inputs(sub: Submission) -> dict:
    """Get run_job inputs for a workflow submission.
    Create custom queue with submission id and configure it to run
    custom workflow

    Args:
        sub: Submission

    Returns:
        dict: queue_id: Queue id
              wf_jsonyaml: workflow inputs

    """
    # TODO: This is a dummy value
    input_dict = {
        "message": "hello world"
    }
    # TODO: The input can also be passed in.
    # Imagine the workfow + input scenario
    with open(f"{sub.id}.json", "w") as input_f:
        json.dump(input_dict, input_f)
    # This is a dummy value because add queue fails without attachments
    attachments = ["file://" + "dummpy.cwl"]
    add_queue(queue_id=sub.id,
              wf_type='CWL',
              wf_url=sub.filePath,
              # TODO: need to fix this bug.  WF attachments shouldn't
              # be required
              wf_attachments=attachments)
    return {'queue_id': sub.id,
            'wf_jsonyaml': os.path.abspath(f"{sub.id}.json")}


def get_runjob_inputs(sub: str, queue_id: str) -> dict:
    """Gets run_job inputs based on submission type

    Args:
        sub: Submission
        queue_id: Queue id

    Returns:
        dict: queue_id: Queue id
              wf_jsonyaml: workflow inputs

    """
    # If docker repository, use _run_docker_submission
    if sub.get("dockerRepositoryName") is None:
        # If the file is a workflow / tool
        if sub.filePath.endswith(".cwl"):
            workflow_inputs = _get_workflow_runjob_inputs(sub)
            wf_jsonyaml = workflow_inputs['wf_jsonyaml']
            queue_id = workflow_inputs['queue_id']
        else:
            wf_jsonyaml = sub.filePath
    else:
        docker_inputs = _get_docker_runjob_inputs(sub)
        wf_jsonyaml = docker_inputs['wf_jsonyaml']
        queue_id = docker_inputs['queue_id']
    return {'queue_id': queue_id,
            'wf_jsonyaml': wf_jsonyaml}


def _set_in_progress(syn: Synapse, status: SubmissionStatus) -> SubmissionStatus:
    """Sets submission to be in progress so that submissions can't
    be scheduled on other instances

    Args:
        syn: Synapse connection
        status: synapseclient.SubmissionStatus object

    Returns:
        status: stored synapseclient.SubmissionStatus

    Raises:
        SynapseHTTPError: Any SynapseHTTPError code other than 412 will be
                          raised.

    """
    try:
        status.status = "EVALUATION_IN_PROGRESS"
        # TODO: add in canCancel later
        # status.canCancel = True
        status = syn.store(status)
    except SynapseHTTPError as err:
        if err.response.status_code != 412:
            raise err
        return
    return status


def run_submission(syn: Synapse, queue_id: str, submission_id: str,
                   wes_id: str = None, opts: dict = None) -> dict:
    """For a single submission to a single evaluation queue, run
    the workflow in a single environment.

    Args:
        syn: Synapse connection
        queue_id: String identifying the workflow queue.
        submission_id: String identifying the submission.
        wes_id: String identifying the WES id.
        opts: run_job parameters

    Returns:
        Run information of submission
        {'run_id':...
         'status':...}

    """
    submission = get_submission_bundle(syn, submission_id)
    sub = submission['submission']
    status = submission['submissionStatus']

    status = _set_in_progress(syn, status)
    # Don't run submission if status is None
    if status is None:
        return
    # if submission['wes_id'] is not None:
    #     wes_id = submission['wes_id']
    # TODO: Fix hard coded wes_id
    wes_id = 'local'

    logger.info(" Submitting to WES endpoint '{}':"
                " \n - submission ID: {}"
                .format(wes_id, submission_id))

    # There are 4 basic types of submissions
    # Each will have different run jobs inputs
    run_job_inputs = get_runjob_inputs(sub, queue_id)
    wf_jsonyaml = run_job_inputs['wf_jsonyaml']
    queue_id = run_job_inputs['queue_id']

    logger.info(" Job parameters: '{}'".format(wf_jsonyaml))

    run_log = run_job(queue_id=queue_id,
                      wes_id=wes_id,
                      wf_jsonyaml="file://" + wf_jsonyaml,
                      submission=True,
                      opts=opts)
    # TODO: rename run['status'] later, it will collide with submission
    # status.status
    status = "INVALID" if run_log['status'] == "FAILED" else None
    update_submission(syn, submission_id, run_log, status)
    return run_log


def run_queue(syn: Synapse, queue_id: str, wes_id: str = None,
              opts: dict = None) -> dict:
    """
    Run all submissions in a queue in a single environment.

    Args:
        syn: Synapse connection
        queue_id: String identifying the workflow queue.
        wes_id: String identifying the WES id.
        opts: run_submission parameters

    Returns:
        Run information for each submission started
        {'submissionid': {'run_id':...
                          'status':...},
         ...}

    """
    queue_log = {}
    for submission_id in get_submissions(syn=syn, queue_id=queue_id,
                                         status='RECEIVED'):
        # submission = get_submission_bundle(syn, submission_id)
        # TODO: Add back in
        # if submission['wes_id'] is not None:
        #     wes_id = submission['wes_id']
        run_log = run_submission(syn=syn,
                                 queue_id=queue_id,
                                 submission_id=submission_id,
                                 wes_id=wes_id,
                                 opts=opts)
        if run_log is not None:
            run_log['wes_id'] = wes_id
            queue_log[submission_id] = run_log
        else:
            continue

    return queue_log


def monitor_queue(syn: Synapse, queue_id: str) -> dict:
    """Update the status of all submissions for a queue.

    Args:
        syn: Synapse connection
        queue_id: String identifying the workflow queue.

    Returns:
        updated information about each submission in a queue
        {'submissionid': {'run_id':...,
                          'status':...,
                          'wes_id':...,
                          'stderr':...,
                          'stdout':...,
                          'elapsed_time':...},
         ...}
    """
    current = dt.datetime.now()
    queue_log = {}
    # TODO: limitation of get_submissions of only being to get submission of
    # one status or all submissions (not combination)
    # TODO: Synapse submission status doesn't map directly into WES defined
    for sub_id in get_submissions(syn=syn, queue_id=queue_id,
                                  status="EVALUATION_IN_PROGRESS"):
        submission = get_submission_bundle(syn=syn, submission_id=sub_id)
        sub_status = submission['submissionStatus']
        sub = submission['submission']

        if  (sub.get('dockerRepositoryName') is not None or
             sub.filePath.endswith('.cwl')):
            queue_id = sub.id

        run_log = from_submission_status_annotations(sub_status.annotations)
        # if sub_status.status == 'RECEIVED':
        #     queue_log[sub_id] = {'status': 'PENDING'}
        #     continue

        # if run_log['run_id'] == 'failed':
        #     queue_log[sub_id] = {'status': 'FAILED'}
        #     continue
        # run_log['wes_id'] = submission['wes_id']
        # TODO: this shouldn't be hard coded
        run_log['wes_id'] = 'local'

        # TODO: SWITCH THIS TO INVALID, ACCEPTED...
        # if run_log['status'] in ['COMPLETE', 'CANCELLED', 'EXECUTOR_ERROR']:
        #     queue_log[sub_id] = run_log
        #     continue

        wes_instance = WES(run_log['wes_id'])
        run_status = wes_instance.get_run_status(run_log['run_id'])

        if run_status['state'] in ['QUEUED', 'INITIALIZING', 'RUNNING']:
            etime = convert_timedelta(
                current - ctime2datetime(run_log['start_time'])
            )
        elif 'elapsed_time' not in run_log:
            etime = 0
        else:
            etime = run_log['elapsed_time']

        run_log['status'] = run_status['state']
        run_log['elapsed_time'] = etime

        update_submission(syn=syn, submission_id=sub_id, value=run_log)

        if run_log['status'] == 'COMPLETE':
            wf_config = queue_config()[queue_id]
            # sub_status = run_log['status']
            sub_status = "ACCEPTED"
            if wf_config['target_queue']:
                # store_verification(wf_config['target_queue'],
                #                    submission['wes_id'])
                sub_status = 'VALIDATED'
            update_submission(syn=syn, submission_id=sub_id, value=run_log,
                              status=sub_status)

        if run_log['status'] in ['CANCELLED', 'EXECUTOR_ERROR']:
            wf_config = queue_config()[queue_id]
            # Differentiate between CANCELLED and EXECUTOR_ERROR
            if run_log['status'] == "CANCELLED":
                sub_status = "CLOSED"
            else:
                sub_status = "INVALID"
                # TODO: put into own function
                stderr = ''
                stdout = ''
                try:
                    stderr = wes_instance.get_run_stderr(run_log['run_id'])
                except Exception as err:
                    stderr = str(err)
                try:
                    stdout = wes_instance.get_run_stdout(run_log['run_id'])
                except Exception as err:
                    stdout = str(err)

                run_log['stderr'] = stderr
                run_log['stdout'] = stdout

            if wf_config['target_queue']:
                # store_verification(wf_config['target_queue'],
                #                    submission['wes_id'])
                sub_status = 'VALIDATED'
            update_submission(syn=syn, submission_id=sub_id, value=run_log,
                              status=sub_status)

        queue_log[sub_id] = run_log

    return queue_log


def monitor():
    """
    Monitor progress of workflow jobs.
    # TODO: This currently doesn't work
    """
    import pandas as pd
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_columns', 10)
    pd.set_option('display.expand_frame_repr', False)

    try:
        while True:
            statuses = []

            clear_output(wait=True)

            for queue_id in queue_config():
                queue_status = monitor_queue(queue_id)
                if queue_status:
                    statuses.append(queue_status)
                    print("\nWorkflow queue: {}".format(queue_id))
                    status_tracker = pd.DataFrame.from_dict(
                        queue_status,
                        orient='index')

                    display(status_tracker)

            terminal_statuses = ['FAILED',
                                 'COMPLETE',
                                 'CANCELED',
                                 'EXECUTOR_ERROR']
            if all([sub['status'] in terminal_statuses
                    for queue in statuses
                    for sub in queue.values()]):
                print("\nNo jobs running...")
            print("\n(Press CTRL+C to quit)")
            time.sleep(2)
            os.system('clear')
            sys.stdout.flush()

    except KeyboardInterrupt:
        print("\nDone")
        return

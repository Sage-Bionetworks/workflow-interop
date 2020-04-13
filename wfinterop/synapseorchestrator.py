#!/usr/bin/env python
"""
Takes a given ID/URL for a workflow registered in a given TRS
implementation; prepare the workflow run request, including
retrieval and formatting of parameters, if not provided; post
the workflow run request to a given WES implementation;
monitor and report results of the workflow run.
"""
import logging
import sys
import time
import os
import json
import datetime as dt

from IPython.display import display, clear_output

from wfinterop.config import queue_config, wes_config
from wfinterop.util import ctime2datetime, convert_timedelta
from wfinterop.wes import WES
from wfinterop.trs2wes import store_verification
from wfinterop.trs2wes import build_wes_request
from wfinterop.trs2wes import fetch_queue_workflow
from wfinterop.synapse_queue import get_submission_bundle
from wfinterop.synapse_queue import get_submissions
from wfinterop.synapse_queue import create_submission
from wfinterop.synapse_queue import update_submission

import synapseclient
from synapseclient.annotations import from_submission_status_annotations


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# TODO: Add synapseclient.login() not as global
# This is just for testing purposes
syn = synapseclient.login()


def run_job(queue_id: str,
            wes_id: str,
            wf_jsonyaml: str,
            opts: dict=None,
            add_attachments: list=None,
            submission: bool=False):
    """Put a workflow in the queue and immmediately run it.

    Args:
        queue_id: String identifying the workflow queue.
        wes_id:
        wf_jsonyaml:
        opts:
        add_attachments:
        submission:

    """
    wf_config = queue_config()[queue_id]
    if wf_config['workflow_url'] is None:
        wf_config = fetch_queue_workflow(queue_id)
    wf_attachments = wf_config['workflow_attachments']
    if add_attachments is not None:
        wf_attachments += add_attachments
        wf_attachments = list(set(wf_attachments))

    # TODO: Remove this for now
    # if not submission:
    #     submission_id = create_submission(queue_id=queue_id,
    #                                       submission_data=wf_jsonyaml,
    #                                       wes_id=wes_id)
    wes_instance = WES(wes_id)
    service_config = wes_config()[wes_id]
    request = {'workflow_url': wf_config['workflow_url'],
               'workflow_params': wf_jsonyaml,
               'attachment': wf_attachments}
    parts = []
    if opts is not None:
        parts = build_wes_request(
            workflow_file=request['workflow_url'],
            jsonyaml=request['workflow_params'],
            attachments=request['attachment'],
            **opts
        )
    if 'workflow_engine_parameters' in service_config:
        parts.append(('workflow_engine_parameters',
                      json.dumps(service_config['workflow_engine_parameters'])))
    parts = parts if len(parts) else None

    run_log = wes_instance.run_workflow(request, parts=parts)
    if run_log['run_id'] == 'failed':
        logger.info("Job submission failed for WES '{}'"
                    .format(wes_id))
        run_status = 'FAILED'
        sub_status = 'INVALID'
    else:
        logger.info("Job received by WES '{}', run ID: {}"
                    .format(wes_id, run_log['run_id']))
        run_log['start_time'] = dt.datetime.now().ctime()
        # TODO: Why the sleep here?
        time.sleep(10)
        run_status = wes_instance.get_run_status(run_log['run_id'])['state']
        sub_status = 'EVALUATION_IN_PROGRESS'
    run_log['status'] = run_status

    # TODO: Add this back in later
    # if not submission:
    #     update_submission(syn, submission_id, run_log, sub_status)
    return run_log


def run_submission(queue_id: str, submission_id: str, wes_id: str=None,
                   opts: dict=None):
    """For a single submission to a single evaluation queue, run
    the workflow in a single environment.

    Args:
        queue_id: String identifying the workflow queue.
        submission_id:
        wes_id:
        opts:

    """
    submission = get_submission_bundle(syn, submission_id)
    sub = submission['submission']
    status = submission['submissionStatus']
    # if submission['wes_id'] is not None:
    #     wes_id = submission['wes_id']
    # TODO: Fix hard coded wes_id
    wes_id = 'local'

    logger.info(" Submitting to WES endpoint '{}':"
                " \n - submission ID: {}"
                .format(wes_id, submission_id))
    wf_jsonyaml = sub.filePath
    logger.info(" Job parameters: '{}'".format(wf_jsonyaml))

    run_log = run_job(queue_id=queue_id,
                      wes_id=wes_id,
                      wf_jsonyaml="file://" + wf_jsonyaml,
                      submission=True,
                      opts=opts)

    update_submission(syn, submission_id, run_log, 'EVALUATION_IN_PROGRESS')
    return run_log


def run_queue(queue_id, wes_id=None, opts=None):
    """
    Run all submissions in a queue in a single environment.

    :param str queue_id: String identifying the workflow queue.
    :param str wes_id:
    :param dict opts:
    """
    queue_log = {}
    for submission_id in get_submissions(syn, queue_id=queue_id, status='RECEIVED'):
        submission = get_submission_bundle(syn, submission_id)
        # TODO: Add back in
        # if submission['wes_id'] is not None:
        #     wes_id = submission['wes_id']
        run_log = run_submission(queue_id=queue_id,
                                 submission_id=submission_id,
                                 wes_id=wes_id,
                                 opts=opts)
        run_log['wes_id'] = wes_id
        queue_log[submission_id] = run_log

    return queue_log


def monitor_queue(queue_id):
    """
    Update the status of all submissions for a queue.

    :param str queue_id: String identifying the workflow queue.
    """
    current = dt.datetime.now()
    queue_log = {}
    # TODO: limitation of get_submissions of only being to get
    # one type of submission
    for sub_id in get_submissions(syn, queue_id=queue_id,
                                  status="EVALUATION_IN_PROGRESS"):
        submission = get_submission_bundle(syn, submission_id=sub_id)
        sub_status = submission['submissionStatus']

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
        # update_submission(syn, submission_id, run_log, 'EVALUATION_IN_PROGRESS')

        update_submission(syn, sub_id, run_log)

        if run_log['status'] == 'COMPLETE':
            wf_config = queue_config()[queue_id]
            # sub_status = run_log['status']
            sub_status = "ACCEPTED"
            if wf_config['target_queue']:
                # store_verification(wf_config['target_queue'],
                #                    submission['wes_id'])
                sub_status = 'VALIDATED'
            update_submission(syn, sub_id, run_log, status=sub_status)

        if run_log['status'] in ['CANCELLED', 'EXECUTOR_ERROR']:
            wf_config = queue_config()[queue_id]
            # TODO: differentiate between CANCELLED and EXECUTOR_ERROR
            if run_log['status'] == "CANCELLED":
                sub_status = "CLOSED"
            else:
                sub_status = "INVALID"
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
            update_submission(syn, sub_id, run_log, status=sub_status)


        queue_log[sub_id] = run_log

    return queue_log


def monitor():
    """
    Monitor progress of workflow jobs.
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
                if len(queue_status):
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


def get_run_log(wes_id, run_id):
    """Gets a workflows run logs"""
    wes_instance = WES(wes_id)
    stderr = wes_instance.get_run_stderr(run_id)
    stdout = wes_instance.get_run_stdout(run_id)
    return stderr, stdout

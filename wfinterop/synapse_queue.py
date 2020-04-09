#!/usr/bin/env python
"""
Synapse Queue
"""
import logging
import os
import datetime as dt

from wfinterop.util import get_json, save_json, annotate_submission
import synapseclient
from synapseclient.retry import _with_retry

logger = logging.getLogger(__name__)


# def create_queue(self):

def create_submission(syn, queue_id: str, entity_id: str) -> str:
    """
    Submit a new job request to an evaluation queue.

    Both type and wf_name are optional but could be used with TRS.
    
    Args:
        queue_id: String identifying the workflow queue.
        entity_id: Entity id to submit

    Returns:
        submission id

    """
    # TODO: This is more complex because we may have to store the file too?
    submission = syn.submit(evaluation=queue_id, entity=entity_id)
    logger.info(" Queueing job for '{}' endpoint:"
                "\n - submission ID: {}".format(wes_id, submission.id))
    return submission.id


def get_submissions(syn, queue_id,
                    status='RECEIVED'):
    """Return all ids with the requested status.

    Args:
        queue_id: String identifying the workflow queue.
        status:

    Returns:
        list: List of submission ids

    """
    submissions = syn.getSubmissionBundles(queue_id, status=status)
    # submissions = get_json(submission_queue)
    # if not exclude_status:
    #     status = [s for s in status if s not in exclude_status]
    try:
        return [sub.id for sub, sub_status in submissions
                if sub_status.status == status]
    except KeyError:
        return []


def get_submission_bundle(syn, submission_id: str) -> dict:
    """Return the submission's info.

    Args:
        submission_id: Submission id

    Returns:
        dict: SubmissionBundle - submission, submissionStatus

    """
    sub = syn.getSubmission(submission_id)
    status = syn.getSubmissionStatus(submission_id)
    # TODO: possible to get wes_id here
    bundle = {'submission': sub,
              'submissionStatus': status}
    return bundle


def update_submission(syn, submission_id: str, value: dict, status: str):
    """
    Update the status of a submission.

    Args:
        submission_id: Submission id
        value: annotation values in a dict
        status: Submission status

    """
    _with_retry(lambda: annotate_submission(syn, submission_id,
                                            value, status),
                wait=3,
                retries=10,
                retry_status_codes=[412, 429, 500, 502, 503, 504],
                verbose=True)

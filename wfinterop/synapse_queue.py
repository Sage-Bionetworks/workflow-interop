#!/usr/bin/env python
"""
Synapse Queue
"""
import logging

from synapseclient import Synapse
from synapseclient.core.retry import with_retry

from .util import annotate_submission

logger = logging.getLogger(__name__)
# TODO: Create OrchestratorQueue and possibly extend submissions


def create_submission(syn: Synapse, queue_id: str, entity_id: str) -> str:
    """
    Submit a new job request to an evaluation queue.

    Both type and wf_name are optional but could be used with TRS.

    Args:
        syn: Synapse connection
        queue_id: String identifying the workflow queue.
        entity_id: Entity id to submit

    Returns:
        submission id

    """
    # TODO: This is more complex because we may have to store the file too?
    submission = syn.submit(evaluation=queue_id, entity=entity_id)
    logger.info(" Queueing job for '{}' endpoint:"
                "\n - submission ID: {}".format(queue_id, submission.id))
    return submission.id


def get_submissions(syn: Synapse, queue_id: str,
                    status: str = None) -> list:
    """Return all ids with the requested status.

    Args:
        syn: Synapse connection.
        queue_id: String identifying the workflow queue.
        status: Status of submission to retrieve.
                One of: https://rest-docs.synapse.org/rest/org/sagebionetworks/evaluation/model/SubmissionStatusEnum.html

    Returns:
        List of submission ids

    """
    submissions = syn.getSubmissionBundles(queue_id, status=status)
    # submissions = get_json(submission_queue)
    # if not exclude_status:
    #     status = [s for s in status if s not in exclude_status]
    try:
        return [sub.id for sub, sub_status in submissions]
    except KeyError:
        return []


def get_submission_bundle(syn: Synapse, submission_id: str) -> dict:
    """Return the submission's info.
    # TODO: Expose this as an API call?

    Args:
        syn: Synapse connection
        submission_id: Submission id

    Returns:
        Submission bundle:
        {'submission': {...},
         'submissionStatus': {...}}

    """
    sub = syn.getSubmission(submission_id)
    status = syn.getSubmissionStatus(submission_id)
    # TODO: possible to get wes_id here (queue can be configured with wes_id)
    bundle = {'submission': sub,
              'submissionStatus': status}
    return bundle


def update_submission(syn: Synapse, submission_id: str, value: dict,
                      status: str = None):
    """
    Update the status of a submission. Accounts for concurrent status updates

    Args:
        syn: Synapse connection
        submission_id: Submission id
        value: annotation values in a dict
        status: Submission status

    """
    # TODO: No idea how to test this...
    with_retry(lambda: annotate_submission(syn, submission_id,
                                           value, status=status,
                                           is_private=False,
                                           force=True),
                wait=3,
                retries=10,
                retry_status_codes=[412, 429, 500, 502, 503, 504],
                verbose=True)

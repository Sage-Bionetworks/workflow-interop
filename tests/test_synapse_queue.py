import logging
import mock
from mock import Mock, patch
import pytest
import json
import datetime as dt

import synapseclient
from synapseclient.retry import _with_retry

from wfinterop import util
from wfinterop.synapse_queue import (create_submission, get_submissions,
                                     get_submission_bundle, update_submission)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def test_create_submission(mock_syn):
    """Tests creating submission"""
    sub = Mock(synapseclient.Submission)
    sub.id = "test"
    with patch.object(mock_syn, "submit", return_value=sub) as patch_submit:
        test_sub_id = create_submission(mock_syn,
                                        queue_id='mock_queue_1',
                                        entity_id='syn12345')
        assert test_sub_id == sub.id
        patch_submit.assert_called_once_with(evaluation='mock_queue_1',
                                             entity='syn12345')


def test_get_submissions(mock_syn):
    # monkeypatch.setattr('wfinterop.queue.submission_queue', 
    #                     str(mock_submissionqueue))
    sub = Mock(synapseclient.Submission)
    sub.id = "test"
    sub2 = Mock(synapseclient.Submission)
    sub2.id = "test2"
    with patch.object(mock_syn, "getSubmissionBundles",
                      return_value=[(sub, None),
                                    (sub2, None)]) as patch_bundles:
        test_submissions = get_submissions(syn=mock_syn,
                                           queue_id='mock_queue_1',
                                           status='RECEIVED')
        assert test_submissions == ['test', 'test2']
        patch_bundles.assert_called_once_with('mock_queue_1',
                                              status='RECEIVED')


def test_get_submission_bundle(mock_syn):
    sub = Mock(synapseclient.Submission)
    sub_status = Mock(synapseclient.SubmissionStatus)
    with patch.object(mock_syn, "getSubmission",
                      return_value=sub) as patch_get,\
         patch.object(mock_syn, "getSubmissionStatus",
                      return_value=sub_status) as patch_get_status:
        test_bundle = get_submission_bundle(syn=mock_syn,
                                            submission_id='mock_sub')
        patch_get.assert_called_once_with('mock_sub')
        patch_get_status.assert_called_once_with('mock_sub')
        assert test_bundle == {"submission": sub,
                               "submissionStatus": sub_status}


def test_update_submission(mock_syn):
    # TODO: Not sure how to test this function
    update_submission(mock_syn, 'mock_sub', {'foo': 'bar'}, 'ACCEPTED')


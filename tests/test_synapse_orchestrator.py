"""Test synapse orchestrator"""
import mock
from mock import Mock, patch
import pytest
import datetime as dt

from bravado.requests_client import RequestsClient
from bravado.client import SwaggerClient, ResourceDecorator
from bravado.testing.response_mocks import BravadoResponseMock
import synapseclient
try:
    from synapseclient.exceptions import SynapseHTTPError
except ModuleNotFoundError:
    from synapseclient.core.exceptions import SynapseHTTPError

from wfinterop.wes.wrapper import WES
from wfinterop.synapse_orchestrator import (run_submission, run_queue,
                                            monitor_queue, monitor,
                                            _set_in_progress)


def test__set_in_progress(mock_syn):
    status = Mock(synapseclient.SubmissionStatus)
    status.id = "test"
    with patch.object(mock_syn, "store", return_value=status) as patch_store:
        new_status = _set_in_progress(mock_syn, status=status)
        assert new_status == status
        status.status = "EVALUATION_IN_PROGRESS"
        patch_store.assert_called_once_with(status)


def test__set_in_progress_412error(mock_syn):
    status = Mock(synapseclient.SubmissionStatus)
    status.id = "test"
    mocked_412 = SynapseHTTPError("foo", response=Mock(status_code=412))
    with patch.object(mock_syn, "store",
                      side_effect=mocked_412):
        new_status = _set_in_progress(mock_syn, status=status)
        assert new_status is None


def test__set_in_progress_raises(mock_syn):
    status = Mock(synapseclient.SubmissionStatus)
    status.id = "test"
    mocked_409 = SynapseHTTPError("foo", response=Mock(status_code=409))
    with patch.object(mock_syn, "store",
                      side_effect=mocked_409),\
         pytest.raises(SynapseHTTPError):
        new_status = _set_in_progress(mock_syn, status=status)
        assert new_status is None


def test_run_submission(mock_run_log,
                        mock_syn,
                        monkeypatch):
    sub = {'submission': Mock(filePath="foo"),
           'submissionStatus': Mock()}
    runjob_inputs = {"wf_jsonyaml": "foo",
                     "queue_id": 'foo'}
    monkeypatch.setattr('wfinterop.synapse_orchestrator.get_submission_bundle',
                        lambda x,y: sub)
    monkeypatch.setattr('wfinterop.synapse_orchestrator._set_in_progress',
                        lambda x,y: sub['submissionStatus'])
    monkeypatch.setattr('wfinterop.synapse_orchestrator.get_runjob_inputs',
                        lambda x,y: runjob_inputs)
    monkeypatch.setattr('wfinterop.synapse_orchestrator.run_job',
                        lambda **kwargs: mock_run_log)
    monkeypatch.setattr('wfinterop.synapse_orchestrator.update_submission',
                        lambda w,x,y,z: None)

    test_run_log = run_submission(syn=mock_syn,
                                  queue_id='mock_queue',
                                  submission_id='mock_sub',
                                  wes_id='local')

    assert test_run_log == mock_run_log


def test_run_queue(mock_queue_config,
                   mock_submission,
                   mock_queue_log,
                   mock_syn,
                   monkeypatch):
    monkeypatch.setattr('wfinterop.synapse_orchestrator.queue_config',
                        lambda: mock_queue_config)
    monkeypatch.setattr('wfinterop.synapse_orchestrator.get_submissions',
                        lambda **kwargs: ['mock_sub'])

    mock_run_log = mock_submission['mock_sub']['run_log']
    monkeypatch.setattr('wfinterop.synapse_orchestrator.run_submission',
                        lambda **kwargs: mock_run_log)

    test_queue_log = run_queue(syn=mock_syn,
                               queue_id='mock_queue_1',
                               wes_id='mock_wes')
    mock_queue_log['mock_sub']['status'] = ''
    mock_queue_log['mock_sub'].pop('elapsed_time')
    assert test_queue_log == mock_queue_log


def test_monitor_queue(mock_submission,
                       mock_queue_log,
                       mock_wes,
                       mock_syn,
                       monkeypatch):
    sub = {'submission': Mock(filePath="foo"),
           'submissionStatus': Mock()}
    monkeypatch.setattr('wfinterop.synapse_orchestrator.get_submissions',
                        lambda **kwargs: ['mock_sub'])
    monkeypatch.setattr('wfinterop.synapse_orchestrator.get_submission_bundle',
                        lambda **kwargs: sub)
    monkeypatch.setattr('wfinterop.synapse_orchestrator.from_submission_status_annotations',
                        lambda x: mock_queue_log['mock_sub'])
    monkeypatch.setattr('wfinterop.synapse_orchestrator.WES',
                        lambda wes_id: mock_wes)
    monkeypatch.setattr('wfinterop.synapse_orchestrator.convert_timedelta',
                        lambda x: 0)
    monkeypatch.setattr('wfinterop.synapse_orchestrator.ctime2datetime',
                        lambda x: dt.datetime.now())
    monkeypatch.setattr('wfinterop.synapse_orchestrator.update_submission',
                        lambda **kwargs: None)

    mock_wes.get_run_status.return_value = {'run_id': 'mock_run', 
                                            'state': 'RUNNING'}

    mock_start_time = dt.datetime.now()

    test_queue_log = monitor_queue(mock_syn, 'mock_queue_1')
    assert test_queue_log == mock_queue_log

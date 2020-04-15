"""Test synapse orchestrator"""
import mock
from mock import Mock
import pytest
import datetime as dt

from bravado.requests_client import RequestsClient
from bravado.client import SwaggerClient, ResourceDecorator
from bravado.testing.response_mocks import BravadoResponseMock

from wfinterop.wes.wrapper import WES
from wfinterop.synapse_orchestrator import run_submission
from wfinterop.synapse_orchestrator import run_queue
from wfinterop.synapse_orchestrator import monitor_queue
from wfinterop.synapse_orchestrator import monitor


def test_run_submission(mock_run_log,
                        mock_syn,
                        monkeypatch):
    sub = {'submission': Mock(filePath="foo"),
           'submissionStatus': Mock()}
    monkeypatch.setattr('wfinterop.synapse_orchestrator.get_submission_bundle',
                        lambda x,y: sub)
    monkeypatch.setattr('wfinterop.synapse_orchestrator.update_submission',
                        lambda w,x,y,z: None)
    monkeypatch.setattr('wfinterop.synapse_orchestrator.run_job',
                        lambda **kwargs: mock_run_log)

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

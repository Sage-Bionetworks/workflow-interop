from datetime import datetime
import logging
from mock import Mock, patch

from challengeutils import utils
import pytest
import yaml
import textwrap
import synapseclient

from wfinterop import util

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def test_open_file_read(tmpdir):
    mock_contents = 'mock text'
    mock_file = tmpdir.join('mock.txt')
    mock_file.write(mock_contents)

    with util.open_file(str(mock_file), 'r') as f:
        test_contents = f.read()
    assert test_contents == mock_contents


def test_open_file_write(tmpdir):
    mock_contents = 'mock text'
    mock_file = tmpdir.join('mock.txt')
    mock_file.write('')

    with util.open_file(str(mock_file), 'w') as f:
        f.write(mock_contents)
    
    assert mock_file.read() == mock_contents


def test_open_file_write_url(tmpdir):
    with pytest.raises(ValueError):
        with util.open_file('http://mock.com', 'w') as f:
            f.write('mock_text')


def test_heredoc():
    test_string = util.heredoc(
        '''
        Mock string:
        {value}
        ''', {'value': 'mock_val'}
    )

    mock_string = "Mock string:\nmock_val\n"
    assert(test_string == textwrap.dedent(mock_string))


def test_get_yaml(tmpdir):
    mock_string = """
    section:
       key: {}
    """
    mock_file = tmpdir.join('mock.yaml')
    mock_file.write(textwrap.dedent(mock_string))

    test_object = util.get_yaml(str(mock_file))
    mock_object = {'section': {'key': {}}}

    assert(test_object == mock_object)


def test_save_yaml(tmpdir):
    mock_object = {'section': {'key': {}}}

    mock_file = tmpdir.join('mock.yaml')

    util.save_yaml(str(mock_file), mock_object)

    mock_string = "section:\n  key: {}\n"

    assert(mock_file.read() == mock_string)


def test_get_json(tmpdir):
    mock_string = """
    {
       "section": {
           "key": {}
        }
    }
    """
    mock_file = tmpdir.join('mock.json')
    mock_file.write(textwrap.dedent(mock_string))

    test_object = util.get_json(str(mock_file))
    mock_object = {'section': {'key': {}}}

    assert(test_object == mock_object)


def test_save_json(tmpdir):
    mock_object = {'section': {'key': {}}}

    mock_file = tmpdir.join('mock.json')

    util.save_json(str(mock_file), mock_object)

    mock_string = """{
    "section": {
        "key": {}
    }
}"""

    assert(mock_file.read() == textwrap.dedent(mock_string))


def test_ctime2datetime():
    mock_string = 'Sun Jan 01 00:00:00 2000'

    test_datetime = util.ctime2datetime(mock_string)

    assert(test_datetime == datetime(2000, 1, 1, 0, 0))


def test_convert_timedelta():
    mock_duration = datetime(2000, 1, 1, 1, 1) - datetime(2000, 1, 1, 0, 0)

    test_string = util.convert_timedelta(mock_duration)

    assert(test_string == '1h:1m:0s')


def test_annotate_submission(mock_syn):
    sub_status = Mock(synapseclient.SubmissionStatus)
    sub_status.status = "RECEIVED"


    updated_status = Mock(synapseclient.SubmissionStatus)
    updated_status.status = "SCORED"

    with patch.object(mock_syn, "getSubmissionStatus",
                      return_value=sub_status) as patch_get,\
         patch.object(util, "update_single_submission_status",
                      return_value=updated_status) as patch_update,\
         patch.object(mock_syn, "store") as patch_store:
        response = util.annotate_submission(mock_syn, 'subid_1',
                                            annotation_dict={"test": "foo",
                                                             "notnone": None},
                                            status="SCORED",
                                            is_private=False, force=True)
        assert response.status_code == 200
        patch_get.assert_called_once_with('subid_1')
        sub_status.status = "SCORED"
        patch_update.assert_called_once_with(sub_status, {'test': 'foo'},
                                             is_private=False,
                                             force=True)
        patch_store.assert_called_once_with(updated_status)

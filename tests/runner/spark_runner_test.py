from datetime import datetime
from pathlib import PosixPath
from typing import List, Tuple, Any, Optional

from dateutil.tz import tzutc

from blurr.core.store_key import Key
from blurr.runner.spark_runner import SparkRunner


def execute_runner(stream_dtc_file: str, window_dtc_file: Optional[str],
                   local_json_files: List[str]) -> Tuple[SparkRunner, Any]:
    runner = SparkRunner(stream_dtc_file, window_dtc_file)
    return runner, runner.execute(runner.get_record_rdd_from_json_files(local_json_files))


def get_spark_output(out_dir: PosixPath) -> List:
    output_files = out_dir.listdir(lambda x: x.basename.startswith('part'))
    output_text = []
    for output_file in output_files:
        output_text.extend(output_file.readlines(cr=False))
    return output_text


def test_only_stream_dtc_provided():
    runner, data = execute_runner('tests/data/stream.yml', None, ['tests/data/raw.json'])
    block_data = {k: v for (k, v) in data.collect()}

    assert len(block_data) == 8

    # Stream DTC output
    assert block_data[Key('userA', 'session', datetime(
        2018, 3, 7, 23, 35, 31, tzinfo=tzutc()))] == {
            '_identity': 'userA',
            '_start_time': datetime(2018, 3, 7, 23, 35, 31, tzinfo=tzutc()).isoformat(),
            '_end_time': datetime(2018, 3, 7, 23, 35, 32, tzinfo=tzutc()).isoformat(),
            'events': 2,
            'country': 'IN',
            'continent': 'World'
        }

    assert block_data[Key('userA', 'session', datetime(2018, 3, 7, 22, 35, 31))] == {
        '_identity': 'userA',
        '_start_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()).isoformat(),
        '_end_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()).isoformat(),
        'events': 1,
        'country': 'US',
        'continent': 'North America'
    }

    assert block_data[Key('userA', 'state')] == {
        '_identity': 'userA',
        'country': 'IN',
        'continent': 'World'
    }

    assert block_data[Key('userB', 'session', datetime(
        2018, 3, 7, 23, 35, 31, tzinfo=tzutc()))] == {
            '_identity': 'userB',
            '_start_time': datetime(2018, 3, 7, 23, 35, 31, tzinfo=tzutc()).isoformat(),
            '_end_time': datetime(2018, 3, 7, 23, 35, 31, tzinfo=tzutc()).isoformat(),
            'events': 1,
            'country': '',
            'continent': ''
        }


def test_no_variable_aggreate_data_stored():
    runner, data = execute_runner('tests/data/stream.yml', None, ['tests/data/raw.json'])
    block_data = {k: v for (k, v) in data.collect()}

    # Variables should not be stored
    assert Key('userA', 'vars') not in block_data


def test_stream_and_window_dtc_provided():
    runner, data = execute_runner('tests/data/stream.yml', 'tests/data/window.yml',
                                  ['tests/data/raw.json'])
    window_data = dict(data.collect())

    assert window_data['userA'] == [{
        'last_session.events': 1,
        'last_session._identity': 'userA',
        'last_day.total_events': 1,
        'last_day._identity': 'userA'
    }]
    assert window_data['userB'] == []


def test_write_output_file_only_source_dtc_provided(tmpdir):
    runner, data = execute_runner('tests/data/stream.yml', None, ['tests/data/raw.json'])
    out_dir = tmpdir.join('out')
    runner.write_output_file(str(out_dir), data)
    output_text = get_spark_output(out_dir)
    assert ('["userA/session/2018-03-07T22:35:31+00:00", {'
            '"_identity": "userA", '
            '"_start_time": "2018-03-07T22:35:31+00:00", '
            '"_end_time": "2018-03-07T22:35:31+00:00", '
            '"events": 1, '
            '"country": "US", '
            '"continent": "North America"'
            '}]') in output_text


def test_write_output_file_with_stream_and_window_dtc_provided(tmpdir):
    runner, data = execute_runner('tests/data/stream.yml', 'tests/data/window.yml',
                                  ['tests/data/raw.json'])
    out_dir = tmpdir.join('out')
    runner.write_output_file(str(out_dir), data)
    output_text = get_spark_output(out_dir)
    assert 'last_day._identity,last_day.total_events,last_session._identity,last_session.events' in output_text
    assert 'userA,1,userA,1' in output_text

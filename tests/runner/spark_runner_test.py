from datetime import datetime
from pathlib import PosixPath
from typing import List, Tuple, Any, Optional, Dict

from dateutil.tz import tzutc

from blurr.core.store_key import Key, KeyType
from blurr.runner.spark_runner import SparkRunner, get_spark_session
from tests.core.conftest import assert_aggregate_snapshot_equals


def execute_runner(stream_bts_file: str,
                   window_bts_file: Optional[str],
                   local_json_files: List[str],
                   old_state: Optional[Dict[str, Dict]] = None) -> Tuple[SparkRunner, Any]:
    runner = SparkRunner(stream_bts_file, window_bts_file)
    if old_state:
        old_state = get_spark_session().sparkContext.parallelize(old_state.items())
    return runner, runner.execute(
        runner.get_record_rdd_from_json_files(local_json_files), old_state)


def get_spark_output(out_dir: PosixPath) -> List:
    output_files = out_dir.listdir(lambda x: x.basename.startswith('part'))
    output_text = []
    for output_file in output_files:
        output_text.extend(output_file.readlines(cr=False))
    return output_text


def test_only_stream_bts_provided():
    runner, data = execute_runner('tests/data/stream.yml', None, ['tests/data/raw.json'])
    block_data = {}
    window_data = {}
    for id, (per_id_block_data, per_id_window_data) in data.collect():
        block_data[id] = per_id_block_data
        window_data[id] = per_id_window_data

    assert len(block_data) == 3

    # Stream BTS output
    assert_aggregate_snapshot_equals(block_data['userA'][Key(KeyType.TIMESTAMP, 'userA', 'session', [],
                                   datetime(2018, 3, 7, 23, 35, 31, tzinfo=tzutc()))], {
                                       '_start_time': datetime(
                                           2018, 3, 7, 23, 35, 31, tzinfo=tzutc()).isoformat(),
                                       '_end_time': datetime(
                                           2018, 3, 7, 23, 35, 32, tzinfo=tzutc()).isoformat(),
                                       'events': 2,
                                       'country': 'IN',
                                       'continent': 'World'
                                   })

    assert_aggregate_snapshot_equals(block_data['userA'][Key(KeyType.TIMESTAMP, 'userA', 'session', [],
                                   datetime(2018, 3, 7, 22, 35, 31))], {
                                       '_start_time': datetime(
                                           2018, 3, 7, 22, 35, 31, tzinfo=tzutc()).isoformat(),
                                       '_end_time': datetime(
                                           2018, 3, 7, 22, 35, 31, tzinfo=tzutc()).isoformat(),
                                       'events': 1,
                                       'country': 'US',
                                       'continent': 'North America'
                                   })

    assert_aggregate_snapshot_equals(block_data['userA'][Key(KeyType.DIMENSION, 'userA', 'state')], {
        'country': 'IN',
        'continent': 'World'
    })

    assert_aggregate_snapshot_equals(block_data['userB'][Key(KeyType.TIMESTAMP, 'userB', 'session', [],
                                   datetime(2018, 3, 7, 23, 35, 31, tzinfo=tzutc()))], {
                                       '_start_time': datetime(
                                           2018, 3, 7, 23, 35, 31, tzinfo=tzutc()).isoformat(),
                                       '_end_time': datetime(
                                           2018, 3, 7, 23, 35, 31, tzinfo=tzutc()).isoformat(),
                                       'events': 1,
                                       'country': '',
                                       'continent': ''
                                   })

    assert window_data == {'userA': [], 'userB': [], 'userC': []}


def test_no_variable_aggreate_data_stored():
    runner, data = execute_runner('tests/data/stream.yml', None, ['tests/data/raw.json'])
    block_data = {}
    for id, (per_id_block_data, _) in data.collect():
        block_data[id] = per_id_block_data

    # Variables should not be stored
    assert Key(KeyType.DIMENSION, 'userA', 'vars') not in block_data['userA']


def test_stream_and_window_bts_provided():
    runner, data = execute_runner('tests/data/stream.yml', 'tests/data/window.yml',
                                  ['tests/data/raw.json'])
    window_data = {}
    for id, (_, per_id_window_data) in data.collect():
        window_data[id] = per_id_window_data

    assert window_data['userA'] == [{
        'last_session.events': 1,
        'last_day.total_events': 1,
    }]
    assert window_data['userB'] == []


def test_stream_bts_with_state():
    _, data_combined = execute_runner('tests/data/stream.yml', None,
                                      ['tests/data/raw.json', 'tests/data/raw2.json'], None)

    _, data_separate = execute_runner('tests/data/stream.yml', None, ['tests/data/raw.json'], None)
    old_state = {
        identity: block_data
        for identity, (block_data, window_data) in data_separate.collect()
    }
    _, data_separate = execute_runner('tests/data/stream.yml', None, ['tests/data/raw2.json'],
                                      old_state)

    assert {}.update(data_separate.collect()) == {}.update(data_combined.collect())


def test_stream_and_window_bts_with_state():
    _, data_combined = execute_runner('tests/data/stream.yml', 'tests/data/window.yml',
                                      ['tests/data/raw.json', 'tests/data/raw2.json'], None)

    _, data_separate = execute_runner('tests/data/stream.yml', 'tests/data/window.yml',
                                      ['tests/data/raw.json'], None)
    old_state = {
        identity: block_data
        for identity, (block_data, window_data) in data_separate.collect()
    }
    _, data_separate = execute_runner('tests/data/stream.yml', 'tests/data/window.yml',
                                      ['tests/data/raw2.json'], old_state)

    assert {}.update(data_separate.collect()) == {}.update(data_combined.collect())


def test_write_output_file_only_source_bts_provided(tmpdir):
    runner, data = execute_runner('tests/data/stream.yml', None, ['tests/data/raw.json'])
    out_dir = tmpdir.join('out')
    runner.write_output_file(str(out_dir), data)
    output_text = get_spark_output(out_dir)
    assert ('["userA/session//2018-03-07T22:35:31+00:00", {'
            '"_start_time": "2018-03-07T22:35:31+00:00", '
            '"_end_time": "2018-03-07T22:35:31+00:00", '
            '"events": 1, '
            '"country": "US", '
            '"continent": "North America"'
            '}]') in output_text


def test_write_output_file_with_stream_and_window_bts_provided(tmpdir):
    runner, data = execute_runner('tests/data/stream.yml', 'tests/data/window.yml',
                                  ['tests/data/raw.json'])
    out_dir = tmpdir.join('out')
    runner.write_output_file(str(out_dir), data)
    output_text = get_spark_output(out_dir)
    assert 'last_day.total_events,last_session.events' in output_text
    assert '1,1' in output_text

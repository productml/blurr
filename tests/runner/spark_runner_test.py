from datetime import datetime

from dateutil.tz import tzutc

from blurr.core.store_key import Key
from blurr.runner.spark_runner import SparkRunner


def test_spark_runner_stream_only():
    spark_runner = SparkRunner(['tests/data/raw.json'], 'tests/data/stream.yml', None)
    block_data = {k: v for (k, v) in spark_runner.execute().collect()}

    assert len(block_data) == 7

    # Stream DTC output
    assert block_data[Key('userA', 'session')] == {
        '_identity': 'userA',
        '_start_time': datetime(2018, 3, 7, 23, 35, 31, tzinfo=tzutc()),
        '_end_time': datetime(2018, 3, 7, 23, 35, 32, tzinfo=tzutc()),
        'events': 2,
        'country': 'IN',
        'continent': 'World'
    }

    assert block_data[Key('userA', 'session',
                                        datetime(2018, 3, 7, 22, 35, 31))] == {
        '_identity': 'userA',
        '_start_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()),
        '_end_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()),
        'events': 1,
        'country': 'US',
        'continent': 'North America'
    }  # yapf: disable

    assert block_data[Key('userA', 'state')] == {
        '_identity': 'userA',
        'country': 'IN',
        'continent': 'World'
    }

    assert block_data[Key('userB', 'session')] == {
        '_identity': 'userB',
        '_start_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()),
        '_end_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()),
        'events': 1,
        'country': '',
        'continent': ''
    }


def test_spark_runner_no_vars_stored():
    spark_runner = SparkRunner(['tests/data/raw.json'], 'tests/data/stream.yml', None)
    block_data = {k: v for (k, v) in spark_runner.execute().collect()}

    # Variables should not be stored
    assert Key('userA', 'vars') not in block_data


def test_spark_runner_with_window():
    spark_runner = SparkRunner(['tests/data/raw.json'], 'tests/data/stream.yml',
                               'tests/data/window.yml')
    window_data = spark_runner.execute().collect()

    assert window_data == [{
        'last_session.events': 1,
        'last_session._identity': 'userA',
        'last_day.total_events': 1,
        'last_day._identity': 'userA'
    }]

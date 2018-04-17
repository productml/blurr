import pytest

from blurr.core.record import Record
from blurr.runner.data_processor import SimpleJsonDataProcessor, IpfixDataProcessor


def test_simple_json_processor_success():
    data_processor = SimpleJsonDataProcessor()
    assert data_processor.process_data('{"test": 1}') == [Record({'test': 1})]


def test_simple_json_processor_invalid_json_error():
    data_processor = SimpleJsonDataProcessor()
    with pytest.raises(Exception):
        data_processor.process_data('a')


def test_ipfix_data_processor_success():
    data_processor = IpfixDataProcessor()
    test_data = (
        '{"Other":1,"DataSets":['
        '[{"I":2,"E":1230,"V":"test1"}],'
        '[{"I":56,"V":"aa:aa:aa:aa:aa:aa"},{"I":12,"V":"0.0.0.0"},{"I":182,"V":80},{"I":183,"V":81},{"I":4,"V":6},{"I":150,"V":1522385684}],'
        '[{"I":56,"V":"bb:bb:bb:bb:bb:bb"}]'
        ']}')
    assert data_processor.process_data(test_data) == [
        Record({
            'source_mac': 'aa:aa:aa:aa:aa:aa',
            'dest_ip': '0.0.0.0',
            'source_port': 80,
            'dest_port': 81,
            4: 6,
            'timestamp': 1522385684
        }),
        Record({
            'source_mac': 'bb:bb:bb:bb:bb:bb'
        })
    ]


def test_ipfix_data_processor_invalid_ipfix():
    data_processor = IpfixDataProcessor()
    assert data_processor.process_data('{"test": 1}') == []


def test_ipfix_data_processor_invalid_json_error():
    data_processor = IpfixDataProcessor()
    with pytest.raises(Exception):
        data_processor.process_data('a')
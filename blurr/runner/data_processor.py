import json
from abc import ABC, abstractmethod
from typing import List, Dict

from blurr.core.record import Record


class DataProcessor(ABC):
    @abstractmethod
    def process_data(self, data_string: str) -> List[Record]:
        pass


class SimpleJsonDataProcessor(DataProcessor):
    def process_data(self, data_string: str) -> List[Record]:
        return [Record(json.loads(data_string))]


class SimpleDictionaryDataProcessor(DataProcessor):
    def process_data(self, data_dict: Dict) -> List[Record]:
        return [Record(data_dict)]


class IpfixDataProcessor(DataProcessor):
    IPFIX_EVENT_MAPPER = {
        4: 'protocol',
        12: 'dest_ip',
        56: 'source_mac',
        182: 'source_port',
        183: 'dest_port',
        150: 'timestamp'
    }

    def process_data(self, data_string: str) -> List[Record]:
        data = json.loads(data_string)
        if not isinstance(data, dict):
            return []

        record_list = []
        for data_row in data.get('DataSets', []):
            record = {}
            for event_dict in data_row:
                i = event_dict.get('I', 0)
                record[self.IPFIX_EVENT_MAPPER.get(i, i)] = event_dict['V']
            if self.IPFIX_EVENT_MAPPER[56] in record:
                record_list.append(Record(record))

        return record_list

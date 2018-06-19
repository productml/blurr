from abc import abstractmethod, ABC
from datetime import datetime, timezone
from typing import Any, List, Tuple, Dict

from blurr.core.base import BaseSchema
from blurr.core.store_key import Key, KeyType


class StoreSchema(BaseSchema):
    pass


class Store(ABC):
    """ Base Store that allows for data to be persisted during / after transformation """

    @abstractmethod
    def get_all(self, identity: str) -> Dict[Key, Any]:
        """
        Gets all the items for an identity
        """
        raise NotImplementedError()

    @abstractmethod
    def get(self, key: Key) -> Any:
        """
        Gets an item by key
        """
        raise NotImplementedError()

    def get_range(self,
                  base_key: Key,
                  start_time: datetime,
                  end_time: datetime = None,
                  count: int = 0) -> List[Tuple[Key, Any]]:
        """
        Returns the list of items from the store based on the given time range or count.
        :param base_key: Items which don't start with the base_key are filtered out.
        :param start_time: Start time to for the range query
        :param end_time: End time of the range query. If None count is used.
        :param count: The number of items to be returned. Used if end_time is not specified.
        """
        if end_time and count:
            raise ValueError('Only one of `end` or `count` can be set')

        if count:
            end_time = datetime.min.replace(
                tzinfo=timezone.utc) if count < 0 else datetime.max.replace(tzinfo=timezone.utc)

        end_time = self._add_timezone_if_required(end_time)
        start_time = self._add_timezone_if_required(start_time)

        if end_time < start_time:
            start_time, end_time = end_time, start_time

        if base_key.key_type == KeyType.TIMESTAMP:
            start_key = Key(KeyType.TIMESTAMP, base_key.identity, base_key.group, [], start_time)
            end_key = Key(KeyType.TIMESTAMP, base_key.identity, base_key.group, [], end_time)
            return self._get_range_timestamp_key(start_key, end_key, count)
        else:
            return self._get_range_dimension_key(base_key, start_time, end_time, count)

    @abstractmethod
    def _get_range_timestamp_key(self, start: Key, end: Key,
                                 count: int = 0) -> List[Tuple[Key, Any]]:
        """
        Returns the list of items from the store based on the given time range or count.

        This is used when the key being used is a TIMESTAMP key.
        """
        raise NotImplementedError()

    def get_time_range(self, identity, group, start_time, end_time) -> List[Tuple[Key, Any]]:
        raise NotImplementedError()

    def get_count_range(self, identity, group, time, count):
        raise NotImplementedError()

    @abstractmethod
    def _get_range_dimension_key(self,
                                 base_key: Key,
                                 start_time: datetime,
                                 end_time: datetime,
                                 count: int = 0) -> List[Tuple[Key, Any]]:
        """
        Returns the list of items from the store based on the given time range or count.

        This is used when the key being used is a DIMENSION key.
        """
        raise NotImplementedError()

    @staticmethod
    def _restrict_items_to_count(items: List[Tuple[Key, Any]], count: int) -> List[Tuple[Key, Any]]:
        """
        Restricts items to count number if len(items) is larger than abs(count). This function
        assumes that items is sorted by time.

        :param items: The items to restrict.
        :param count: The number of items returned.
        """
        if abs(count) > len(items):
            count = Store._sign(count) * len(items)

        if count < 0:
            return items[count:]
        else:
            return items[:count]

    @abstractmethod
    def save(self, key: Key, item: Any) -> None:
        """
        Saves an item to store
        """
        raise NotImplementedError()

    @abstractmethod
    def delete(self, key: Key) -> None:
        """
        Deletes an item from the store by key
        """
        raise NotImplementedError()

    @abstractmethod
    def finalize(self) -> None:
        """
        Finalizes the store by flushing all remaining data to persistence
        """
        raise NotImplementedError()

    @staticmethod
    def _sign(x: int) -> int:
        return (1, -1)[x < 0]

    @staticmethod
    def _add_timezone_if_required(time: datetime) -> datetime:
        if time.tzinfo is None or time.tzinfo.utcoffset(time) is None:
            time = time.replace(tzinfo=timezone.utc)

        return time

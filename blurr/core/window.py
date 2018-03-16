from datetime import datetime
from typing import Dict, Any, List

from blurr.core.base import BaseSchema
from blurr.core.session_data_group import SessionDataGroup
from blurr.core.store import Store


class WindowSchema(BaseSchema):
    ATTRIBUTE_VALUE = 'Value'
    ATTRIBUTE_SOURCE = 'Source'

    def __init__(self, spec: Dict[str, Any]) -> None:
        # Inject 'source' window name if not present
        if self.ATTRIBUTE_NAME not in spec:
            spec[self.ATTRIBUTE_NAME] = 'source'
        super().__init__(spec)

    def load(self, spec: Dict[str, Any]) -> None:
        self.value = spec[self.ATTRIBUTE_VALUE]
        self.source = spec[self.ATTRIBUTE_SOURCE]

    def validate(self, spec: Dict[str, Any]) -> None:
        self.validate_required_attribute(spec, self.ATTRIBUTE_VALUE)
        self.validate_required_attribute(spec, self.ATTRIBUTE_SOURCE)


class Window:
    def __init__(self, schema: WindowSchema) -> None:
        self.schema = schema
        self.view: List[SessionDataGroup] = []

    def prepare(self, store: Store, identity: str,
                start_time: datetime) -> None:
        if self.schema.type == 'day' or self.schema.type == 'hour':
            self.view = store.get_window_by_time(identity, self.schema.source,
                                                 start_time,
                                                 self.get_end_time(start_time))
        else:
            self.view = store.get_window_by_count(
                identity, self.schema.source, start_time, self.schema.value)

    def get_end_time(self, start_time: datetime) -> datetime:
        pass

    def __getattr__(self, item: str) -> List[Any]:
        return [getattr(session, item) for session in self.view]

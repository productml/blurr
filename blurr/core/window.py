from datetime import datetime, timedelta
from typing import Dict, Any, List

from blurr.core.base import BaseSchema, BaseItem
from blurr.core.evaluation import EvaluationContext
from blurr.core.loader import TypeLoader
from blurr.core.schema_loader import SchemaLoader
from blurr.core.session_data_group import SessionDataGroup
from blurr.core.store import Store, Key


class WindowSchema(BaseSchema):
    ATTRIBUTE_VALUE = 'Value'
    ATTRIBUTE_SOURCE = 'Source'

    def load(self) -> None:
        self.value = self._spec[self.ATTRIBUTE_VALUE]
        self.source = self.schema_loader.get_schema_object(
            self._spec[self.ATTRIBUTE_SOURCE])

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
            self.view = self._load_sessions(
                store.get_range(
                    Key(identity, self.schema.source.name, start_time),
                    Key(identity, self.schema.source.name,
                        self.get_end_time(start_time))))
        else:
            self.view = self._load_sessions(
                store.get_range(
                    Key(identity, self.schema.source.name, start_time), None,
                    self.schema.value))

    def get_end_time(self, start_time: datetime) -> datetime:
        if self.schema.type == 'day':
            return start_time + timedelta(days=self.schema.value)
        elif self.schema.type == 'hour':
            return start_time + timedelta(hours=self.schema.value)

    def _load_sessions(self, sessions: List[Any]) -> List[BaseItem]:
        return [
            SessionDataGroup(self.schema.source, EvaluationContext()).restore(
                session[1]) for session in sessions
        ]

    def __getattr__(self, item: str) -> List[Any]:
        return [getattr(session, item) for session in self.view]

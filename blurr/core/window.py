from datetime import datetime, timedelta
from typing import Any, List, Tuple

from blurr.core.base import BaseSchema, BaseItem
from blurr.core.errors import PrepareWindowMissingSessionsError
from blurr.core.evaluation import EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.session_data_group import SessionDataGroup
from blurr.core.store import Key


class WindowSchema(BaseSchema):
    """
    Represents the schema for the window to be created on the pre-aggregated
    source data.
    """
    ATTRIBUTE_VALUE = 'Value'
    ATTRIBUTE_SOURCE = 'Source'

    def __init__(self, fully_qualified_name: str,
                 schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        self.value = self._spec[self.ATTRIBUTE_VALUE]
        self.source = self.schema_loader.get_schema_object(
            self._spec[self.ATTRIBUTE_SOURCE])


class Window:
    """
    Generates a window view on the pre-aggregated source data.
    Does not inherit from BaseItem as a window is used for setting up the evaluation
    context and does not directly participate in the evaluation path.
    """

    def __init__(self, schema: WindowSchema) -> None:
        self.schema = schema
        self.view: List[SessionDataGroup] = []

    def prepare(self, identity: str, start_time: datetime) -> None:
        """
        Prepares the window view on the source data.
        :param store: Store to be used to query for the source data.
        :param identity: Identity is used as a Key for store query.
        :param start_time: The Anchor session start_time from where the window
        should be generated.
        :return: None
        """
        store = self.schema.source.store
        if self.schema.type == 'day' or self.schema.type == 'hour':
            self.view = self._load_sessions(  # type: ignore
                store.get_range(
                    Key(identity, self.schema.source.name, start_time),
                    Key(identity, self.schema.source.name,
                        self._get_end_time(start_time))), identity)
        else:
            self.view = self._load_sessions(  # type: ignore
                store.get_range(
                    Key(identity, self.schema.source.name, start_time), None,
                    self.schema.value), identity)

        self._validate_view()

    def _validate_view(self) -> None:
        if self.schema.type == 'count' and len(self.view) != abs(
                self.schema.value):
            raise PrepareWindowMissingSessionsError(
                'Expecting {} but not found {} sessions'.format(
                    abs(self.schema.value), len(self.view)))

        if len(self.view) == 0:
            raise PrepareWindowMissingSessionsError(
                'No matching sessions found')

    # TODO: Handle end time which is beyond the expected range of data being
    # processed. In this case a PrepareWindowMissingSessionsError error should
    # be raised.
    def _get_end_time(self, start_time: datetime) -> datetime:
        """
        Generates the end time to be used for the store range query.
        :param start_time: Start time to use as an offset to calculate the end time
        based on the window schema.
        :return:
        """
        if self.schema.type == 'day':
            return start_time + timedelta(days=self.schema.value)
        elif self.schema.type == 'hour':
            return start_time + timedelta(hours=self.schema.value)
        else:
            raise ValueError(
                "invalid schema type: " +
                self.schema.type)  # TODO: test case for this invalid input

    def _load_sessions(self, sessions: List[Tuple[Key, Any]],
                       identity: str) -> List[BaseItem]:
        """
        Converts [(Key, Session)] to [SessionDataGroup]
        :param sessions: List of (Key, Session) sessions.
        :return: List of SessionDataGroup
        """
        return [
            SessionDataGroup(self.schema.source, identity,
                             EvaluationContext()).restore(session)
            for (_, session) in sessions
        ]

    def __getattr__(self, item: str) -> List[Any]:
        return [getattr(session, item) for session in self.view]

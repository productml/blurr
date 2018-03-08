from blurr.core.context import Context
from blurr.core.group import Group, GroupSchema


PREDEFINED_SESSION_FIELDS = [
    {'Name': 'start_time', 'Type': 'datetime', 'Value': 'time if start_time is None else time if time < start_time else start_time'},
    {'Name': 'end_time', 'Type': 'datetime', 'Value': 'time if end_time is None else time if time > end_time else end_time'},
]


class SessionGroupSchema(GroupSchema):
    def __init__(self, schema: dict) -> None:
        self.split = schema['Split']
        schema['Fields'][0:0] = PREDEFINED_SESSION_FIELDS
        print(schema['Fields'])
        super(SessionGroupSchema, self).__init__(schema)


class StaleSessionError(Exception):
    pass


class SessionGroup(Group):
    def __init__(self, schema: SessionGroupSchema, global_context: Context) -> None:
        super(SessionGroup, self).__init__(schema, global_context)

    def evaluate(self) -> None:
        if self.start_time is not None and self.end_time is not None:
            if not self._schema.split or self.evaluate_expr(self._schema.split):
                raise StaleSessionError()

        super(SessionGroup, self).evaluate()

    def split(self):
        pass
        # TODO Flush the current session to store

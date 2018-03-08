from blurr.core.interpreter import Interpreter
from blurr.core.group import Group, GroupSchema


class SessionGroupSchema(GroupSchema):
    def __init__(self, schema: dict) -> None:
        self.split = schema['Split']
        super(SessionGroupSchema, self).__init__(schema)


class StaleSessionError(Exception):
    pass


class SessionGroup(Group):
    def __init__(self, schema: SessionGroupSchema) -> None:
        super(SessionGroup, self).__init__(schema)

    def evaluate(self, interpreter: Interpreter) -> None:
        if not self._schema.split or interpreter.evaluate(self._schema.split):
            raise StaleSessionError()

        super(SessionGroup, self).evaluate(interpreter)

    def split(self):
        pass
        # TODO Flush the current session to store

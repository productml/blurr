from core.Interpreter import Interpreter
from core.SessionGroupSchema import SessionGroupSchema
from core.Group import Group


class StaleSessionError(Exception):
    pass


class SessionGroup(Group):
    def __init__(self, schema: SessionGroupSchema) -> None:
        super(SessionGroup, self).__init__(schema)

    def evaluate(self, interpreter: Interpreter) -> None:
        if not self._schema.split or interpreter.evaluate(self._schema.split):
            raise StaleSessionError()

        super(SessionGroup, self).evaluate(interpreter)

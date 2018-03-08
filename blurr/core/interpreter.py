

class Interpreter:
    def __init__(self, global_context, local_context):
        self._global_context = global_context
        self._local_context = local_context

    # TODO Each interpreter should maintain execution context (globals and locals)
    # There is one interpreter state per identity.
    def evaluate(self, statement):
        return eval(statement, self._global_context, self._local_context)

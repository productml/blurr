

class Interpreter:
    # TODO Each interpreter should maintain execution context (globals and locals)
    # There is one interpreter state per identity.
    def evaluate(self, statement):
        return eval(statement)

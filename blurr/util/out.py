import sys


class Out:
    def print(self, s):
        print(s, file=sys.stdout)

    def eprint(self, s):
        print(s, file=sys.stderr)


class OutStub:
    def __init__(self):
        self._stdout = ""
        self._stderr = ""

    def print(self, s):
        self._stdout += s + "\n"

    def eprint(self, s):
        self._stderr += s + "\n"

    @property
    def stdout(self):
        print("Getting value")
        return self._stdout

    @property
    def stderr(self):
        return self._stderr

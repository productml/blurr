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

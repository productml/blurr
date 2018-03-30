from blurr.cli.out import Out


class OutStub(Out):
    def __init__(self):
        self._stdout = ""
        self._stderr = ""

    def print(self, *args):
        self._stdout += ' '.join(args) + "\n"

    def eprint(self, *args):
        self._stderr += ' '.join(args) + "\n"

    @property
    def stdout(self):
        return self._stdout

    @property
    def stderr(self):
        return self._stderr

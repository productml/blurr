class OutStub:
    def __init__(self) -> None:
        self._stdout = ""
        self._stderr = ""

    def print(self, s: str) -> None:
        self._stdout += s + "\n"

    def eprint(self, s: str) -> None:
        self._stderr += s + "\n"

    @property
    def stdout(self) -> str:
        print("Getting value")
        return self._stdout

    @property
    def stderr(self) -> str:
        return self._stderr

import sys


class Out:
    def print(self, s: str) -> None:
        print(s, file=sys.stdout)

    def eprint(self, s: str) -> None:
        print(s, file=sys.stderr)

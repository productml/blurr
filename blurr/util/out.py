import sys


class Out:
    def print(self, s):
        print(s, file=sys.stdout)

    def eprint(self, s):
        print(s, file=sys.stderr)

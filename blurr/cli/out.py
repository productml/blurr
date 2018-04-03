import sys


class Out:
    def print(self, *args):
        print(*args, file=sys.stdout)

    def eprint(self, *args):
        print(*args, file=sys.stderr)

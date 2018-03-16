from __future__ import print_function
import sys


def eprint(*args, **kwargs):
    """
       Same as print(), but prints output to stderr instead of stdout
    """
    print(*args, file=sys.stderr, **kwargs)

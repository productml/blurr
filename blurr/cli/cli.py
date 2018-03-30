from typing import Dict, Any

from blurr.cli.validate import validate_command
from blurr.util.out import Out


def cli(arguments: Dict[str, Any], out: Out) -> int:
    if arguments['validate']:
        return validate_command(arguments['<DTC>'], out)
    else:
        return 1

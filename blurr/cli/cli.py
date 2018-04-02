from typing import Dict, Any

from blurr.cli.transform import transform
from blurr.cli.validate import validate_command
from blurr.cli.out import Out


def cli(arguments: Dict[str, Any], out: Out) -> int:
    if arguments['validate']:
        return validate_command(arguments['<DTC>'], out)
    elif arguments['transform']:
        return transform(arguments['--streaming-dtc'],
                         arguments['--window-dtc'],
                         arguments['<raw-json-files>'], out)

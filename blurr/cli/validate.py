from typing import List, Optional

import yaml

from blurr.cli.out import Out
from blurr.cli.util import get_yml_files
from blurr.core.errors import InvalidSchemaError
from blurr.core.syntax.schema_validator import validate


def validate_command(dtc_files: List[str], out: Optional[Out] = None) -> int:
    all_files_valid = True
    if len(dtc_files) == 0:
        dtc_files = get_yml_files()
    for dtc_file in dtc_files:
        if out:
            out.print('Running syntax validation on', dtc_file)
        if validate_file(dtc_file, out) == 1:
            all_files_valid = False

    return 0 if all_files_valid else 1


def validate_file(dtc_file: str, out: Optional[Out] = None) -> int:
    try:
        dtc_dict = yaml.safe_load(open(dtc_file))
        validate(dtc_dict)
        if out:
            out.print('document is valid')
        return 0
    except yaml.YAMLError:
        if out:
            out.eprint('invalid yaml')
        return 1
    except InvalidSchemaError as err:
        if out:
            out.eprint(str(err))
        return 1
    except:
        if out:
            out.eprint('there was an error parsing the document')
        return 1


def get_valid_yml_files(yml_files: List[str]) -> List[str]:
    return [yml_file for yml_file in yml_files if validate_file(yml_file) == 0]

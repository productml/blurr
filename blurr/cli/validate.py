import sys
from typing import List

import yaml

from blurr.cli.util import get_yml_files, eprint
from blurr.core import logging
from blurr.core.errors import InvalidSchemaError
from blurr.core.syntax.schema_validator import validate


def validate_command(dtc_files: List[str]) -> int:
    all_files_valid = True
    if len(dtc_files) == 0:
        dtc_files = get_yml_files()
    for dtc_file in dtc_files:
        if validate_file(dtc_file) == 1:
            all_files_valid = False

    return 0 if all_files_valid else 1


def validate_file(dtc_file: str) -> int:
    print('Running syntax validation on {}'.format(dtc_file))
    try:
        dtc_dict = yaml.safe_load(open(dtc_file, 'r', encoding='utf-8'))
        validate(dtc_dict)
        print('Document is valid')
        return 0
    except yaml.YAMLError as err:
        eprint('Invalid yaml')
        eprint(str(err))
        return 1
    except InvalidSchemaError as err:
        eprint(str(err))
        return 1
    except:
        exception_value = sys.exc_info()[1]
        logging.error(exception_value)
        eprint('There was an error parsing the document')
        return 1


def get_valid_yml_files(yml_files: List[str]) -> List[str]:
    return [yml_file for yml_file in yml_files if validate_file(yml_file) == 0]

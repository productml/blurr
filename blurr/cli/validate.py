import sys
from typing import List, Any, Dict

import yaml

from blurr.cli.util import get_yml_files, eprint
from blurr.core import logging
from blurr.core.errors import SchemaError, InvalidSpecError
from blurr.core.schema_loader import SchemaLoader


def validate_command(dtc_files: List[str]) -> int:
    all_files_valid = True
    if len(dtc_files) == 0:
        dtc_files = get_yml_files()
    for dtc_file in dtc_files:
        if validate_file(dtc_file) == 1:
            all_files_valid = False

    return 0 if all_files_valid else 1


def validate_file(dtc_file: str) -> int:
    print('Running validation on {}'.format(dtc_file))
    try:
        dtc_dict = yaml.safe_load(open(dtc_file, 'r', encoding='utf-8'))
        validate(dtc_dict)
        print('Document is valid')
        return 0
    except yaml.YAMLError as err:
        eprint('Invalid yaml')
        eprint(str(err))
        return 1
    except SchemaError as err:
        eprint(str(err))
        return 1
    except Exception as err:
        exception_value = sys.exc_info()[1]
        logging.error(exception_value)
        eprint('There was an error parsing the document. Error:\n' + str(err))
        return 1


def validate(spec: Dict[str, Any]) -> None:
    schema_loader = SchemaLoader()
    dtc_name = schema_loader.add_schema_spec(spec)
    if not dtc_name:
        raise InvalidSpecError(spec)
    schema_loader.raise_errors()
    schema_loader.get_schema_object(dtc_name)
    print(schema_loader.get_errors())
    schema_loader.raise_errors()


def get_valid_yml_files(yml_files: List[str]) -> List[str]:
    return [yml_file for yml_file in yml_files if validate_file(yml_file) == 0]

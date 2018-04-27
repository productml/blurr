import re
from typing import Dict, Any

from blurr.core.errors import InvalidSchemaError

VALIDATOR_IDENTITY_REGEX = re.compile(r'^_|[^\S]')

ATTRIBUTE_NAME = 'Name'
ATTRIBUTE_TYPE = 'Type'
ATTRIBUTE_INTERNAL = '_Internal'


def validate_identity(name, spec, attribute):
    if VALIDATOR_IDENTITY_REGEX.findall(spec[attribute]):
        raise InvalidSchemaError(('`{attribute}: {attribute_value}` in section `{name}` is invalid. '
                                  '`{attribute}` must not start with `_` '
                                  'and must be a python valid variable name.').format(
            attribute=attribute,
            attribute_value=spec[attribute],
            name=name))


def validate_required(name, spec: Dict[str, Any], *attributes):
    missing_attributes = [attribute for attribute in attributes if not spec.get(attribute, None)]
    if missing_attributes:
        raise InvalidSchemaError('{attributes} missing in section `{name}`'.format(
            attributes=', '.join(['`' + f + ':`' for f in missing_attributes]), name=name))


def validate_schema_basics(name, spec: Dict[str, Any]):
    # If the spec has been added by Blurr, ignore validation
    if ATTRIBUTE_INTERNAL in spec:
        return

    empty_attributes = [attribute for attribute, value in spec.items() if not value]
    if empty_attributes:
        raise InvalidSchemaError('{attributes} in section `{name}` cannot have an empty value.'.format(
            attributes=', '.join(['`' + f + ':`' for f in empty_attributes]), name=name))

    validate_required(name, spec, ATTRIBUTE_NAME, ATTRIBUTE_TYPE)
    validate_identity(name, spec, ATTRIBUTE_NAME)

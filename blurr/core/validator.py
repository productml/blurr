from typing import Dict, Any, Optional

from blurr.core.errors import InvalidSchemaError, InvalidIdentifierError, RequiredAttributeError, EmptyAttributeError

ATTRIBUTE_NAME = 'Name'
ATTRIBUTE_TYPE = 'Type'
ATTRIBUTE_INTERNAL = '_Internal'


def validate_identifier(fully_qualified_name: str, spec: Dict[str, Any],
                        attribute: str) -> Optional[InvalidIdentifierError]:
    if attribute not in spec:
        return None

    value = spec[attribute]
    reason = None
    if value.startswith('_'):
        reason = InvalidIdentifierError.Reason.STARTS_WITH_UNDERSCORE
    elif not value.isidentifier():
        reason = InvalidIdentifierError.Reason.INVALID_PYTHON_IDENTIFIER

    return InvalidIdentifierError(fully_qualified_name, spec, attribute, reason) if reason else None


def validate_required(fully_qualified_name: str, spec: Dict[str, Any],
                      *attributes: str) -> Optional[InvalidSchemaError]:
    errors = []
    for attribute in attributes:
        if not spec.get(attribute, None):
            errors.append(RequiredAttributeError(fully_qualified_name, spec, attribute))

    return InvalidSchemaError(fully_qualified_name, spec, errors) if errors else None


def validate_schema_basics(fully_qualified_name: str,
                           spec: Dict[str, Any]) -> Optional[InvalidSchemaError]:
    if ATTRIBUTE_INTERNAL in spec:
        return None

    errors = []
    for attribute, value in spec.items():
        if not value:
            errors.append(EmptyAttributeError(fully_qualified_name, spec, attribute))

    required_errors = validate_required(fully_qualified_name, spec, ATTRIBUTE_NAME, ATTRIBUTE_TYPE)
    if required_errors:
        errors = errors + required_errors.errors

    identifier_error = validate_identifier(fully_qualified_name, spec, ATTRIBUTE_NAME)
    if identifier_error:
        errors.append(identifier_error)

    return InvalidSchemaError(fully_qualified_name, spec, errors) if errors else None

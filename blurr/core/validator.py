from typing import Dict, Any, Type, Optional, Union

from blurr.core.errors import InvalidIdentifierError, RequiredAttributeError, EmptyAttributeError, \
    SchemaErrorCollection, InvalidNumberError

ATTRIBUTE_NAME = 'Name'
ATTRIBUTE_TYPE = 'Type'
ATTRIBUTE_INTERNAL = '_Internal'


def validate_identifier(fully_qualified_name: str, spec: Dict[str, Any],
                        *attributes: str) -> SchemaErrorCollection:
    """ Validates a set of attributes as identifiers in a spec """
    errors = SchemaErrorCollection()
    for attribute in attributes:
        if attribute not in spec:
            continue

        value = spec[attribute]

        if value.startswith('_'):
            errors.add(
                InvalidIdentifierError(fully_qualified_name, spec, attribute,
                                       InvalidIdentifierError.Reason.STARTS_WITH_UNDERSCORE))
        elif not value.isidentifier():
            errors.add(
                InvalidIdentifierError(fully_qualified_name, spec, attribute,
                                       InvalidIdentifierError.Reason.INVALID_PYTHON_IDENTIFIER))

    return errors


def validate_required(fully_qualified_name: str, spec: Dict[str, Any],
                      *attributes: str) -> SchemaErrorCollection:
    """ Validates to ensure that a set of attributes are present in spec """
    errors = SchemaErrorCollection()
    for attribute in attributes:
        if not spec.get(attribute, None):
            errors.add(RequiredAttributeError(fully_qualified_name, spec, attribute))

    return errors


def validate_number(fully_qualified_name: str,
                    spec: Dict[str, Any],
                    attribute: str,
                    value_type: Union[Type[int], Type[float]] = int,
                    minimum: Optional[Union[int, float]] = None,
                    maximum: Optional[Union[int, float]] = None) -> Optional[InvalidNumberError]:
    if attribute not in spec:
        return

    try:
        value = value_type(spec[attribute])
        if (minimum and value < minimum) or (maximum and value > maximum):
            raise Exception()
    except:
        return InvalidNumberError(fully_qualified_name, spec, attribute, value_type, minimum,
                                  maximum)


def validate_schema_basics(fully_qualified_name: str,
                           spec: Dict[str, Any]) -> SchemaErrorCollection:
    """
    Validates the basic requirements of a schema block
        1. Items may not have empty value
        2. `Name` and `Type` must be defined
        3. `Name` must have a value that passes as a valid python identity and must not start with '_'

    :param fully_qualified_name: Fully qualified name of the block
    :param spec: The spec of the schema block being validated
    :return: A collection of validation errors that have been found.  If no errors are found,
                and empty collection is returned
    """
    errors = SchemaErrorCollection()

    if ATTRIBUTE_INTERNAL not in spec:
        errors.add([
            EmptyAttributeError(fully_qualified_name, spec, attribute)
            for attribute, value in spec.items()
            if not value
        ])
        errors.add(validate_required(fully_qualified_name, spec, ATTRIBUTE_NAME, ATTRIBUTE_TYPE))
        errors.add(validate_identifier(fully_qualified_name, spec, ATTRIBUTE_NAME))

    return errors

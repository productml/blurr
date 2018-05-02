from typing import Dict, Any, Type, Optional, Union, List, Tuple, Callable

from blurr.core.errors import InvalidIdentifierError, RequiredAttributeError, EmptyAttributeError, \
    SchemaErrorCollection, InvalidNumberError, InvalidSchemaError

ATTRIBUTE_NAME = 'Name'
ATTRIBUTE_TYPE = 'Type'
ATTRIBUTE_INTERNAL = '_Internal'


def validate_python_identifier_attributes(fully_qualified_name: str, spec: Dict[str, Any],
                                          *attributes: str) -> List[InvalidIdentifierError]:
    """ Validates a set of attributes as identifiers in a spec """
    errors: List[InvalidIdentifierError] = []

    checks: List[Tuple[Callable, InvalidIdentifierError.Reason]] = [
        (lambda x: x.startswith('_'), InvalidIdentifierError.Reason.STARTS_WITH_UNDERSCORE),
        (lambda x: x.startswith('run_'), InvalidIdentifierError.Reason.STARTS_WITH_RUN),
        (lambda x: not x.isidentifier(), InvalidIdentifierError.Reason.INVALID_PYTHON_IDENTIFIER),
    ]

    for attribute in attributes:
        if attribute not in spec:
            continue

        for check in checks:
            if check[0](spec[attribute]):
                errors.append(InvalidIdentifierError(fully_qualified_name, spec, attribute, check[1]))
                break

    return errors


def validate_required_attributes(fully_qualified_name: str, spec: Dict[str, Any],
                                 *attributes: str) -> SchemaErrorCollection:
    """ Validates to ensure that a set of attributes are present in spec """
    errors = SchemaErrorCollection()
    for attribute in attributes:
        if not spec.get(attribute, None):
            errors.add(RequiredAttributeError(fully_qualified_name, spec, attribute))

    return errors


def validate_number_attribute(fully_qualified_name: str,
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
        errors.add(validate_required_attributes(fully_qualified_name, spec, ATTRIBUTE_NAME, ATTRIBUTE_TYPE))
        errors.add(validate_python_identifier_attributes(fully_qualified_name, spec, ATTRIBUTE_NAME))

    return errors

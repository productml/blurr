from typing import Dict, Any, Type, Optional, Union, List, Tuple, Callable, Set

from blurr.core.errors import InvalidIdentifierError, RequiredAttributeError, EmptyAttributeError, \
    InvalidNumberError, InvalidValueError

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
        if attribute not in spec or spec.get(ATTRIBUTE_INTERNAL, False):
            continue

        for check in checks:
            if check[0](spec[attribute]):
                errors.append(
                    InvalidIdentifierError(fully_qualified_name, spec, attribute, check[1]))
                break

    return errors


def validate_required_attributes(fully_qualified_name: str, spec: Dict[str, Any],
                                 *attributes: str) -> List[RequiredAttributeError]:
    """ Validates to ensure that a set of attributes are present in spec """
    return [
        RequiredAttributeError(fully_qualified_name, spec, attribute)
        for attribute in attributes
        if attribute not in spec
    ]


def validate_empty_attributes(fully_qualified_name: str, spec: Dict[str, Any],
                              *attributes: str) -> List[EmptyAttributeError]:
    """ Validates to ensure that a set of attributes do not contain empty values """
    return [
        EmptyAttributeError(fully_qualified_name, spec, attribute)
        for attribute in attributes
        if not spec.get(attribute, None)
    ]


def validate_number_attribute(
        fully_qualified_name: str,
        spec: Dict[str, Any],
        attribute: str,
        value_type: Union[Type[int], Type[float]] = int,
        minimum: Optional[Union[int, float]] = None,
        maximum: Optional[Union[int, float]] = None) -> Optional[InvalidNumberError]:
    """ Validates to ensure that the value is a number of the specified type, and lies with the specified range """

    if attribute not in spec:
        return

    try:
        value = value_type(spec[attribute])
        if (minimum is not None and value < minimum) or (maximum is not None and value > maximum):
            raise None
    except:
        return InvalidNumberError(fully_qualified_name, spec, attribute, value_type, minimum,
                                  maximum)


def validate_enum_attribute(fully_qualified_name: str, spec: Dict[str, Any], attribute: str,
                            candidates: Set[Union[str, int, float]]) -> Optional[InvalidValueError]:
    """ Validates to ensure that the value of an attribute lies within an allowed set of candidates """

    if attribute not in spec:
        return

    if spec[attribute] not in candidates:
        return InvalidValueError(fully_qualified_name, spec, attribute, candidates)

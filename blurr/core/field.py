from typing import Any

from abc import ABC, abstractmethod

from blurr.core.base import BaseSchema, BaseItem
from blurr.core.evaluation import Expression, EvaluationContext
from blurr.core.schema_loader import SchemaLoader


class FieldSchema(BaseSchema, ABC):
    """
    An individual field schema.
        1. Field Schema must be defined inside a Group
        2. Contain the attributes:
            a. Name (inherited from base)
            b. Type (inherited from base)
            c. Value (required)
            d. Filter (inherited from base)
    """

    # Field Name Definitions
    ATTRIBUTE_VALUE = 'Value'

    def __init__(self, fully_qualified_name: str,
                 schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)
        self.value: Expression = Expression(self._spec[self.ATTRIBUTE_VALUE])

    @property
    @abstractmethod
    def type_object(self) -> Any:
        """
        Returns the type object the Type represents
        """
        raise NotImplementedError('type_object is required')

    def is_type_of(self, instance: Any) -> bool:
        """
        Checks if instance is of the type
        :param instance: An object instance
        :return: True if the object is of this type, False otherwise
        """
        return isinstance(instance, self.type_object)

    @property
    @abstractmethod
    def default(self) -> Any:
        """
        Returns the default value for this type
        """
        raise NotImplementedError('type_object is required')


class Field(BaseItem, ABC):
    """
    An individual field object responsible for retaining the field value
    """

    def __init__(self, schema: FieldSchema,
                 evaluation_context: EvaluationContext) -> None:
        """
        Initializes the Field with the default for the schema
        :param schema: Field schema
        :param evaluation_context: Context dictionary for evaluation
        """
        super().__init__(schema, evaluation_context)

        # When the field is created, the value is set to the field type default
        self.value = self.schema.default

    def evaluate(self) -> None:
        """
        Overrides the base evaluation to set the value to the evaluation result of the value
        expression in the schema
        """
        new_value = None
        if self.needs_evaluation:
            new_value = self.schema.value.evaluate(self.evaluation_context)

        if new_value is None:
            return

        # Only set the value if it conforms to the field type
        if not self.schema.is_type_of(new_value):
            if self.schema.type_object is str:
                # If the destination field is string, stringify the returned value
                new_value = str(new_value)
            else:
                raise TypeError(
                    'Value expression for {} returned an incompatible type.'.
                    format(self.name))

        self.value = new_value

    @property
    def snapshot(self) -> Any:
        """
        Snapshots the current value of the field
        """
        return self.value

    def restore(self, snapshot) -> None:
        """
        Restores the value of a field from a snapshot
        """
        self.value = snapshot


class ComplexTypeBase(ABC):
    """
    Implements a wrapper for base methods declared in base types such that the current object
    is returned in cases there are no returned values.  This ensures that evaluating the `Value`
    expression for field always returns an object.
    """

    def __getattribute__(self, item):
        """ Overrides the default getattribute to return self"""

        # Resolve the attribute by inspecting the current object
        attribute = super().__getattribute__(item)

        # Return the attribute as-is when it is NOT a function
        if not callable(attribute):
            return attribute

        # Wrap the attribute in a function that changes its return value
        def wrapped_attribute(*args, **kwargs):

            # Executing the underlying method
            result = attribute(*args, **kwargs)

            # If the execution does not return a value
            if result is None:
                return self

            # Get the type of the current object
            self_type = type(self)

            # If the method executed is defined in the base type and a base type object is returned
            # (and not the current type), then wrap the base object into an object of the current type
            if isinstance(result, self_type.__bases__) and not isinstance(
                    result, self_type):
                return self_type(result)
                # TODO This creates a shallow copy of the object - find a way to optimize this

            # Return the result as-is on all other conditions
            return result

        # Return the wrapped attribute instead of the default
        return wrapped_attribute

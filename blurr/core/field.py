from abc import ABC, abstractmethod
from typing import Any

from blurr.core import logging
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

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        self.value: Expression = self.build_expression(self.ATTRIBUTE_VALUE)

    def validate_schema_spec(self) -> None:
        super().validate_schema_spec()
        self.validate_required_attributes(self.ATTRIBUTE_VALUE)

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

    @staticmethod
    def encoder(value: Any) -> Any:
        return value

    # TODO: Add unit tests for the casting being done here.
    def decoder(self, value: Any) -> Any:
        return self.type_object(value)


class Field(BaseItem):
    """
    An individual field object responsible for retaining the field value
    """

    def __init__(self, schema: FieldSchema, evaluation_context: EvaluationContext) -> None:
        """
        Initializes the Field with the default for the schema
        :param schema: Field schema
        :param evaluation_context: Context dictionary for evaluation
        """
        super().__init__(schema, evaluation_context)

        # When the field is created, the value is set to the field type default
        self.value = self._schema.default
        self.eval_error = False

    def run_evaluate(self) -> None:
        """
        Overrides the base evaluation to set the value to the evaluation result of the value
        expression in the schema
        """
        result = None
        self.eval_error = False
        if self._needs_evaluation:
            result = self._schema.value.evaluate(self._evaluation_context)

        self.eval_error = result is None
        if self.eval_error:
            return

        # Only set the value if it conforms to the field type
        if not self._schema.is_type_of(result):
            try:
                result = self._schema.type_object(result)
            except Exception as err:
                logging.debug('{} in casting {} to {} for field {}. Error: {}'.format(
                    type(err).__name__, result, self._schema.type,
                    self._schema.fully_qualified_name, err))
                self.eval_error = True
                return

        self.value = result

    @property
    def _snapshot(self) -> Any:
        """
        Snapshots the current value of the field
        """
        return self._schema.encoder(self.value)

    def run_restore(self, snapshot) -> None:
        """
        Restores the value of a field from a snapshot
        """
        self.value = self._schema.decoder(snapshot)

    def run_reset(self) -> None:
        self.value = self._schema.default


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
        if not callable(attribute) or (item.startswith('__') and item.endswith('__')):
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
            if isinstance(result, self_type.__bases__) and not isinstance(result, self_type):
                return self_type(result)
                # TODO This creates a shallow copy of the object - find a way to optimize this

            # Return the result as-is on all other conditions
            return result

        # Return the wrapped attribute instead of the default
        return wrapped_attribute

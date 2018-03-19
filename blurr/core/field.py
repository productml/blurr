from abc import ABC, abstractmethod
from typing import Dict, Any

from blurr.core.base import BaseSchema, BaseItem
from blurr.core.evaluation import Expression, EvaluationContext


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

    def validate(self, spec: Dict[str, Any]) -> None:
        self.validate_required_attribute(spec, self.ATTRIBUTE_VALUE)

    def load(self) -> None:
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

        # Only set the value if it conforms to the field type
        if not self.schema.is_type_of(new_value):
            raise TypeError(
                'Value expression for "{}" returned an incompatible type.',
                self.name)

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

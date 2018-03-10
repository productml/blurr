from typing import Dict, Any

from blurr.core.base import BaseSchema, BaseItem
from blurr.core.evaluation import Context, Expression
from abc import ABC, abstractmethod


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

    def __init__(self, spec: Dict[str, Any]) -> None:
        super().__init__(spec)

    def validate(self, spec: Dict[str, Any]) -> None:
        self.validate_required_attribute(spec, self.ATTRIBUTE_VALUE)

    def load(self, spec: Dict[str, Any]) -> None:
        self.value: Expression = Expression(spec[self.ATTRIBUTE_VALUE])

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


class Field(BaseItem):
    """
    An individual field object responsible for retaining the field value
    """

    def __init__(self, schema: FieldSchema, global_context: Context,
                 local_context: Context) -> None:
        super().__init__(schema, global_context, local_context)

        # When the field is created, the value is set to the field type default
        self.value = self.schema.default

    def initialize(self, value) -> None:
        """
        Initialization is invoked when a previous field state is restored by the parent group
        :param value: Initial value of the field
        """
        self.value = value

    def evaluate(self) -> None:
        """
        Overrides the base evaluation to set the value to the evaluation result of the value
        expression in the schema
        """
        new_value = None
        if self.needs_evaluation:
            new_value = self.schema.value.evaluate(self.global_context,
                                                   self.local_context)

        # Only set the value if it conforms to the field type
        if not self.schema.is_type_of(new_value):
            raise TypeError(
                'Value expression for "{}" returned an incompatible type.',
                self.name)

        self.value = new_value

    @property
    def snapshot(self):
        return self.value

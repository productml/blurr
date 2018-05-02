from typing import Dict, Optional

from blurr.core.aggregate_block import BlockAggregate
from blurr.core.aggregate_window import WindowAggregate
from blurr.core.anchor import Anchor
from blurr.core.errors import AnchorBlockNotDefinedError
from blurr.core.evaluation import Context, EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.transformer import Transformer, TransformerSchema
from blurr.core.type import Type


class WindowTransformerSchema(TransformerSchema):
    """
    Represents the schema for processing aggregated data using windows.
    Handles attributes specific to the window DTC schema
    """

    ATTRIBUTE_ANCHOR = 'Anchor'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        # TODO Improve this validation in the text round.
        try:
            self.anchor = self.schema_loader.get_schema_object(
                self.fully_qualified_name + '.anchor')
        except:
            self.anchor = None

    def validate_schema_spec(self) -> None:
        super().validate_schema_spec()
        self.validate_required_attributes(self.ATTRIBUTE_ANCHOR)

    def extend_schema_spec(self) -> None:
        super().extend_schema_spec()
        if not self.errors:
            self._spec[self.ATTRIBUTE_ANCHOR][self.ATTRIBUTE_NAME] = 'anchor'
            self._spec[self.ATTRIBUTE_ANCHOR][self.ATTRIBUTE_TYPE] = Type.ANCHOR
            self.schema_loader.add_schema_spec(self._spec[self.ATTRIBUTE_ANCHOR],
                                               self.fully_qualified_name)


class WindowTransformer(Transformer):
    """
    The Window DTC transformer that performs window operations on pre-aggregated
    block data.
    """

    def __init__(self, schema: WindowTransformerSchema, identity: str, context: Context) -> None:
        super().__init__(schema, identity)
        self._evaluation_context.merge(EvaluationContext(context))
        self._anchor = Anchor(schema.anchor)

    def run_evaluate(self, block: BlockAggregate) -> bool:
        """
        Evaluates the anchor condition against the specified block.
        :param block: Block to run the anchor condition against.
        :return: True, if the anchor condition is met, otherwise, False.
        """
        if self._anchor.evaluate_anchor(block, self._evaluation_context):

            try:
                self.run_reset()
                self._evaluation_context.global_add('anchor', block)
                self._evaluate()
                self._anchor.add_condition_met()
                return True
            finally:
                self._evaluation_context.global_remove('anchor')

        return False

    def _evaluate(self):
        if 'anchor' not in self._evaluation_context.global_context or self._anchor.anchor_block is None:
            raise AnchorBlockNotDefinedError()

        if not self._needs_evaluation:
            return

        for item in self._nested_items.values():
            if isinstance(item, WindowAggregate):
                item._prepare_window(self._anchor.anchor_block._start_time)

        super().run_evaluate()

    @property
    def run_flattened_snapshot(self) -> Dict:
        """
        Generates a flattened snapshot where the final key for a field is <aggregate_name>.<field_name>.
        :return: The flattened snapshot.
        """
        snapshot_dict = super()._snapshot

        # Flatten to feature dict
        return self._flatten_snapshot(None, snapshot_dict)

    def _flatten_snapshot(self, prefix: Optional[str], value: Dict) -> Dict:
        flattened_dict = {}
        for k, v in value.items():
            if isinstance(v, dict):
                flattened_dict.update(self._flatten_snapshot(k, v))
            else:
                flattened_dict[prefix + '.' + k if prefix is not None else k] = v

        return flattened_dict

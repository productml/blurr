from typing import Dict, Optional

from blurr.core.anchor import Anchor
from blurr.core.window_data_group import WindowDataGroup
from blurr.core.errors import AnchorBlockNotDefinedError, \
    PrepareWindowMissingBlocksError
from blurr.core.evaluation import Context, EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.block_data_group import BlockDataGroup
from blurr.core.transformer import Transformer, TransformerSchema


class WindowTransformerSchema(TransformerSchema):
    """
    Represents the schema for processing aggregated data using windows.
    Handles attributes specific to the window DTC schema
    """

    ATTRIBUTE_ANCHOR = 'Anchor'

    def __init__(self, fully_qualified_name: str,
                 schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        # Inject name and type as expected by BaseSchema
        self._spec[self.ATTRIBUTE_ANCHOR][self.ATTRIBUTE_NAME] = 'anchor'
        self._spec[self.ATTRIBUTE_ANCHOR][self.ATTRIBUTE_TYPE] = 'anchor'
        self.schema_loader.add_schema(self._spec[self.ATTRIBUTE_ANCHOR],
                                      self.fully_qualified_name)

        self.anchor = self.schema_loader.get_schema_object(
            self.fully_qualified_name + '.anchor')


class WindowTransformer(Transformer):
    """
    The Window DTC transformer that performs window operations on pre-aggregated
    block data.
    """

    def __init__(self, schema: WindowTransformerSchema, identity: str,
                 context: Context) -> None:
        super().__init__(schema, identity, context)
        self.anchor = Anchor(
            schema.anchor, EvaluationContext(global_context=context))

    def evaluate_anchor(self, block: BlockDataGroup) -> bool:
        """
        Evaluates the anchor condition against the specified block.
        :param block: Block to run the anchor condition against.
        :return: True, if the anchor condition is met, otherwise, False.
        """
        # Set up context so that anchor can process the block
        self.evaluation_context.local_context.add(
            block.schema.fully_qualified_name, block)
        if self.anchor.evaluate_anchor(block):

            try:
                self.evaluation_context.global_add('anchor', block)
                self.evaluate()
                self.anchor.add_condition_met()
                return True
            except PrepareWindowMissingBlocksError:
                return False
            finally:
                del self.evaluation_context.global_context['anchor']

        return False

    def evaluate(self):
        if 'anchor' not in self.evaluation_context.global_context or self.anchor.anchor_block is None:
            raise AnchorBlockNotDefinedError()

        if not self.needs_evaluation:
            return

        for item in self.nested_items.values():
            if isinstance(item, WindowDataGroup):
                item.prepare_window(self.anchor.anchor_block.start_time)

        super().evaluate()

    @property
    def flattened_snapshot(self) -> Dict:
        """
        Generates a flattened snapshot where the final key for a field is <data_group_name>.<field_name>.
        :return: The flattened snapshot.
        """
        snapshot_dict = super().snapshot

        # Flatten to feature dict
        return self._flatten_snapshot(None, snapshot_dict)

    def _flatten_snapshot(self, prefix: Optional[str], value: Dict) -> Dict:
        flattened_dict = {}
        for k, v in value.items():
            if isinstance(v, dict):
                flattened_dict.update(self._flatten_snapshot(k, v))
            else:
                flattened_dict[prefix + '.' + k
                               if prefix is not None else k] = v

        return flattened_dict

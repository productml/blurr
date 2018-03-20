from datetime import datetime
from typing import Any, Dict

from blurr.core.anchor import AnchorSchema, Anchor
from blurr.core.anchor_data_group import AnchorDataGroup
from blurr.core.errors import AnchorSessionNotDefinedError
from blurr.core.evaluation import Context, EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.session_data_group import SessionDataGroup
from blurr.core.store import Store
from blurr.core.transformer import Transformer, TransformerSchema


class WindowTransformerSchema(TransformerSchema):
    """
    Represents the schema for processing aggregated data using windows.
    Handles attributes specific to the window DTC schema
    """

    ATTRIBUTE_ANCHOR = 'Anchor'

    def validate(self, spec: Dict[str, Any]) -> None:
        # Ensure that the base validator is invoked
        super().validate(spec)

        # Validate schema specific attributes
        self.validate_required_attribute(spec, self.ATTRIBUTE_ANCHOR)

    def load(self) -> None:
        # Ensure that the base loader is invoked
        super().load()

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
    session data.
    """

    def __init__(self, store: Store, schema: WindowTransformerSchema,
                 identity: str, context: Context) -> None:
        super().__init__(store, schema, identity, context)
        self.anchor = Anchor(
            schema.anchor, EvaluationContext(global_context=context))

    def evaluate_anchor(self, session: SessionDataGroup) -> bool:
        """
        Evaluates the anchor condition against the specified session.
        :param session: Session to run the anchor condition against.
        :return: True, if the anchor condition is met, otherwise, False.
        """
        # Set up context so that anchor can process the session
        if self.anchor.evaluate_anchor(session):
            self.evaluation_context.global_add('anchor', session)
            self.evaluate()
            del self.evaluation_context.global_context['anchor']
            return True

        return False

    def evaluate(self):
        if 'anchor' not in self.evaluation_context.global_context or self.anchor.anchor_session is None:
            raise AnchorSessionNotDefinedError()

        if not self.needs_evaluation:
            return

        for _, item in self.nested_items.items():
            if isinstance(item, AnchorDataGroup):
                item.prepare_window(self.store, self.identity,
                                    self.anchor.anchor_session.start_time)

        super().evaluate()

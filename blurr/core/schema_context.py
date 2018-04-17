from typing import Dict, List

from blurr.core.evaluation import Expression, EvaluationContext, ExpressionType


class SchemaContext:

    ATTRIBUTE_IMPORT_MODULE = 'Module'
    ATTRIBUTE_IMPORT_IDENTIFIERS = 'Identifiers'

    def __init__(self, import_spec: List[Dict]):
        self.import_spec = import_spec
        self.import_statements = self._generate_import_statements()
        self._context = None

    def _generate_import_statements(self) -> List[Expression]:
        import_expression_list = []
        if self.import_spec is None:
            return import_expression_list

        for custom_import in self.import_spec:
            module = custom_import[self.ATTRIBUTE_IMPORT_MODULE]
            identifier_list = custom_import.get(self.ATTRIBUTE_IMPORT_IDENTIFIERS, None)

            statement = ' '.join(['import', module]) if not identifier_list else ' '.join(
                ['from', module, 'import', ','.join(identifier_list)])
            import_expression_list.append(Expression(statement, ExpressionType.EXEC))

        return import_expression_list

    @property
    def context(self) -> EvaluationContext:
        if self._context:
            return self._context
        eval_context = EvaluationContext()
        for import_statement in self.import_statements:
            import_statement.evaluate(eval_context)

        return eval_context

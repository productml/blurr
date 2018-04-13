from typing import Dict, List

from blurr.core.evaluation import Expression, EvaluationContext, ExpressionType


class SchemaContext:

    ATTRIBUTE_IMPORT_MODULE = 'Module'
    ATTRIBUTE_IMPORT_IDENTIFIER = 'Identifier'

    def __init__(self, import_spec: List[Dict]):
        self.import_spec = import_spec
        self.import_statements = self._generate_import_statements()

    def _generate_import_statements(self) -> List[Expression]:
        import_expression_list = []
        if self.import_spec is None:
            return import_expression_list

        for custom_import in self.import_spec:
            module = custom_import[self.ATTRIBUTE_IMPORT_MODULE]
            if self.ATTRIBUTE_IMPORT_IDENTIFIER not in custom_import or not custom_import[self.ATTRIBUTE_IMPORT_IDENTIFIER]:
                import_str = 'import ' + module
                import_expression_list.append(Expression(import_str, ExpressionType.EXEC))
                continue

            import_str = 'from ' + module + ' import '

            for identifier in custom_import[self.ATTRIBUTE_IMPORT_IDENTIFIER]:
                import_expression_list.append(Expression(import_str + identifier, ExpressionType.EXEC))

        return import_expression_list

    @property
    def context(self) -> EvaluationContext:
        eval_context = EvaluationContext()
        for import_statement in self.import_statements:
            import_statement.evaluate(eval_context)

        return eval_context

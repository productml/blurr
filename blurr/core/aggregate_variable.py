from blurr.core.aggregate import Aggregate, AggregateSchema


class VariableAggregateSchema(AggregateSchema):
    def validate_schema_spec(self):
        """ Ignores the store validation requirement of the Aggregate Schema """
        super(AggregateSchema, self).validate_schema_spec()


class VariableAggregate(Aggregate):
    pass

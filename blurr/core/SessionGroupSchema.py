from core.GroupSchema import GroupSchema


class SessionGroupSchema(GroupSchema):
    def __init__(self, schema: dict) -> None:
        self.split = schema['Split']
        super(SessionGroupSchema, self).__init__(schema)


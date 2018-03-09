class Context(dict):
    def add_context(self, name, value):
        self[name] = value

    def merge_context(self, context):
        self.update(context)

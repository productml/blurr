import yaml
import json
from blurr.core.transformer import TransformerSchema, Transformer
from blurr.core.record import Record
from blurr.core.context import Context
from dateutil import parser


class LocalRunner:
    def __init__(self, local_json_files, output_file, stream_dtc_file):
        self._raw_files = local_json_files
        self._output_file = output_file
        self._stream_dtc =  yaml.safe_load(open(stream_dtc_file))
        self._transformer_schema = TransformerSchema(self._stream_dtc)
        self._user_transformer = {}
        self._exec_context = Context()
        self._exec_context.add_context('parser', parser)

    def _consume_record(self, record):
        source_context = Context({'source': record})
        identity = self._transformer_schema.get_identity(source_context)
        user_transformer = self._user_transformer.get(identity, Transformer(self._transformer_schema,identity, self._exec_context))
        user_transformer.set_source_context(source_context)
        user_transformer.evaluate()
        self._user_transformer[identity] = user_transformer

    def _execute_file(self, file):
        with open(file) as f:
            for record in f:
                self._consume_record(Record(json.loads(record)))

    def execute(self):
        for file in self._raw_files:
            self._execute_file(file)


def main():
    pass


if __name__ == "__main__":
    main()
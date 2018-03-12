import yaml
import json
from blurr.core.streaming_transformer import StreamingTransformerSchema, StreamingTransformer
from blurr.core.record import Record
from blurr.core.evaluation import Context
from blurr.store.local_store import LocalStore
from dateutil import parser


class LocalRunner:
    def __init__(self, local_json_files, output_file, stream_dtc_file):
        self._raw_files = local_json_files
        self._output_file = output_file
        self._stream_dtc = yaml.safe_load(open(stream_dtc_file))
        self._transformer_schema = StreamingTransformerSchema(self._stream_dtc)
        self._user_transformer = {}
        self._exec_context = Context()
        self._exec_context.add('parser', parser)

    def _consume_record(self, record):
        source_context = Context({'source': record})
        identity = self._transformer_schema.get_identity(source_context)
        if identity not in self._user_transformer:
            self._user_transformer[identity] = StreamingTransformer(
                LocalStore(), self._transformer_schema, identity,
                self._exec_context, Context())
        user_transformer = self._user_transformer[identity]
        user_transformer.set_source_context(source_context)
        user_transformer.evaluate()

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

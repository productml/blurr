import yaml

from blurr.core.evaluation import Context
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.streaming_transformer import StreamingTransformerSchema
from pyspark.sql import SparkSession
import json
from dateutil import parser
import blurr.runner.identity_runner as identity_runner


def get_per_user_records(
        event_str: str, stream_transformer_schema: StreamingTransformerSchema):
    record = Record(json.loads(event_str))
    source_context = Context({'source': record})
    source_context.add('parser', parser)
    return (stream_transformer_schema.get_identity(source_context),
               (stream_transformer_schema.get_time(source_context), record))


def execute_dtc(user_events, stream_dtc, window_dtc):
    identity = user_events[0]
    events = user_events[1]
    _, window_data = identity_runner.execute_dtc(events, identity, stream_dtc, window_dtc)
    return window_data

spark = SparkSession \
    .builder \
    .appName("Process Data") \
    .getOrCreate()

spark_context = spark.sparkContext

stream_dtc = yaml.safe_load(open('tests/data/stream.yml'))
window_dtc = yaml.safe_load(open('tests/data/window.yml'))

schema_loader = SchemaLoader()
stream_dtc_name = schema_loader.add_schema(stream_dtc)
stream_transformer_schema = StreamingTransformerSchema(stream_dtc_name,
                                                       schema_loader)

per_user_records = spark_context.textFile('tests/data/raw.json').map(
    lambda x: get_per_user_records(x, stream_transformer_schema)).groupByKey().mapValues(list)

output = per_user_records.flatMap(lambda x: execute_dtc(x,stream_dtc, window_dtc))


output.take(5)
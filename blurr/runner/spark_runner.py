"""
Usage:
    spark_runner.py --raw-data=<files> --streaming-dtc=<file> [--window-dtc=<file>] --output-file=<file>
    spark_runner.py (-h | --help)
"""

import json
from datetime import datetime
from typing import List, Optional, Tuple, Any, Dict, Union

import yaml
from dateutil import parser
from docopt import docopt

import blurr.runner.identity_runner as identity_runner
from blurr.core.evaluation import Context
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key
from blurr.core.transformer_streaming import StreamingTransformerSchema
from blurr.core.syntax.schema_validator import validate
from pyspark import RDD, SparkContext
from pyspark.sql import SparkSession


class SparkRunner:
    def __init__(self,
                 json_files: List[str],
                 stream_dtc_file: str,
                 window_dtc_file: Optional[str] = None):
        self._raw_files = json_files

        self._schema_loader = SchemaLoader()
        self._stream_dtc = yaml.safe_load(open(stream_dtc_file))
        self._stream_dtc_name = self._schema_loader.add_schema(self._stream_dtc)
        self._stream_transformer_schema = StreamingTransformerSchema(self._stream_dtc_name,
                                                                     self._schema_loader)

        self._window_dtc = None if window_dtc_file is None else yaml.safe_load(
            open(window_dtc_file))

        self._validate_dtc_syntax()

    def _validate_dtc_syntax(self) -> None:
        validate(self._stream_dtc)
        if self._window_dtc is not None:
            validate(self._window_dtc)

    def execute_per_user_events(self, user_events: Tuple[str, List[Tuple[datetime, Record]]]
                                ) -> Union[Dict[Key, Any], List[Dict]]:
        identity = user_events[0]
        events = user_events[1]
        block_data, window_data = identity_runner.execute_dtc(events, identity, self._stream_dtc,
                                                              self._window_dtc)
        if self._window_dtc is None:
            return block_data
        else:
            return window_data

    def get_per_user_records(self, event_str: str) -> Tuple[str, Tuple[datetime, Record]]:
        record = Record(json.loads(event_str))
        source_context = Context({'source': record})
        source_context.add('parser', parser)
        return (self._stream_transformer_schema.get_identity(source_context),
                (self._stream_transformer_schema.get_time(source_context), record))

    def execute(self, spark_context: SparkContext) -> RDD:
        raw_records = spark_context.union(
            [spark_context.textFile(file) for file in self._raw_files])
        per_user_records = raw_records.map(
            lambda x: self.get_per_user_records(x)).groupByKey().mapValues(list)

        return per_user_records.flatMap(lambda x: self.execute_per_user_events(x))

    def write_output_file(self, spark: SparkSession, path: str, per_user_data: RDD) -> None:
        if self._window_dtc is None:
            per_user_data.map(lambda x: json.dumps(x, default=str)).saveAsTextFile(path)
        else:
            # Convert to a DataFrame first so that the data can be saved as a CSV
            spark.createDataFrame(per_user_data).write.csv(path, header=True)


def main():
    arguments = docopt(__doc__, version='pre-alpha')
    spark_runner = SparkRunner(arguments['--raw-data'].split(','), arguments['--streaming-dtc'],
                               arguments['--window-dtc'])

    spark = SparkSession \
        .builder \
        .appName("BlurrSparkRunner") \
        .getOrCreate()
    spark_context = spark.sparkContext
    per_user_data_rdd = spark_runner.execute(spark_context)
    spark_runner.write_output_file(spark, arguments['--output-file'], per_user_data_rdd)


if __name__ == "__main__":
    main()

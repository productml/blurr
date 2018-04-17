import json
from typing import List, Optional

from blurr.runner.runner import Runner
_spark_import_err = None
try:
    from pyspark import RDD, SparkContext
    from pyspark.sql import SparkSession
except ImportError as err:
    # Ignore import error because the CLI can be used even if spark is not
    # installed.
    _spark_import_err = err

# Setting these at global level as they cannot be part of the spark runner object because they
# cannot be serialized
spark_session: 'SparkSession' = None


def get_spark_context() -> 'SparkContext':
    global spark_session
    if spark_session:
        return spark_session.sparkContext

    spark_session = SparkSession \
        .builder \
        .appName("BlurrSparkRunner") \
        .getOrCreate()
    return spark_session.sparkContext


class SparkRunner(Runner):
    def __init__(self,
                 json_files: List[str],
                 stream_dtc_file: str,
                 window_dtc_file: Optional[str] = None,
                 record_processor: str = 'default'):
        if _spark_import_err:
            raise _spark_import_err
        super().__init__(json_files, stream_dtc_file, window_dtc_file, record_processor)

    def execute(self):
        spark_context = get_spark_context()
        raw_records = spark_context.union(
            [spark_context.textFile(file) for file in self._raw_files])
        per_user_records = raw_records.flatMap(
            lambda x: self.get_per_user_records(x)).groupByKey().mapValues(list)

        return per_user_records.flatMap(lambda x: self.execute_per_user_events(x))

    def write_output_file(self, path: str, per_user_data) -> None:
        if self._window_dtc is None:
            per_user_data.map(lambda x: json.dumps(x, default=str)).saveAsTextFile(path)
        else:
            # Convert to a DataFrame first so that the data can be saved as a CSV
            spark_session.createDataFrame(per_user_data).write.csv(path, header=True)

    def print_output(self, per_user_data) -> None:
        for row in per_user_data.map(lambda x: json.dumps(x, default=str)).collect():
            print(row)

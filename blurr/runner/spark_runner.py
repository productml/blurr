import json
from typing import List, Optional

from blurr.runner.data_processor import DataProcessor, SimpleJsonDataProcessor
from blurr.runner.runner import Runner
_spark_import_err = None
try:
    from pyspark import RDD, SparkContext
    from pyspark.sql import SparkSession
except ImportError as err:
    # Ignore import error because the CLI can be used even if spark is not
    # installed.
    _spark_import_err = err

# Setting these at module level as they cannot be part of the spark runner object because they
# cannot be serialized
_module_spark_session: 'SparkSession' = None


def get_spark_session(spark_session: Optional['SparkSession']) -> 'SparkSession':
    if spark_session:
        return spark_session

    global _module_spark_session
    if _module_spark_session:
        return _module_spark_session

    _module_spark_session = SparkSession \
        .builder \
        .appName("BlurrSparkRunner") \
        .getOrCreate()
    return _module_spark_session


class SparkRunner(Runner):
    def __init__(self,
                 json_files: List[str],
                 stream_dtc_file: str,
                 window_dtc_file: Optional[str] = None,
                 data_processor: DataProcessor = SimpleJsonDataProcessor()):
        if _spark_import_err:
            raise _spark_import_err
        super().__init__(json_files, stream_dtc_file, window_dtc_file, data_processor)

    def execute(self, spark_session: Optional['SparkSession'] = None):
        spark_context = get_spark_session(spark_session).sparkContext
        raw_records = spark_context.union(
            [spark_context.textFile(file) for file in self._raw_files])
        per_identity_records = raw_records.flatMap(
            lambda x: self.get_per_identity_records(x)).groupByKey().mapValues(list)

        return per_identity_records.flatMap(lambda x: self.execute_per_identity_records(x))

    def write_output_file(self,
                          path: str,
                          per_identity_data: 'RDD',
                          spark_session: Optional['SparkSession'] = None) -> None:
        _spark_session_ = get_spark_session(spark_session)
        if not self._window_dtc:
            per_identity_data.map(lambda x: json.dumps(x, default=str)).saveAsTextFile(path)
        else:
            # Convert to a DataFrame first so that the data can be saved as a CSV
            _spark_session_.createDataFrame(per_identity_data.flatMap(lambda x: x[1])).write.csv(
                path, header=True)

    def print_output(self, per_identity_data) -> None:
        for row in per_identity_data.map(lambda x: json.dumps(x, default=str)).collect():
            print(row)

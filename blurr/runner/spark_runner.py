import json
from typing import List, Optional

from blurr.runner.data_processor import DataProcessor, SimpleJsonDataProcessor, \
    SimpleDictionaryDataProcessor
from blurr.runner.runner import Runner, BlurrJSONEncoder

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
    def __init__(self, stream_dtc_file: str, window_dtc_file: Optional[str] = None):
        if _spark_import_err:
            raise _spark_import_err
        super().__init__(stream_dtc_file, window_dtc_file)

    def execute(self, identity_records: 'RDD'):
        return identity_records.flatMap(lambda x: self.execute_per_identity_records(x))

    def get_record_rdd_from_json_files(self,
                                       json_files: List[str],
                                       data_processor: DataProcessor = SimpleJsonDataProcessor(),
                                       spark_session: Optional['SparkSession'] = None) -> 'RDD':
        spark_context = get_spark_session(spark_session).sparkContext
        raw_records: 'RDD' = spark_context.union(
            [spark_context.textFile(file) for file in json_files])
        return raw_records.mapPartitions(
            lambda x: self.get_per_identity_records(x, data_processor)).groupByKey().mapValues(list)

    def get_record_rdd_from_rdd(self,
                                rdd: 'RDD',
                                data_processor: DataProcessor = SimpleDictionaryDataProcessor()
                                ) -> 'RDD':
        return rdd.mapPartitions(
            lambda x: self.get_per_identity_records(x, data_processor)).groupByKey().mapValues(list)

    def write_output_file(self,
                          path: str,
                          per_identity_data: 'RDD',
                          spark_session: Optional['SparkSession'] = None) -> None:
        _spark_session_ = get_spark_session(spark_session)
        if not self._window_dtc:
            per_identity_data.map(lambda x: json.dumps(x, cls=BlurrJSONEncoder)).saveAsTextFile(
                path)
        else:
            # Convert to a DataFrame first so that the data can be saved as a CSV
            _spark_session_.createDataFrame(per_identity_data.flatMap(lambda x: x[1])).write.csv(
                path, header=True)

    def print_output(self, per_identity_data) -> None:
        for row in per_identity_data.map(lambda x: json.dumps(x, cls=BlurrJSONEncoder)).collect():
            print(row)

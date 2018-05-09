import json
from typing import List, Optional, Tuple, Dict, Union

from blurr.runner.data_processor import DataProcessor, SimpleJsonDataProcessor, \
    SimpleDictionaryDataProcessor
from blurr.runner.json_encoder import BlurrJSONEncoder
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


def get_spark_session(spark_session: Optional['SparkSession'] = None) -> 'SparkSession':
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
    """
    Provides these functionality in a Spark session:
    - Convert events data into Records.
    - Execute Blurr DTC on Records.
    - Basic output to file and to stdout.

    Example usage to process JSON events in files:
    ```
    json_files = [...]
    runner = SparkRunner(stream_dtc_file, window_dtc_file)
    # Provide a custom data processor if the json files contain data in a format other than
    # new-line separated JSONs where each JSON object is an event dictionary.
    records_rdd = runner.get_record_rdd_from_json_files(json_files)
    output_rdd = runner.execute(records_rdd)
    # Output rdd contains data in a Tuple[Identity, Tuple[Streaming DTC State, List of Window DTC
    # output]] format. These two can then be written out to external storage for passing DTC State
    # for the next execution run and as the training data for the model respectively.
    ```
    """

    def __init__(self, stream_dtc_file: str, window_dtc_file: Optional[str] = None):
        """
        Initialize SparkRunner.

        :param stream_dtc_file: Streaming DTC to use. Must be provded.
        :param window_dtc_file: Window DTC to use. If none is provided only the streaming DTC output
            is generated.
        """
        if _spark_import_err:
            raise _spark_import_err
        super().__init__(stream_dtc_file, window_dtc_file)

    def _execute_per_identity_records(
            self, identity_records_with_state: Tuple[str, Union[List, Tuple[List, Dict]]]):
        identity, records_with_state = identity_records_with_state
        if isinstance(records_with_state, tuple):
            record, state = records_with_state
        else:
            record, state = records_with_state, None
        return self.execute_per_identity_records(identity, record, state)

    def execute(self, identity_records: 'RDD', old_state_rdd: Optional['RDD'] = None) -> 'RDD':
        """
        Executes Blurr DTC with the given records. old_state_rdd can be provided to load an older
        state from a previous run.

        :param identity_records: RDD of the form Tuple[Identity, List[TimeAndRecord]]
        :param old_state_rdd: A previous streaming DTC state RDD as Tuple[Identity, Streaming DTC
            State]
        :return: RDD[Identity, Tuple[Streaming DTC State, List of Window DTC output]]
        """
        identity_records_with_state = identity_records
        if old_state_rdd:
            identity_records_with_state = identity_records.fullOuterJoin(old_state_rdd)
        return identity_records_with_state.map(lambda x: self._execute_per_identity_records(x))

    def get_record_rdd_from_json_files(self,
                                       json_files: List[str],
                                       data_processor: DataProcessor = SimpleJsonDataProcessor(),
                                       spark_session: Optional['SparkSession'] = None) -> 'RDD':
        """
        Reads the data from the given json_files path and converts them into the `Record`s format for
        processing. `data_processor` is used to process the per event data in those files to convert
        them into `Record`.

        :param json_files: List of json file paths. Regular Spark path wildcards are accepted.
        :param data_processor: `DataProcessor` to process each event in the json files.
        :param spark_session: `SparkSession` to use for execution. If None is provided then a basic
            `SparkSession` is created.
        :return: RDD containing Tuple[Identity, List[TimeAndRecord]] which can be used in
            `execute()`
        """
        spark_context = get_spark_session(spark_session).sparkContext
        raw_records: 'RDD' = spark_context.union(
            [spark_context.textFile(file) for file in json_files])
        return raw_records.mapPartitions(
            lambda x: self.get_per_identity_records(x, data_processor)).groupByKey().mapValues(list)

    def get_record_rdd_from_rdd(
            self,
            rdd: 'RDD',
            data_processor: DataProcessor = SimpleDictionaryDataProcessor(),
    ) -> 'RDD':
        """
        Converts a RDD of raw events into the `Record`s format for processing. `data_processor` is
        used to process the per row data to convert them into `Record`.

        :param rdd: RDD containing the raw events.
        :param data_processor: `DataProcessor` to process each row in the given `rdd`.
        :return: RDD containing Tuple[Identity, List[TimeAndRecord]] which can be used in
            `execute()`
        """
        return rdd.mapPartitions(
            lambda x: self.get_per_identity_records(x, data_processor)).groupByKey().mapValues(list)

    def write_output_file(self,
                          path: str,
                          per_identity_data: 'RDD',
                          spark_session: Optional['SparkSession'] = None) -> None:
        """
        Basic helper function to persist data to disk.

        If window DTC was provided then the window DTC output to written in csv format, otherwise,
        the streaming DTC output is written in JSON format to the `path` provided

        :param path: Path where the output should be written.
        :param per_identity_data: Output of the `execute()` call.
        :param spark_session: `SparkSession` to use for execution. If None is provided then a basic
            `SparkSession` is created.
        :return:
        """
        _spark_session_ = get_spark_session(spark_session)
        if not self._window_dtc:
            per_identity_data.flatMap(
                lambda x: [json.dumps(data, cls=BlurrJSONEncoder) for data in x[1][0].items()]
            ).saveAsTextFile(path)
        else:
            # Convert to a DataFrame first so that the data can be saved as a CSV
            _spark_session_.createDataFrame(per_identity_data.flatMap(lambda x: x[1][1])).write.csv(
                path, header=True)

    def print_output(self, per_identity_data: 'RDD') -> None:
        """
        Basic helper function to write data to stdout. If window DTC was provided then the window
        DTC output is written, otherwise, the streaming DTC output is written to stdout.

        WARNING - For large datasets this will be extremely slow.

        :param per_identity_data: Output of the `execute()` call.
        """
        if not self._window_dtc:
            data = per_identity_data.flatMap(
                lambda x: [json.dumps(data, cls=BlurrJSONEncoder) for data in x[1][0].items()])
        else:
            # Convert to a DataFrame first so that the data can be saved as a CSV
            data = per_identity_data.map(
                lambda x: json.dumps((x[0], x[1][1]), cls=BlurrJSONEncoder))
        for row in data.collect():
            print(row)

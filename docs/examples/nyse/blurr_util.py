import json

from IPython.display import HTML, display

from blurr.cli.validate import validate_command
from blurr.runner.data_processor import SimpleJsonDataProcessor
from blurr.runner.local_runner import LocalRunner


def validate(template):
    validate_command([template])


def transform(*, log_files, stream_dtc, window_dtc=None, output_file):
    transform_local(stream_dtc, window_dtc, log_files, output_file)


def transform_local(stream_dtc_file, window_dtc_file,
                    raw_json_files, output_file) -> int:
    runner = LocalRunner(stream_dtc_file, window_dtc_file)
    result = runner.execute(
        runner.get_identity_records_from_json_files(raw_json_files, SimpleJsonDataProcessor()))
    runner.write_output_file(output_file, result)


def print_head(file):
    table = "<table>"

    with open(file) as f:
        first_log = json.loads(f.readline())
        table += "<tr>"
        for key, value in first_log[1].items():
            table += "<th>" + key + "<th>"
        table += "</tr>"


    with open(file) as f:
        i = 0
        while i < 4:
            log = json.loads(f.readline())
            if not log[1]['_identity']:
                continue;
            i = i + 1
            table += "<tr>"
            for key, value in log[1].items():
                table += "<td>" + str(value) + "<td>"
            table += "</tr>"

    table += "</table>"
    display(HTML(table))
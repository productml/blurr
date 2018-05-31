import os
import sys
from typing import List, Tuple, Dict

import yaml

from blurr.core.type import Type


def is_window_dtc(dtc_dict: Dict) -> bool:
    return Type.is_type_equal(dtc_dict.get('Type', ''), Type.BLURR_TRANSFORM_WINDOW)


def is_streaming_dtc(dtc_dict: Dict) -> bool:
    return Type.is_type_equal(dtc_dict.get('Type', ''), Type.BLURR_TRANSFORM_STREAMING)


def get_yml_files(path: str = '.') -> List[str]:
    return [
        os.path.join(path, file)
        for file in os.listdir(path)
        if (file.endswith('.yml') or file.endswith('.yaml'))
    ]


def get_stream_window_dtc_files(dtc_files: List[str]) -> Tuple[str, str]:
    stream_dtc_file = None
    window_dtc_file = None
    for dtc_file in dtc_files:
        if stream_dtc_file and window_dtc_file:
            break

        dtc_dict = yaml.safe_load(open(dtc_file))
        if not stream_dtc_file and is_streaming_dtc(dtc_dict):
            stream_dtc_file = dtc_file

        if not window_dtc_file and is_window_dtc(dtc_dict):
            window_dtc_file = dtc_file

    return stream_dtc_file, window_dtc_file


def eprint(*args):
    print(*args, file=sys.stderr)

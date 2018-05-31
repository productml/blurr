import os
import sys
from typing import List, Tuple, Dict

import yaml

from blurr.core.type import Type


def is_window_bts(bts_dict: Dict) -> bool:
    return Type.is_type_equal(bts_dict.get('Type', ''), Type.BLURR_TRANSFORM_WINDOW)


def is_streaming_bts(bts_dict: Dict) -> bool:
    return Type.is_type_equal(bts_dict.get('Type', ''), Type.BLURR_TRANSFORM_STREAMING)


def get_yml_files(path: str = '.') -> List[str]:
    return [
        os.path.join(path, file)
        for file in os.listdir(path)
        if (file.endswith('.yml') or file.endswith('.yaml'))
    ]


def get_stream_window_bts_files(bts_files: List[str]) -> Tuple[str, str]:
    stream_bts_file = None
    window_bts_file = None
    for bts_file in bts_files:
        if stream_bts_file and window_bts_file:
            break

        bts_dict = yaml.safe_load(open(bts_file))
        if not stream_bts_file and is_streaming_bts(bts_dict):
            stream_bts_file = bts_file

        if not window_bts_file and is_window_bts(bts_dict):
            window_bts_file = bts_file

    return stream_bts_file, window_bts_file


def eprint(*args):
    print(*args, file=sys.stderr)

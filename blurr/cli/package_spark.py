import zipfile
from distutils.dir_util import copy_tree

import os
import tempfile
from pip._internal import main as pip_main

from blurr.cli.util import eprint
from blurr.core import logging


def _create_zip(dir: str, target_file: str) -> None:
    dir_abs_path = os.path.abspath(dir)
    zip_file = zipfile.ZipFile(target_file, 'w', zipfile.ZIP_DEFLATED)
    for root, dirs, files in os.walk(dir):
        for file in files:
            file_path = os.path.join(root, file)
            zip_file.write(file_path, file_path.replace(dir_abs_path, ''))
    zip_file.close()


def _resolve_pip_dependencies(tmp_dir: str) -> int:
    pip_verbosity = [] if logging.is_debug_enabled() else ["--quiet"]
    pip_cmd = [
        "install", "--requirement",
        os.path.join(tmp_dir, "requirements.txt"), "--target", tmp_dir
    ] + pip_verbosity
    return pip_main(pip_cmd)


def package_spark(source_dir: str, target_file: str) -> int:
    if not os.path.isdir(source_dir):
        eprint(source_dir + " is not a valid directory")
        return 1

    req_file = os.path.join(source_dir, "requirements.txt")
    if not os.path.exists(req_file):
        eprint("requirements.txt not found in " + source_dir)
        return 1

    tmp_dir = tempfile.mkdtemp()

    logging.debug("copying app files to " + tmp_dir)
    copy_tree(source_dir, tmp_dir)

    logging.debug("resolving pip dependencies into " + tmp_dir)
    pip_out = _resolve_pip_dependencies(tmp_dir)
    if pip_out != 0:
        eprint("there was an error processing " + req_file)
        return 1

    logging.debug("creating package " + target_file)
    _create_zip(tmp_dir, target_file)
    print("spark app generated successfully: " + os.path.abspath(target_file))

    return 0

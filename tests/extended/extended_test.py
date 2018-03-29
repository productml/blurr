from blurr.core.store import Key
from blurr.runner.local_runner import LocalRunner
from datetime import datetime, timezone


def test_extended_runner():
    local_runner = LocalRunner(['tests/extended/raw.json'],
                               'tests/extended/stream.yml')
    local_runner.execute()

    assert local_runner._session_data is not None

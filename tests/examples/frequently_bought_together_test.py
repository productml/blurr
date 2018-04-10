from blurr.cli.validate import validate_file


def test_streaming_dtc_is_valid():
    assert validate_file('examples/frequently-bought-together/fbt-streaming-dtc.yml') == 0


def test_window_dtc_is_valid():
    assert validate_file('examples/frequently-bought-together/fbt-window-dtc.yml') == 0

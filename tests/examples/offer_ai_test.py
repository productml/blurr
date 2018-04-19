from blurr.cli.validate import validate_file


def test_streaming_dtc_is_valid():
    assert validate_file('examples/offer-ai/offer-ai-streaming-dtc.yml') == 0


def test_window_dtc_is_valid():
    assert validate_file('examples/offer-ai/offer-ai-window-dtc.yml') == 0

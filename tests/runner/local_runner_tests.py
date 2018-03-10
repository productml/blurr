from blurr.runner.local_runner import LocalRunner


def test_local_runner():
    local_runner = LocalRunner(['data/raw.json'], '', 'data/sample.yml')
    local_runner.execute()

    assert local_runner._user_transformer['userA'].export['session'][
        'events'] == 1
    assert local_runner._user_transformer['userB'].export['session'][
        'events'] == 1
    assert local_runner._user_transformer['userC'].export['session'][
        'events'] == 1

from blurr.runner.local_runner import LocalRunner


def test_local_runner():
    local_runner = LocalRunner(['data/raw.json'], '', 'data/sample.yml')
    local_runner.execute()

    assert local_runner._user_transformer['userA'].snapshot['session'][
        'events'] == 1
    assert local_runner._user_transformer['userB'].snapshot['session'][
        'events'] == 1
    assert local_runner._user_transformer['userC'].snapshot['session'][
        'events'] == 1

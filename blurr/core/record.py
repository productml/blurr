class Record:
    def __init__(self, event_dict):
        self._event_dict = event_dict

    def __getattr__(self, item):
        if isinstance(self._event_dict[item], dict):
            return Record(self._event_dict[item])
        else:
            self._event_dict[item]

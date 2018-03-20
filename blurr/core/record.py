class Record:
    def __init__(self, event_dict):
        for k, v in event_dict.items():
            self.__dict__[k] = v

    def __getattr__(self, name):
        if not hasattr(super(), name):
            return None

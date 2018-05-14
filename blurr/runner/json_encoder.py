from decimal import Decimal
from json import JSONEncoder

from blurr.core.store_key import Key


class BlurrJSONEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            ratio = o.as_integer_ratio()
            if ratio[1] == 1:
                return int(o)
            else:
                return float(o)

        if isinstance(o, Key):
            return str(o)

        return super().default(o)

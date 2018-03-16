import yaml

from blurr.core.errors import InvalidSchemaError
from blurr.core.syntax.schema_validator import validate


def validate_command(dtc_file, out):
    try:
        dtc_dict = yaml.safe_load(open(dtc_file))
        validate(dtc_dict)
        out.print("document is valid")
        return 0
    except yaml.YAMLError:
        out.eprint("invalid yaml")
        return 1
    except InvalidSchemaError as err:
        out.eprint(str(err))
        return 1
    except:
        out.eprint("there was an error parsing the document")
        return 1

import yaml

from blurr.core.errors import InvalidSchemaError
from blurr.util.eprint import eprint


def validate_command(dtc_file):
    print("validating " + dtc_file + " DTC")
    try:
        yaml.safe_load(open(dtc_file))
        # TODO : validate once ready
        print("document is valid")
    except yaml.YAMLError:
        eprint("document is not a valid yaml document")
    except InvalidSchemaError as err:
        eprint(str(err.value))
    except:
        eprint("there was an error parsing the document")



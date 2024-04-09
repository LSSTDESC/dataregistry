import os
import yaml


def _populate_defaults(mydict):
    """
    Populate the default values for rows that haven't been specified in the
    YAML file.

    Parameters
    ----------
    mydict : dict
    """

    # Attributes we check for and populate with these default value if missing
    atts = {"nullable": True, "primary_key": False, "foreign_key": False, "cli_optional": False,
            "cli_default": None, "choices": None}

    # Loop over eah row and ingest
    for table in mydict.keys():
        for row in mydict[table].keys():
            for att in atts.keys():
                if att not in mydict[table][row].keys():
                    mydict[table][row][att] = atts[att]


def load_schema():
    """Load the schema layout from the YAML file"""

    # Load
    yaml_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "schema.yaml"
    )
    with open(yaml_file_path, "r") as file:
        yaml_data = yaml.safe_load(file)

    # Populate defaults
    _populate_defaults(yaml_data)

    return yaml_data

def load_preset_keywords():
    """Load the preset keywords from the yaml file"""

    # Load
    yaml_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "keywords.yaml"
    )
    with open(yaml_file_path, "r") as file:
        yaml_data = yaml.safe_load(file)

    return yaml_data

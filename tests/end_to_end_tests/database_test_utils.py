import os

import pytest
from dataregistry.db_basic import SCHEMA_VERSION

__all__ = [
    "dummy_file",
    "_insert_alias_entry",
    "_insert_execution_entry",
    "_insert_dataset_entry",
]


@pytest.fixture
def dummy_file(tmp_path):
    """
    Create some dummy (temporary) files and directories to work with during
    testing.

    Structure is as follows:

    | - <tmp_path>
    |   - <source>
    |     - file1.txt
    |     - file2.txt
    |     - <directory1>
    |       - file2.txt
    |   - <root_dir>
    |     - <schema/user/uid>
    |       - <dummy_dir>
    |         - file1.txt
    |       - file1.txt

    Parameters
    ----------
    tmp_path : pathlib.Path object

    Returns
    -------
    tmp_src_dir : pathlib.Path object
        Temporary files we are going to be copying into the registry will be
        created in here
    tmp_root_dir : pathlib.Path object
        Temporary root_dir for the registry we can copy files to
    """

    # Temp dir for files that we copy files from (old_location)
    tmp_src_dir = tmp_path / "source"
    tmp_src_dir.mkdir()

    for i in range(2):
        f = tmp_src_dir / f"file{i+1}.txt"
        f.write_text("i am a dummy file")

    p = tmp_src_dir / "directory1"
    p.mkdir()
    f = p / "file2.txt"
    f.write_text("i am another dummy file")

    # Temp root_dir of the registry
    tmp_root_dir = tmp_path / "root_dir"

    # Make some dummy data already on location
    for THIS_SCHEMA in [SCHEMA_VERSION + "/", ""]:
        p = tmp_root_dir / f"{THIS_SCHEMA}user/{os.getenv('USER')}/dummy_dir"
        p.mkdir(parents=True)

        f = p / "file1.txt"
        f.write_text("i am another dummy file (but on location in a dir)")

        p = tmp_root_dir / f"{THIS_SCHEMA}user/{os.getenv('USER')}"
        f = p / "file1.txt"
        f.write_text("i am another dummy file (but on location)")

    return tmp_src_dir, tmp_root_dir


def _insert_alias_entry(reg, name, dataset_id, ref_alias_id=None,
                        supersede=False):
    """
    Wrapper to create dataset alias entry

    Parameters
    ----------
    reg : an instance of the Registrar class
    name : str
        Name of alias
    dataset_id : int
        Dataset we are assigning alias name to
    ref_alias_id : int
        Optional.  If dataset_id is None, the new alias will point to
        the one identified by alias_id

    Returns
    -------
    new_id : int
        The alias ID for this new entry
    """

    new_id = reg.dataset_alias.register(name, dataset_id,
                                        ref_alias_id=ref_alias_id,
                                        supersede=supersede)
    if not new_id:
        print("Dataset alias entry creation failed")
        if not supersede:
            print("Did you mean to supersede?")
        return None

    print(f"Created dataset alias entry with id {new_id}")

    return new_id


def _insert_execution_entry(
    datareg,
    name,
    description,
    input_datasets=[],
    input_production_datasets=[],
    configuration=None,
):
    """
    Wrapper to create execution entry

    Parameters
    ----------
    name : str
        Name of execution
    description : str
        Description of execution
    intput_datasets : list
        List of dataset ids
    configuration : str
        Path to configuration file for execution

    Returns
    -------
    new_id : int
        The execution ID for this new entry
    """

    new_id = datareg.Registrar.execution.register(
        name,
        description=description,
        input_datasets=input_datasets,
        input_production_datasets=input_production_datasets,
        configuration=configuration,
    )

    assert new_id is not None, "Trying to create a execution that already exists"
    print(f"Created execution entry with id {new_id}")

    return new_id


def _insert_dataset_entry(
    datareg,
    relpath,
    version,
    owner_type=None,
    owner=None,
    description=None,
    name=None,
    execution_id=None,
    version_suffix=None,
    old_location=None,
    is_overwritable=False,
    which_datareg=None,
    execution_name=None,
    execution_description=None,
    execution_start=None,
    execution_site=None,
    execution_configuration=None,
    input_datasets=[],
    location_type="dummy",
    contact_email=None,
    url=None,
    keywords=[],
):
    """
    Wrapper to create dataset entry

    Parameters
    ----------
    relpath : str
        Relative path within the data registry to store the data
        Relative to <ROOT>/<owner_type>/<owner>/...
    version : str
        Semantic version string (i.e., M.N.P) or
        "major", "minor", "patch" to automatically bump the version previous
    owner_type : str
        Either "production", "group", "user"
    owner : str
        Dataset owner
    description : str
        Description of dataset
    name : str
        A manually selected name for the dataset
    execution_id : int
        Execution entry related to this dataset
    version_suffix : str
        Append a suffix to the version string
    old_location : str
        Path to data to be copied to data registry
    which_datareg : DataRegistry object
        In case we want to register using a custom DataRegistry object
    execution_name : str, optional
            Typically pipeline name or program name
    execution_description : str, optional
        Human readible description of execution
    execution_start : datetime, optional
    execution_site : str, optional
        Where was the execution performed?
    execution_configuration : str, optional
        Path to text file used to configure the execution
    input_datasets : list, optional
        List of dataset ids that were the input to this execution
    location_type : str, optional
        "dataregistry", "external" or "dummy"
    contact_email : str
        Contact email for external datasets
    url : str
        Url for external datasets
    keywords : list[str]
        List of keywords to tag

    Returns
    -------
    dataset_id : int
        The dataset it created for this entry
    """

    # Some defaults over all test datasets
    make_sym_link = False

    # Add new entry.
    dataset_id, execution_id = datareg.Registrar.dataset.register(
        relpath,
        version,
        version_suffix=version_suffix,
        name=name,
        creation_date=None,
        description=description,
        old_location=old_location,
        copy=(not make_sym_link),
        execution_id=execution_id,
        verbose=True,
        owner=owner,
        owner_type=owner_type,
        is_overwritable=is_overwritable,
        execution_name=execution_name,
        execution_description=execution_description,
        execution_start=execution_start,
        execution_site=execution_site,
        execution_configuration=execution_configuration,
        input_datasets=input_datasets,
        location_type=location_type,
        contact_email=contact_email,
        url=url,
        keywords=keywords,
    )

    assert dataset_id is not None, "Trying to create a dataset that already exists"
    assert execution_id is not None, "Trying to create a execution that already exists"
    print(f"Created dataset entry with id {dataset_id}")

    return dataset_id

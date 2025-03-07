import os

import pytest

__all__ = [
    "dummy_file",
    "_insert_alias_entry",
    "_insert_execution_entry",
    "_insert_dataset_entry",
    "_replace_dataset_entry",
]

DEFAULT_SCHEMA_WORKING = "lsst_desc_working"

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
    |     - file1_sym.txt (symlink)
    |     - <directory1>
    |       - file2.txt
    |     - <directory1_sym> (symlink)
    |   - <root_dir>
    |     - <schema/user/uid>
    |       - <dummy_dir>
    |         - file1.txt
    |       - <dummy_dir_2>
    |         - file1.txt
    |       - file1.txt
    |       - file2.txt

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

    # Make two text files in source
    for i in range(2):
        f = tmp_src_dir / f"file{i+1}.txt"
        f.write_text("i am a dummy file")

    # Make a symlink to `file1.txt`
    link = tmp_src_dir / "file1_sym.txt"
    link.symlink_to(tmp_src_dir / "file1.txt")

    # Make a symlink to `directory1`
    link = tmp_src_dir / "directory1_sym"
    link.symlink_to(tmp_src_dir / "directory1")

    # Make directory in source and a file within that directory
    p = tmp_src_dir / "directory1"
    p.mkdir()
    f = p / "file2.txt"
    f.write_text("i am another dummy file")

    # Temp root_dir of the registry
    tmp_root_dir = tmp_path / "root_dir"

    # Make some dummy data already on location
    for THIS_SCHEMA in [DEFAULT_SCHEMA_WORKING + "/", ""]:
        for f in ["dummy_dir", "dummy_dir_2"]:
            p = tmp_root_dir / f"{THIS_SCHEMA}user/{os.getenv('USER')}/{f}"
            p.mkdir(parents=True)

            f = p / "file1.txt"
            f.write_text("i am another dummy file (but on location in a dir)")

        for f in ["file1.txt", "file2.txt"]:
            p = tmp_root_dir / f"{THIS_SCHEMA}user/{os.getenv('USER')}"
            f = p / f
            f.write_text("i am another dummy file (but on location)")

    return tmp_src_dir, tmp_root_dir


def _insert_alias_entry(reg, name, dataset_id, ref_alias_id=None, supersede=False):
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
    supersede : boolean
        If True, make the entry even if the alias was already used. In this
        case mark old entries as superseded.

    Returns
    -------
    new_id : int
        The alias ID for this new entry
    """

    new_id = reg.dataset_alias.register(
        name, dataset_id, ref_alias_id=ref_alias_id, supersede=supersede
    )
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
    **See `src/dataregistry/registrar/execution.py` for a full description of
    parameters

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
    name,
    version,
    owner_type=None,
    owner=None,
    description=None,
    execution_id=None,
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
    relative_path=None,
    access_api=None,
):
    """
    Wrapper to create dataset entry during testing

    Parameters
    ----------
    **See `src/dataregistry/registrar/dataset.py` for a full description of
    parameters 

    Returns
    -------
    dataset_id : int
        The dataset it created for this entry
    """

    # Add new entry.
    dataset_id, execution_id = datareg.Registrar.dataset.register(
        name,
        version,
        creation_date=None,
        description=description,
        old_location=old_location,
        execution_id=execution_id,
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
        relative_path=relative_path,
        access_api=access_api,
    )

    assert dataset_id is not None, "Trying to create a dataset that already exists"
    assert execution_id is not None, "Trying to create a execution that already exists"
    print(f"Created dataset entry with id {dataset_id}")

    return dataset_id

def _replace_dataset_entry(
    datareg,
    name,
    version,
    owner_type=None,
    owner=None,
    description=None,
    execution_id=None,
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
    relative_path=None,
    access_api=None,
):
    """
    Wrapper to replace dataset entry during testing

    Parameters
    ----------
    **See `src/dataregistry/registrar/dataset.py` for a full description of
    parameters

    Returns
    -------
    dataset_id : int
        The dataset it created for this entry
    """

    # Add new entry.
    dataset_id, execution_id = datareg.Registrar.dataset.replace(
        name,
        version,
        creation_date=None,
        description=description,
        old_location=old_location,
        execution_id=execution_id,
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
        access_api=access_api,
    )

    return dataset_id

# Define constants for dataset's "status" bit position
__all__ = ["set_dataset_status", "get_dataset_status", "DATASET_STATUS_BIT",
           "DATASET_STATUS_VALUE"]
DATASET_STATUS_BIT = {
    # Is a valid dataset or not. "Invalid" means the dataset entry was created in
    # the database, but there was an issue copying the physical data.
    "valid": 0,
    # Has the data of this dataset been deleted from the `root_dir`?
    "deleted": 1,
    # Has the data for this dataset been archived?
    "archived": 2,
    # Has this dataset been replaced at some point?
    "replaced": 3,
    # Is there a request to archive this dataset?
    "request_archive": 4,
    # Is there a request to delete from disk following archive?
    "request_archive_delete": 5,
}

DATASET_STATUS_VALUE = {key: 1 << DATASET_STATUS_BIT[key] for key in DATASET_STATUS_BIT}


def set_dataset_status(
    current_status, valid=None, deleted=None, archived=None,
    replaced=None, request_archive=None, request_archive_delete=None,
):
    """
    Update a value of a dataset's status bit poistion.

    These properties are not mutually exclusive, e.g., a dataset can be both
    archived and deleted.

    Properties
    ----------
    current_status : int
        The current bitwise representation of the dataset's status
    valid : bool, optional
        True to set the dataset as valid, False for invalid
    deleted : bool, optional
        True to set the dataset as deleted
    archived : bool, optional
        True to set the dataset as archived
    replaced : bool, optional
        True to set the dataset as replaced
    request_archive : bool, optional
        True to allow archive
    request_archive_delete : bool, optional
        True to allow archive followed by delete from disk

    Returns
    -------
    new_status : int
        The datasets new bitwise representation
    """

    # Set the bits for each condition
    for cond, ref in zip(
        [valid, deleted, archived, replaced, request_archive,
         request_archive_delete],
        ["valid", "deleted", "archived", "replaced", "request_archive",
         "request_archive_delete"],
    ):

        if cond is not None:
            if cond is True:  # set the bit
                current_status |= DATASET_STATUS_VALUE[ref]
            else:    # clear the bit
                current_status &= ~(DATASET_STATUS_VALUE[ref])

    return current_status


def get_dataset_status(current_valid_flag, which_bit):
    """
    Return the status of a dataset for a given bit index.

    Properties
    ----------
    current_flag_value : int
        The current bitwise representation of the dataset's status
    which_bit : str
        One of DATASET_STATUS_BIT keys()

    Returns
    -------
    - : bool
        True if `which_bit` is 1. e.g., If a dataset is deleted
        `get_dataset_status(<current_valid_flag>, "deleted") will return True.
    """

    # Make sure `which_bit` is valid.
    if which_bit not in DATASET_STATUS_BIT.keys():
        raise ValueError(f"{which_bit} is not a valid dataset status")

    return (current_valid_flag & (1 << DATASET_STATUS_BIT[which_bit])) != 0

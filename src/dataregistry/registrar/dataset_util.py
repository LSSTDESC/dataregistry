# Define constants for dataset's "status" bit position
VALID_STATUS_BITS = {
    # Is a valid dataset or not. "Invalid" means the dataset entry was created in
    # the database, but there was an issue copying the physical data.
    "valid": 0,
    # Has the data of this dataset been deleted from the `root_dir`?
    "deleted": 1,
    # Has the data for this dataset been archived?
    "archived": 2,
    # Has this dataset been replaced at some point?
    "replaced": 3,
}

# Dataset `valid` value if dataset is "valid" and nothing else
DATASET_ONLY_VALID = 1


def set_dataset_status(
    current_valid_flag, valid=None, deleted=None, archived=None, replaced=None
):
    """
    Update a value of a dataset's status bit poistion.

    These properties are not mutually exclusive, e.g., a dataset can be both
    archived and deleted.

    Properties
    ----------
    current_valid_flag : int
        The current bitwise representation of the dataset's status
    valid : bool, optional
        True to set the dataset as valid, False for invalid
    deleted : bool, optional
        True to set the dataset as deleted
    archived : bool, optional
        True to set the dataset as archived
    replaced : bool, optional
        True to set the dataset as replaced

    Returns
    -------
    valid_flag : int
        The datasets new bitwise representation
    """

    # Set the bits for each condition
    for cond, ref in zip(
        [valid, deleted, archived, replaced],
        ["valid", "deleted", "archived", "replaced"],
    ):
        if cond is not None:
            current_valid_flag &= ~(1 << VALID_STATUS_BITS[ref])
            current_valid_flag |= cond << VALID_STATUS_BITS[ref]

    return current_valid_flag


def get_dataset_status(current_valid_flag, which_bit):
    """
    Return the status of a dataset for a given bit index.

    Properties
    ----------
    current_flag_value : int
        The current bitwise representation of the dataset's status
    which_bit : str
        One of VALID_STATUS_BITS keys()

    Returns
    -------
    - : bool
        True if `which_bit` is 1. e.g., If a dataset is deleted
        `get_dataset_status(<current_valid_flag>, "deleted") will return True.
    """

    # Make sure `which_bit` is valid.
    if which_bit not in VALID_STATUS_BITS.keys():
        raise ValueError(f"{which_bit} is not a valid dataset status")

    return (current_valid_flag & (1 << VALID_STATUS_BITS[which_bit])) != 0

# Define constants for dataset's "status" bit position
VALID_STATUS_BITS = {
    # Is a valid dataset or not. "Invalid" means the dataset entry was created in
    # the database, but there was an issue copying the physical data.
    "valid": 0,
    # Has the data of this dataset been deleted from the `root_dir`?
    "deleted": 1,
    # Has the data for this dataset been archived?
    "archived": 2,
}


def set_dataset_status(current_valid_flag, valid=None, deleted=None, archived=None):
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

    Returns
    -------
    valid_flag : int
        The datasets new bitwise representation
    """

    if valid is not None:
        current_valid_flag &= ~(1 << VALID_STATUS_BITS["valid"])
        current_valid_flag |= valid << VALID_STATUS_BITS["valid"]

    if deleted is not None:
        current_valid_flag &= ~(1 << VALID_STATUS_BITS["deleted"])
        current_valid_flag |= deleted << VALID_STATUS_BITS["deleted"]

    if archived is not None:
        current_valid_flag &= ~(1 << VALID_STATUS_BITS["archived"])
        current_valid_flag |= archived << VALID_STATUS_BITS["archived"]

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

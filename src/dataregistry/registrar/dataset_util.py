# Define constants for dataset's "status" bit position
VALID_STATUS_BITS = {
    # Is a valid dataset or not? What characteristics does it have.
    # "Invalid" (least sig. bit = 0) means the dataset entry was created in
    # the database, but there was an issue copying the physical data.
    "registered": 0,
    # Has the data of this dataset been deleted from the `root_dir`?
    "deleted": 1,
    # Has the data for this dataset been archived?
    "archived": 2,
    # Has this dataset been replaced at some point?
    "replaced": 3,
    # Is dataregistry managing data for this dataset?
    "managed": 4,
}


def set_dataset_status(
        current_valid_mask, registered=None, deleted=None, archived=None,
        replaced=None, managed=None
):
    """
    Update a value of a dataset's status bit poistion.

    These properties are not mutually exclusive, e.g., a dataset can be both
    archived and deleted.

    Properties
    ----------
    current_valid_mask : int
        The current bitwise representation of the dataset's status
    registered : bool, optional
        True to set the dataset as properly registered; False for invalid
    deleted : bool, optional
        True to set the dataset as deleted
    archived : bool, optional
        True to set the dataset as archived
    replaced : bool, optional
        True to set the dataset as replaced
    managed : bool, optional
        True to set the dataset a managed

    Returns
    -------
    valid_mask : int
        The dataset status new bitwise representation
    """

    # Set the bits for each condition
    for cond, ref in zip(
        [registered, deleted, archived, replaced, managed],
        ["registered", "deleted", "archived", "replaced", "managed"],
    ):
        if cond is not None:
            current_valid_mask &= ~(1 << VALID_STATUS_BITS[ref])
            current_valid_mask |= cond << VALID_STATUS_BITS[ref]

    return current_valid_mask


def get_dataset_status(current_valid_mask, which_bit):
    """
    Return the status of a dataset for a given bit index.

    Parameters
    ----------
    current_valid_mask : int
        The current bitwise representation of the dataset's status
    which_bit : str
        One of VALID_STATUS_BITS keys()

    Returns
    -------
    - : bool
        True if `which_bit` is 1. e.g., If a dataset is deleted
        `get_dataset_status(<current_valid_mask>, "deleted") will return True.
    """

    # Make sure `which_bit` is valid.
    if which_bit not in VALID_STATUS_BITS.keys():
        raise ValueError(f"{which_bit} is not a valid dataset status")

    return (current_valid_mask & (1 << VALID_STATUS_BITS[which_bit])) != 0


def translate_status(status, as_string=True):
    """
    Given an integer value of dataset.status, translate to a word or words

    Parameters
    ----------
    status     int       The input integer status value
    as_string  Boolean   if True return a single string of blank-separated
                         values.  If False return a list
    """
    word_list = []
    if status == 0:
        if as_string:
            return "invalid"
        else:
            return ["invalid"]
    for v in VALID_STATUS_BITS:
        if (1 << (VALID_STATUS_BITS[v]) & status):
            word_list.append(v)

    if as_string:
        return ' '.join(word_list)
    else:
        return word_list

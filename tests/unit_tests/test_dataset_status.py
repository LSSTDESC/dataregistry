import pytest
from dataregistry.registrar.dataset_util import get_dataset_status, set_dataset_status


@pytest.mark.parametrize(
    "start_status,valid,deleted,archived,end_status",
    [
        (0, True, False, False, "0b1"),
        (0, True, True, True, "0b111"),
        (0, True, False, True, "0b101"),
        (5, None, True, None, "0b111"),
    ],
)
def test_set_dataset_status(start_status, valid, deleted, archived, end_status):
    """
    Make sure dataset bitwise valid flags get set correctly

    Starts from a value <start_status> and adds a flag, e.g., `deleted`, then
    checks the combined bitmask is correct.
    """

    assert (
        bin(
            set_dataset_status(
                start_status, valid=valid, deleted=deleted, archived=archived
            )
        )
        == end_status
    )


@pytest.mark.parametrize(
    "bin_status,is_valid,is_deleted,is_archived",
    [
        ("0b1", True, False, False),
        ("0b111", True, True, True),
        ("0b101", True, False, True),
        ("0b011", True, True, False),
    ],
)
def test_get_dataset_status(bin_status, is_valid, is_deleted, is_archived):
    """
    Make sure dataset bitwise valid flags get checked correctly.

    For a given `bin_status` (binary status), check that it pulls out the
    individual flags correctly.
    """

    assert get_dataset_status(int(bin_status, 2), "valid") == is_valid
    assert get_dataset_status(int(bin_status, 2), "deleted") == is_deleted
    assert get_dataset_status(int(bin_status, 2), "archived") == is_archived

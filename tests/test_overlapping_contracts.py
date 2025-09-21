from datetime import datetime

from powerbot_asyncio_client.models import Contract


def test_overlapping_contracts_qh_product_overlaps_with_the_h_product(algorithm):
    """
    This test ensures that the overlap check will return true if the changed QH product overlaps with the H related contract.
    """
    changed_contract = Contract(
        delivery_start=datetime.fromisoformat("2022-12-01T00:15+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T00:30+00:00")
    )
    related_contract = Contract(
        delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T01:00+00:00")
    )

    assert algorithm._Algorithm__is_overlapping(changed_contract, related_contract)


def test_overlapping_contracts_qh_product_overlaps_with_the_hh_product(algorithm):
    """
    This test ensures that the overlap check will return true if the changed QH product overlaps with the HH related contract.
    """
    changed_contract = Contract(
        delivery_start=datetime.fromisoformat("2022-12-01T00:15+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T00:30+00:00")
    )
    related_contract = Contract(
        delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T00:30+00:00")
    )

    assert algorithm._Algorithm__is_overlapping(changed_contract, related_contract)


def test_overlapping_contracts_h_product_overlaps_with_2h_block_product(algorithm):
    """
    This test ensures that the overlap check will return true if the changed H product overlaps with the 2H block related contract.
    """
    changed_contract = Contract(
        delivery_start=datetime.fromisoformat("2022-12-01T01:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T02:00+00:00")
    )
    related_contract = Contract(
        delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T02:00+00:00")
    )

    assert algorithm._Algorithm__is_overlapping(changed_contract, related_contract)


def test_overlapping_contracts_2h_block_product_overlaps_with_h_product(algorithm):
    """
    This test ensures that the overlap check will return true if the changed 2H block product overlaps with the H related contract.
    """
    changed_contract = Contract(
        delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T02:00+00:00")
    )
    related_contract = Contract(
        delivery_start=datetime.fromisoformat("2022-12-01T01:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T02:00+00:00")
    )

    assert algorithm._Algorithm__is_overlapping(changed_contract, related_contract)


def test_overlapping_contracts_2h_block_product_does_not_overlap_with_h_product(algorithm):
    """
    This test ensures that the overlap check will return false if the changed 2H block product does not overlap with the H related contract.
    """
    changed_contract = Contract(
        delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T02:00+00:00")
    )
    related_contract = Contract(
        delivery_start=datetime.fromisoformat("2022-12-01T02:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T03:00+00:00")
    )

    assert not algorithm._Algorithm__is_overlapping(changed_contract, related_contract)


def test_overlapping_contracts_h_product_does_not_overlap_with_qh_product(algorithm):
    """
    This test ensures that the overlap check will return false if the changed H product does not overlap with the QH related contract.
    """
    changed_contract = Contract(
        delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T02:00+00:00")
    )
    related_contract = Contract(
        delivery_start=datetime.fromisoformat("2022-12-01T02:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T02:15+00:00")
    )

    assert not algorithm._Algorithm__is_overlapping(changed_contract, related_contract)

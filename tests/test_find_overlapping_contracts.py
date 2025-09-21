from datetime import datetime

from powerbot_asyncio_client.models import Contract


def test_find_overlapping_contracts_delivery_durations_is_not_set_should_return_the_same_related_contracts(algorithm):
    """
    This test ensures, that if the delivery_durations parameter is not set, then the search will be skipped, and the same collection of the related
    contracts will be returned, accompanied by the longest contract.
    """

    algorithm.delivery_durations = []

    changed_contract = Contract(
        delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T01:00+00:00")
    )

    true_related_contracts = [
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T02:00+00:00")),
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T03:00+00:00")),
    ]

    related_contracts, longest_contract = algorithm._Algorithm__find_overlapping_contracts(
        changed_contract=changed_contract, related_contracts=true_related_contracts
    )

    assert related_contracts == true_related_contracts
    assert longest_contract == Contract(
        delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T03:00+00:00")
    )


def test_find_overlapping_contracts_delivery_durations_is_set_to_accept_h_and_qh_should_return_only_h_and_qh_overlapping_products(algorithm):
    """
    This test ensures, that if the delivery_durations parameter set to accept H and QH products, all the other products will be filtered out, and the
    overlapping contracts will be found among them.
    """

    algorithm.delivery_durations = [15, 60]

    changed_contract = Contract(
        delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T00:15+00:00")
    )

    # related contracts defined by the backend
    backend_related_contracts = [
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:15+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T00:30+00:00")),
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:30+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T00:45+00:00")),
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:45+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T01:00+00:00")),
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T01:00+00:00")),
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T03:00+00:00")),
    ]

    related_contracts, longest_contract = algorithm._Algorithm__find_overlapping_contracts(
        changed_contract=changed_contract, related_contracts=backend_related_contracts
    )

    # 3H product is filtered out, all the others are overlapping with the longest contract
    true_related_contracts = [
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:15+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T00:30+00:00")),
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:30+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T00:45+00:00")),
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:45+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T01:00+00:00")),
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T01:00+00:00")),
    ]

    assert related_contracts == true_related_contracts
    assert longest_contract == Contract(
        delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T01:00+00:00")
    )


def test_find_overlapping_contracts_delivery_durations_is_set_to_accept_hh_and_qh_should_return_only_hh_and_qh_overlapping_products(algorithm):
    """
    This test ensures, that if the delivery_durations parameter set to accept HH and QH products, all the other products will be filtered out, and the
    overlapping contracts will be found among them.
    """

    algorithm.delivery_durations = [15, 30]

    changed_contract = Contract(
        delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T00:15+00:00")
    )

    # related contracts defined by the backend
    backend_related_contracts = [
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T00:30+00:00")),
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:15+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T00:30+00:00")),
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:30+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T00:45+00:00")),
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:45+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T01:00+00:00")),
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T01:00+00:00")),
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T03:00+00:00")),
    ]

    related_contracts, longest_contract = algorithm._Algorithm__find_overlapping_contracts(
        changed_contract=changed_contract, related_contracts=backend_related_contracts
    )

    # 3H product is filtered out, all the others are overlapping with the longest contract
    true_related_contracts = [
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T00:30+00:00")),
        Contract(delivery_start=datetime.fromisoformat("2022-12-01T00:15+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T00:30+00:00")),
    ]

    assert related_contracts == true_related_contracts
    assert longest_contract == Contract(
        delivery_start=datetime.fromisoformat("2022-12-01T00:00+00:00"), delivery_end=datetime.fromisoformat("2022-12-01T00:30+00:00")
    )

from datetime import datetime


def test_is_delivery_durations_compliant_parameter_is_not_set_should_return_true(algorithm):
    """
    This test ensures, that if the delivery_durations parameter is not set, then the check will return true.
    """

    algorithm.delivery_durations = []

    delivery_start = datetime.fromisoformat("2022-12-01T00:00+00:00")
    delivery_end = datetime.fromisoformat("2022-12-01T02:00+00:00")

    assert algorithm._Algorithm__is_delivery_durations_compliant(delivery_start=delivery_start, delivery_end=delivery_end)


def test_is_delivery_durations_compliant_parameter_is_set_to_accept_qh_h_products_h_product_is_passed_should_return_true(algorithm):
    """
    This test ensures, that if the delivery_durations parameter is set to accept QH and H products, and the H product is passed,
    then the check will return True.
    """

    algorithm.delivery_durations = [15, 60]

    delivery_start = datetime.fromisoformat("2022-12-01T00:00+00:00")
    delivery_end = datetime.fromisoformat("2022-12-01T01:00+00:00")

    assert algorithm._Algorithm__is_delivery_durations_compliant(delivery_start=delivery_start, delivery_end=delivery_end)


def test_is_delivery_durations_compliant_parameter_is_set_to_accept_qh_h_products_hh_product_is_passed_should_return_false(algorithm):
    """
    This test ensures, that if the delivery_durations parameter is set to accept QH and H products, and the HH product is passed,
    then the check will return False.
    """

    algorithm.delivery_durations = [15, 60]

    delivery_start = datetime.fromisoformat("2022-12-01T00:00+00:00")
    delivery_end = datetime.fromisoformat("2022-12-01T00:30+00:00")

    assert algorithm._Algorithm__is_delivery_durations_compliant(delivery_start=delivery_start, delivery_end=delivery_end) is False

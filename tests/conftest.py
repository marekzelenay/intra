import logging
import os
from unittest import mock

import pytest

from src.algorithm_base_class.algorithm import Algorithm


@pytest.fixture
@mock.patch.dict(
    os.environ,
    {"ALGORITHM_ID": "test", "INSTANCE_ID": "test", "API_KEY": "test", "URL_API": "test", "URL_WEBSOCKET": "test", "ALGO_LOG_LEVEL": "ERROR"},
)
def algorithm():
    """Algorithm fixture."""
    algorithm = Algorithm()
    algorithm.logger = logging.getLogger()
    algorithm.portfolio_ids = ["test"]
    algorithm.delivery_area = "test"
    algorithm.signals_api = mock.AsyncMock()
    algorithm.orders_api = mock.AsyncMock()

    return algorithm

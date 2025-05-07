import logging
from unittest.mock import patch, MagicMock

import pytest
import requests

from debt_checker.api_client import get_debt_amount, _handle_api_response


@pytest.fixture
def mock_logger(mocker):
    return mocker.MagicMock(spec=logging.Logger)

@pytest.fixture
def api_token():
    return "test_token"

def test_handle_api_response_successful_debt(mock_logger):
    response_data = {
        "status": 200,
        "count": 1,
        "records": [{"sum": "1500.50"}]
    }
    result = _handle_api_response(response_data, "123", mock_logger)
    assert result == 1500.50
    mock_logger.info.assert_not_called()

def test_handle_api_response_no_debt(mock_logger):
    response_data = {
        "status": 200,
        "count": 0
    }
    result = _handle_api_response(response_data, "123", mock_logger)
    assert result == 0.0
    mock_logger.info.assert_called_once()

def test_handle_api_response_token_no_access(mock_logger):
    response_data = {
        "error": "602",
        "message": "Access denied"
    }
    result = _handle_api_response(response_data, "123", mock_logger)
    assert result == "TOKEN_NO_ACCESS"
    mock_logger.error.assert_called_once()

def test_handle_api_response_invalid_data(mock_logger):
    response_data = {
        "status": 200,
        "count": 1,
        "records": [{"invalid": "data"}]
    }
    result = _handle_api_response(response_data, "123", mock_logger)
    assert result is None
    mock_logger.error.assert_called_once()


def test_get_debt_amount_success(mock_logger, api_token):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "status": 200,
        "count": 1,
        "records": [{"sum": "1500.50"}]
    }

    with patch('requests.get', return_value=mock_response):
        result = get_debt_amount("123", api_token, mock_logger)

    assert result == 1500.50

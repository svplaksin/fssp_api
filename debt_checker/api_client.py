"""API client module for interacting with FSSP debt checking service."""

import json
import logging
import time
from typing import Union

import requests
from tenacity import retry, stop_after_attempt, wait_exponential


def _handle_api_response(data: dict,
                         number: str,
                         logger: logging.Logger
                         ) -> Union[float, str, None]:
    """Process successful API response and extract debt information.

    Args:
        data: Parsed JSON response from API
        number: Taxpayer number being checked
        logger: Logger instance for reporting

    Returns:
        float: Debt amount if found (0.0 means no debt)
        str: Error code if token issues ('TOKEN_NO_ACCESS' or 'TOKEN_NO_MONEY')
        None: For invalid/missing data

    """
    if "error" in data:
        if data["error"] == "602":
            logger.error(f'API access denied for {number}: {data["message"]}')
            return "TOKEN_NO_ACCESS"
        if data["error"] == "498":
            logger.error(f'Insufficient balance for {number}: {data["message"]}')
            return "TOKEN_NO_MONEY"
        return None

    if data.get("status") != 200:
        logger.warning(f'Non-200 status for {number}: {data.get("status")}')
        return None

    if data.get("count") == 1:
        try:
            return float(data["records"][0]["sum"])
        except (KeyError, ValueError, IndexError) as e:
            logger.error(f"Data parsing failed for {number}: {e!s}")
            return None
    elif data.get("count") == 0:
        logger.info(f"No debt found for {number}")
        return 0.0
    return None


def _log_api_error(error: Exception, number: str, logger: logging.Logger) -> None:
    """Log API processing errors with appropriate level."""
    if isinstance(error, json.JSONDecodeError):
        logger.error(f"Invalid JSON for {number}: {error}")
    elif isinstance(error, requests.exceptions.RequestException):
        logger.error(f"Request failed for {number}: {error}")
    elif isinstance(error, KeyError):
        logger.error(f"Missing expected keys in response for {number}: {error}")
    else:
        logger.exception(f"Unexpected error processing {number}")


def get_debt_amount(number: str,
                    api_token: str,
                    logger: logging.Logger,
                    timeout: int = 60) -> Union[float, str, None]:
    """Fetch debt amount for an enforcement process number from FSSP API.

    Args:
        number: Taxpayer identification number
        api_token: Authentication token
        logger: Configured logger instance
        timeout: Request timeout in seconds

    Returns:
        float: Debt amount (0.0 = no debt)
        str: Token error code
        None: Processing error occurred

    """
    api_url = f"https://api-cloud.ru/api/fssp.php?type=ip&number={number}&token={api_token}"

    try:
        start_time = time.time()
        response = requests.get(api_url, timeout=timeout)
        response.raise_for_status()

        logger.info(f"API request for {number} took {time.time() - start_time:.2f}s")
        return _handle_api_response(response.json(), number, logger)

    except Exception as e:
        _log_api_error(e, number, logger)
        return None

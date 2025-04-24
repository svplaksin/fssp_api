"""API client module for interacting with FSSP debt checking service."""

import json
import logging
import time

import requests
from tenacity import retry, stop_after_attempt, wait_exponential


@retry(
    stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10)
)  # Retry 3 times with exponential backoff
def get_debt_amount(number: str, api_token: str, logger: logging.Logger, timeout: int = 60):
    """Fetch debt amount for a given enforcement procedure number (ITN) from FSSP API.

    Args:
        number: Enforcement procedure number as string
        api_token: Authentication token for API access
        logger: Configured logger instance for error tracking
        timeout: Request timeout in seconds (default: 60)

    Returns:
        Union[float, str, None]:
            - float: Debt amount if found (0.0 means no debt)
            - 'TOKEN_NO_ACCESS' if API access denied
            - 'TOKEN_NO_MONEY' if account balance insufficient
            - None for any other errors

    Raises:
        requests.exceptions.RequestException: On network-related errors
        ValueError: If debt amount cannot be converted to float

    Example:
        >>> debt = get_debt_amount('1234/56/7890-ИП', 'your_api_token', logger)
        >>> if isinstance(debt, float):
        ...     print(f"Debt amount: {debt}")

    """
    api_url = (
        f"https://api-cloud.ru/api/fssp.php?type=ip&number={number}&token={api_token}"
    )
    start_time = time.time()
    try:
        response = requests.get(api_url, timeout=timeout)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        end_time = time.time()
        elapsed_time = end_time - start_time

        logger.info(f"API request for number {number} took {elapsed_time:.2f} seconds")

        if "error" in data:
            if data["error"] == "602":
                logger.error(
                    f"API access denied for number {number}. Message: {data['message']}"
                )
                return "TOKEN_NO_ACCESS"
            elif data["error"] == "498":
                logger.error(
                    f"Not enough money on balance for number {number}. Message: {data['message']}"
                )
                return "TOKEN_NO_MONEY"
        if data["status"] == 200:
            if data["count"] == 1:
                debt = float(data["records"][0]["sum"])
                return debt  # Extract total debt amount
            elif data["count"] == 0:
                logger.info(
                    f"No debt found for number {number}. (Not in FSSP database)"
                )
                return 0
        else:
            logger.warning(f"API returned a non-200 status for number {number}")
            return None
    except json.decoder.JSONDecodeError:
        logger.error(
            f"JSONDecodeError: Could not decode JSON response for number {number}"
        )
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed for number {number}: {e}")
        return None
    except KeyError as e:
        logger.error(
            f"KeyError: {e}. API response structure might have changed for number {number}"
        )
        return None
    except ValueError as e:
        logger.error(
            f"ValueError. Could not convert debt amount to float for number {number}: {e}"
        )
        return None
    except Exception as e:
        logger.exception(f"An unexpected error occurred for number {number}: {e}")
        return None

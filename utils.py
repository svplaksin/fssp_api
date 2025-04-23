import os
import requests
from dataclasses import dataclass
from typing import Optional, Union

import pandas as pd

from api_client import get_debt_amount

# Constants
TEMP_FILES_DIR = "temp_files" # Define the temporary files directory (relative to the current directory)
FINAL_FILE = 'numbers_with_debt.xlsx' # Name for the final file
SAVE_INTERVAL = 10
API_TIMEOUT = 60
API_DELAY = 0.5
MAX_THREADS = 20

def save_dataframe_to_excel(df, filename, index=False, logger=None):
    try:
        df.to_excel(filename, index=index)
        if logger:
            logger.info(f'Data saved to {filename}')
    except Exception as e:
        if logger:
            logger.exception(f'Error saving to Excel file: {e}')
        raise e


def save_temp_data(data, counter, logger, temp_files_dir):
    """Save data to temporary CSV file"""
    try:
        if data:
            filename = f'numbers_with_debt_temp_{counter}.csv'
            full_path = os.path.join(temp_files_dir, filename)
            temp_df = pd.DataFrame(data)
            temp_df.to_csv(full_path, index=False)
            logger.info(f'Data saved to {full_path} after processing {counter} API calls')
        else:
            logger.info('No data to save')
    except Exception as e:
        logger.exception(f"Error saving temporary file: {e}")

@dataclass
class ProcessResult:
    index: int
    number: str
    debt_amount: Optional[float]
    error: Optional[str] = None

def process_row(index: int,
                row:pd.Series,
                api_token: str,
                logger,
                stop_processing: bool = False
            ) -> Union[ProcessResult, None]:
    """Processes a single row of the DataFrame"""
    if stop_processing:
        logger.info(f'process for index {index} interrupted.')
        return None

    num = str(row.iloc[0])
    existing_debt = row.get('Debt Amount', pd.NA)

    if not pd.isna(existing_debt):
        logger.info(f'Debt amount already exists for number {num} at index {index}. Skipping API call')
        return None
    try:
        debt_amount = get_debt_amount(num, api_token, logger, API_TIMEOUT)

        if debt_amount in ('TOKEN_NO_ACCESS', 'TOKEN_NO_MONEY'):
            logger.error(f'Stopping processing due to API error: {debt_amount}')
            return ProcessResult(index, num, debt_amount, None)

        logger.info(f'Found and updated debt amount for number {num} at index {index}: {debt_amount}')
        return ProcessResult(index, num, debt_amount)
    except requests.exceptions.RequestException as e:
        logger.error(f'Network error during API call for number {num} at index {index}: {e}')
        return ProcessResult(index, num, None, 'NETWORK_ERROR')
    except Exception as e:
        logger.error(f'Network error during API call for number {num} at index {index}: {e}')
        return ProcessResult(index, num, None, 'UNKNOWN_ERROR')

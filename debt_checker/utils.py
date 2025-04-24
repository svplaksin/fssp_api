"""Core utilities for debt processing and data handling."""

import concurrent.futures
import logging
import os
import signal
import sys
import threading
from dataclasses import dataclass
from typing import List, Optional, Union

import pandas as pd
import requests
from tqdm.auto import tqdm

from debt_checker.api_client import get_debt_amount

# Constants
TEMP_FILES_DIR = "temp_files"  # Define the temporary files directory
FINAL_FILE = "numbers_with_debt.xlsx"  # Name for the final file
SAVE_INTERVAL = 10
API_TIMEOUT = 60
API_DELAY = 0.5
MAX_THREADS = 20

# Thread safe stop flag
stop_event = threading.Event()


# Signal handler
def setup_signal_handler(
    temp_data: List,
    processed_data: List,
    counter: int,
    logger: logging.Logger,
    temp_files_dir: str,
) -> None:
    """Configure the signal handler for graceful shutdown.

    Must be called after variables are initialized but before processing starts.
    """

    def signal_handler(sig, frame):
        stop_event.set()
        logger.info("Received termination signal. Saving data...")
        save_temp_data(temp_data + processed_data, counter, logger, temp_files_dir)
        logger.info("Exiting...")

    signal.signal(signal.SIGINT, signal_handler)

def load_input_data(file_path: str, logger: logging.Logger) -> pd.DataFrame:
    """Load and validate input Excel file with numbers.

    Returns:
         pd.DataFrame: Loaded dataframe with 'Debt Amount' column
    Raises:
        SystemExit: If file cannot be loaded or is invalid.

    """
    try:
        df = pd.read_excel(file_path)
        logger.info(f"File loaded successfully. Found {len(df)} numbers")

        # Ensure required column exists
        if "Debt Amount" not in df.columns:
            df["Debt Amount"] = pd.NA
            logger.info('Created "Debt Amount" column for missing values')

        return df

    except FileNotFoundError:
        logger.error(f"Input file not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Error loading input file: {e}")
        sys.exit(1)


@dataclass
class ProcessResult:
    """Stores debt check results for a single enforcement number."""

    index: int
    number: str
    debt_amount: Optional[float]
    error: Optional[str] = None


def process_row(
    index: int, row: pd.Series, api_token: str, logger
) -> Union[ProcessResult, None]:
    """Process single row of enforcement numbers data and check for debts.

    Args:
        index: Row index from source DataFrame
        row: Pandas Series containing enforcement numbers data
        api_token: API authentication token
        logger: Configured logger instance

    Returns:
        Union[ProcessResult, str, None]:
            - ProcessResult: Contains debt data if processed
            - 'API_ERROR': For token/auth failures
            - None: If skipped (existing data) or interrupted

    Note:
        Uses global stop_event for graceful interruption handling

    """
    if stop_event.is_set():
        logger.info(f"process for index {index} interrupted.")
        return None

    num = str(row.iloc[0])
    existing_debt = row.get("Debt Amount", pd.NA)

    if not pd.isna(existing_debt):
        logger.info(
            f"Debt amount already exists for"
            f"number {num} at index {index}. Skipping API call"
        )
        return None

    try:
        debt_amount = get_debt_amount(num, api_token, logger, API_TIMEOUT)

        if debt_amount in ("TOKEN_NO_ACCESS", "TOKEN_NO_MONEY"):
            logger.error(f"Stopping processing due to API error: {debt_amount}")
            stop_event.set()
            return ProcessResult(index, num, debt_amount, None)
            # return 'API_ERROR'

        logger.info(
            f"Found and updated debt amount"
            f"for number {num} at index {index}: {debt_amount}"
        )
        return ProcessResult(index, num, debt_amount)
    except requests.exceptions.RequestException as e:
        logger.error(
            f"Network error during API call for number {num} at index {index}: {e}"
        )
        return ProcessResult(index, num, None, "NETWORK_ERROR")
    except Exception as e:
        logger.error(
            f"Network error during API call for number {num} at index {index}: {e}"
        )
        return ProcessResult(index, num, None, "UNKNOWN_ERROR")


def process_rows_concurrently(
    df, api_token, max_threads, save_interval, temp_dir, logger
):
    """Process DataFrame rows concurrently with ThreadPoolExecutor.

    Args:
        df: Input DataFrame containing enforcement numbers
        api_token: API authentication token
        max_threads: Maximum worker threads to use
        save_interval: Save progress every N records
        temp_dir: Directory for temporary data saves
        logger: Configured logger instance

    Returns:
        tuple: (processed_data, counter)
            processed_data: List of ProcessResult objects
            counter: Total records processed

    Note:
        Implements periodic saving and graceful interruption handling

    """
    processed_data = []
    counter = 0

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = {
                executor.submit(process_row, index, row, api_token, logger): index
                for index, row in df.iterrows()
            }
            for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(df),
                desc="Processing...",
                unit="number",
            ):
                if stop_event.is_set():
                    logger.info("Exiting from the process loop...")
                    break

                # index = futures[future]
                result = future.result()

                if result == "API_ERROR":
                    logger.error("Stopping processing due to API error")
                    break

                if result:
                    processed_data.append(result)
                    counter += 1

                if counter % save_interval == 0:
                    save_temp_data(processed_data.copy(), counter, logger, temp_dir)
                    processed_data.clear()

        return processed_data, counter
    except Exception as e:
        logger.exception(f"Error occurred during processing: {e}")
        raise


def save_temp_data(data, counter, logger, temp_files_dir):
    """Save data to temporary CSV file."""
    try:
        if data:
            filename = f"numbers_with_debt_temp_{counter}.csv"
            full_path = os.path.join(temp_files_dir, filename)
            temp_df = pd.DataFrame(data)
            temp_df.to_csv(full_path, index=False)
            logger.info(
                f"Data saved to {full_path} after processing {counter} API calls"
            )
        else:
            logger.info("No data to save")
    except Exception as e:
        logger.exception(f"Error saving temporary file: {e}")


def merge_temp_files(temp_dir, original_df, final_path, logger):
    """Merge all temporary files into final Excel file."""
    try:
        all_temp_files = [
            os.path.join(temp_dir, f)
            for f in os.listdir(temp_dir)
            if f.startswith("numbers_with_debt_temp_") and f.endswith(".csv")
        ]

        if not all_temp_files:
            logger.warning("No temporary CSV files found to merge")
            return None

        all_dfs = [pd.read_csv(temp) for temp in all_temp_files]
        merged_df = pd.concat(all_dfs, ignore_index=True)

        original_df["number"] = original_df["number"].astype(str)
        merged_df["number"] = merged_df["number"].astype(str)

        final_df = pd.merge(original_df, merged_df, on="number", how="left")
        final_df["Debt Amount"] = final_df["debt_amount"].fillna(
            final_df["Debt Amount"]
        )
        final_df = final_df.drop(
            columns=[col for col in final_df if col not in ("number", "Debt Amount")]
        )

        save_dataframe_to_excel(final_df, final_path, index=False, logger=logger)
        logger.info(f"Merged data from temporary files and saved to {final_path}")
        return final_df
    except Exception as e:
        logger.exception(f"Error occurred during merging temporary files: {e}")
        raise


def save_dataframe_to_excel(df, filename, index=False, logger=None):
    """Save DataFrame to Excel file with error logging.

    Args:
        df: Data to save
        filename: Output path
        index: Write row index if True
        logger: Optional logger instance

    """
    try:
        df.to_excel(filename, index=index)
        if logger:
            logger.info(f"Data saved to {filename}")
    except Exception as e:
        if logger:
            logger.exception(f"Error saving to Excel file: {e}")
        raise e

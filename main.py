import concurrent.futures
import os
import signal
import sys

import pandas as pd
import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

from api_client import get_debt_amount
from logging_config import setup_logging
from utils import (save_dataframe_to_excel, save_temp_data,
                   signal_handler_multiprocessing)

# Set up logging
logger = setup_logging()

# Load environment variables
load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')
if not API_TOKEN:
    logger.error('API_TOKEN is not found. Please set the API_TOKEN environment variable.')
    sys.exit(1)

# Constants
TEMP_FILES_DIR = "temp_files" # Define the temporary files directory (relative to the current directory)
FINAL_FILE = 'numbers_with_debt.xlsx' # Name for the final file
SAVE_INTERVAL = 20
API_TIMEOUT = 60
API_DELAY = 0.3
MAX_THREADS = 20

# Make sure the temporary files directory exists
os.makedirs(TEMP_FILES_DIR, exist_ok=True)

# Initialize variables before signal handler
counter = 0
temp_data = []
processed_data = []

# Signal handler
signal.signal(signal.SIGINT,
              lambda sig,
                frame: signal_handler_multiprocessing(sig, frame, temp_data, processed_data, counter, logger, TEMP_FILES_DIR)
              )

# Retry logic for API calls
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10)) # Retry 3 times with exponential backoff
def get_debt_amount_with_retry(number, api_token, logger, timeout):
    """Wrapper for get_debt_amount with retry logic"""
    return get_debt_amount(number, api_token, logger, timeout)


def process_row(index, row, API_TOKEN):
    """Processes a single row of the DataFrame"""
    num = str(row.iloc[0])
    existing_debt = row.get('Debt Amount', pd.NA)

    if pd.isna(existing_debt):
        try:
            debt_amount = get_debt_amount_with_retry(num, API_TOKEN, logger, API_TIMEOUT)

            if debt_amount == 'TOKEN_NO_ACCESS' or debt_amount == 'TOKEN_NO_MONEY':
                logger.error(f'Stopping processing due to API error: {debt_amount}')
                return 'API_ERROR'
            elif debt_amount is not None:
                logger.info(f'Found and updated debt amount for number {num} at index {index}: {debt_amount}')
                return {'index': index, 'number': num, 'debt_amount': debt_amount}
            else:
                logger.info(f'No debt found for number {num} at index {index}. Setting to None')
                return {'index': index, 'number': num, 'debt_amount': None}
        except requests.exceptions.RequestException as e:
            logger.error(f'Network error during API call for number {num} at index {index}: {e}')
            return {'index': index, 'number': num, 'debt_amount': None}
        except Exception as e:
            logger.error(f'Unexpected error during API call for number {num} at index {index}: {e}')
            return {'index': index, 'number': num, 'debt_amount': None}
    else:
        logger.info(f'Debt amount already exists for number {num} at index {index}. Skipping API call')
        return None
#Load the Excel file
try:
    df = pd.read_excel('numbers.xlsx')
    logger.info(f'File loaded successfully. Found {len(df)} numbers')
except FileNotFoundError:
    logger.error('Error: Numbers file not found')
    sys.exit(1)
except Exception as e:
    logger.exception(f'Error reading Excel file: {e}')
    sys.exit(1)

# Ensure the 'Debt Amount' column exists
if 'Debt Amount' not in df.columns:
    df['Debt Amount'] = pd.NA  # Initialize with pd.NA (missing values)
    logger.info("Created new column 'Debt Amount'")

# Main processing using multiprocessing
try:
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(process_row, index, row, API_TOKEN): index for index, row in df.iterrows()}

        # Process results as they become available
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(df), desc='Processing...', unit='numbers'):
            index = futures[future]
            result = future.result()

            if result == 'API_ERROR':
                logger.error('Stopping processing due to API error')
                break
            if result:
                processed_data.append(result)
                counter += 1
            if counter % SAVE_INTERVAL == 0 and counter != 0:
                save_temp_data(temp_data + processed_data, counter, logger, TEMP_FILES_DIR)
                temp_data = []
                processed_data = []
except Exception as e:
    logger.exception(f'Error occurred during processing: {e}')
finally:
    logger.info('Saving before exiting...')
    # Save any remaining data in temp_data
    save_temp_data(temp_data + processed_data, counter, logger, TEMP_FILES_DIR)  # Save remaining data

    # Merge temp CSV files into final Excel
    all_temp_files = [os.path.join(TEMP_FILES_DIR, f) for f in os.listdir(TEMP_FILES_DIR) if f.startswith('numbers_with_debt_temp_') and f.endswith('.csv')]
    if all_temp_files:
        try:
            all_dfs = [pd.read_csv(f) for f in all_temp_files]
            merged_df = pd.concat(all_dfs, ignore_index=True)
            # Merge data on 'number' column
            df['number'] = df['number'].astype(str)
            merged_df['number'] = merged_df['number'].astype(str)
            final_df = pd.merge(df, merged_df[['number', 'debt_amount']],on='number', how='left')
            final_df['Debt Amount'] = final_df['debt_amount'].fillna(final_df['Debt Amount'])
            final_df = final_df.drop('debt_amount', axis=1) # Drop the debt_amount column

            save_dataframe_to_excel(final_df, FINAL_FILE, index=False, logger=logger)
            logger.info(f'Merged data from temporary files and saved to {FINAL_FILE}')
            # --- OPTIONAL: Remove the temporary CSV files (cleanup) ---
            # for temp_file in all_temp_files:
            #     os.remove(temp_file)
            #     logger.info('Removed temporary CSV files')
        except Exception as e:
            logger.exception(f'Error merging and saving temporary files: {e}')
    else:
        logger.warning('No temporary CSV files found to merge')

    logger.info('Exiting...')
    sys.exit(0)

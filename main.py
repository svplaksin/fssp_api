import os
import signal
import sys
import time

import pandas as pd
import requests.exceptions
from dotenv import load_dotenv
from tqdm import tqdm

from tenacity import retry, stop_after_attempt, wait_exponential

from api_client import get_debt_amount
from logging_config import setup_logging
from utils import save_dataframe_to_excel, save_temp_data, signal_handler

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
API_DELAY = 0.1

# Make sure the temporary files directory exists
os.makedirs(TEMP_FILES_DIR, exist_ok=True)

# Initialize variables before signal handler
counter = 0
temp_data = []

# Signal handler
signal.signal(signal.SIGINT,
              lambda sig,
                frame: signal_handler(sig, frame, temp_data, counter, logger, TEMP_FILES_DIR)
              )

# Retry logic for API calls
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10)) # Retry 3 times with exponential backoff
def get_debt_amount_with_retry(number, api_token, logger, timeout):
    """Wrapper for get_debt_amount with retry logic"""
    return get_debt_amount(number, api_token, logger, timeout)

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

# Main processing
try:
    for index, row in tqdm(df.iterrows(), total=len(df), desc='Processing...', unit='number'):
        num = str(row.iloc[0])  # First column: Number
        existing_debt = row.iloc[1] # Second column: Existing debt amount

        # Check if a debt amount already exists
        if pd.isna(existing_debt): # Check if the value is NaN (missing)
            try:
                debt_amount = get_debt_amount_with_retry(num, API_TOKEN, logger, API_TIMEOUT)

                if debt_amount == 'TOKEN_NO_ACCESS' or debt_amount == 'TOKEN_NO_MONEY':
                    logger.error(f'Stopping processing due to API error: {debt_amount}')
                    break # Exit the loop, to proceed to the finally block
                elif debt_amount is not None:
                    df.loc[index, 'Debt Amount'] = debt_amount  # Update the DataFrame
                    temp_data.append({'index': index, 'number': num, 'debt_amount': debt_amount}) # Add the row to the temp_data list
                    logger.info(f'Found and updated debt amount for number {num} at index {index}: {debt_amount}')
                    counter += 1 # Increment a counter ONLY when a new API request is made
                else:
                    df.loc[index, 'Debt Amount'] = pd.NA  # Keep it None if Error occurred
                    temp_data.append({'index': index, 'number': num, 'debt_amount': None})
                    logger.info(f'No debt found for index {index}, setting to None')
            except requests.exceptions.RequestException as e:
                logger.error(f'Network error during API call: for number {num}: {e}')
                df.loc[index, 'Debt Amount'] = pd.NA # Set to NA in case of error
                temp_data.append({'index': index, 'number': num, 'debt_amount': None})
            except Exception as e:
                logger.exception(f'Unexpected error during API call: for number {num}: {e}')
                df.loc[index, 'Debt Amount'] = pd.NA
                temp_data.append({'index': index, 'number': num, 'debt_amount': None})

        else:
            logger.info(f'Debt amount already exists for number {num}. Skipping API call')

        time.sleep(API_DELAY)  # Add a small delay to avoid overwhelming the API

        # Save temporary data every SAVE_INTERVAL API calls
        if counter % SAVE_INTERVAL == 0 and counter > 0:
            save_temp_data(temp_data, counter, logger, TEMP_FILES_DIR)
            temp_data = [] # Reset temp_data after saving

except Exception as e: # Catch any exception during processing
    logger.exception(f'Error occurred during processing: {e}')
finally:
    logger.info('Saving before exiting...')
    save_temp_data(temp_data, counter, logger, TEMP_FILES_DIR)
    try:
        save_dataframe_to_excel(df, FINAL_FILE, index=False, logger=logger)
        logger.info(f'Final file saved into {FINAL_FILE}')
    except Exception as e:
        logger.exception(f'Error saving final Excel file: {e}')
    logger.info('Exiting...')
    sys.exit(0)

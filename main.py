import concurrent.futures
import os
import signal
import sys

import pandas as pd
# import requests
from dotenv import load_dotenv
from tqdm import tqdm

# from api_client import get_debt_amount
from logging_config import setup_logging
from utils import save_dataframe_to_excel, save_temp_data, process_row, TEMP_FILES_DIR, MAX_THREADS, SAVE_INTERVAL, \
    FINAL_FILE

# Set up logging
logger = setup_logging()

# Load environment variables
load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')
if not API_TOKEN:
    logger.error('API_TOKEN is not found. Please set the API_TOKEN environment variable.')
    sys.exit(1)

# Make sure the temporary files directory exists
os.makedirs(TEMP_FILES_DIR, exist_ok=True)

# Initialize variables before signal handler
counter = 0
temp_data = []
processed_data = []

# Thread termination flag
stop_processing = False

# Signal handler
def signal_handler_multiprocessing(sig, frame):
    global stop_processing
    stop_processing = True
    logger.info('Received termination signal. Saving data...')
    save_temp_data(temp_data + processed_data, counter, logger, TEMP_FILES_DIR)
    logger.info('Exiting...')

signal.signal(signal.SIGINT, signal_handler_multiprocessing)

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
        # Prepare tasks for the thread pool
        futures = {executor.submit(process_row, index, row, API_TOKEN, logger, stop_processing): index for index, row in df.iterrows()}

        # Process results as they become available
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(df), desc='Processing...', unit='number'):
            if stop_processing:
                logger.info('Exiting from the process loop')
                break

            index = futures[future]
            result = future.result()

            if result == 'API_ERROR':
                logger.error('Stopping processing due to API error')
                stop_processing = True
                break
            if result:
                processed_data.append(result)
                counter += 1

            if counter % SAVE_INTERVAL == 0:
                save_temp_data(temp_data + processed_data, counter, logger, TEMP_FILES_DIR)
                temp_data = []
                processed_data = []
except Exception as e:
    logger.exception(f'Error occurred during processing: {e}')
finally:
    logger.info('Saving before exiting...')
    # Save any remaining data in temp_data
    save_temp_data(temp_data + processed_data, counter, logger, TEMP_FILES_DIR)  # Save remaining data

    # Shutdown the executor
    executor.shutdown(wait=True) # Shutdown the pool and wait

    # Merge temp CSV files into final Excel
    all_temp_files = [os.path.join(TEMP_FILES_DIR, f) for f in os.listdir(TEMP_FILES_DIR) if f.startswith('numbers_with_debt_temp_') and f.endswith('.csv')]
    if all_temp_files:
        try:
            all_dfs = []
            for temp_file in all_temp_files:
                try:
                    temp_df = pd.read_csv(temp_file)
                    if 'number' not in temp_df.columns:
                        logger.info(f'Skipping {temp_file} - Missing "numbers" column')
                        continue
                    all_dfs.append(temp_df)
                except Exception as e:
                    logger.error(f'Error reading or processing {temp_file}: {e}')
            if not all_dfs:
                logger.warning('No valid temporary CSV files to merge.')
            else:
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

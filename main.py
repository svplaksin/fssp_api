import os
import sys

import pandas as pd
from dotenv import load_dotenv

from logging_config import setup_logging
from utils import (FINAL_FILE, MAX_THREADS, SAVE_INTERVAL, TEMP_FILES_DIR,
                   setup_signal_handler, merge_temp_files, process_rows_concurrently, save_temp_data)


def main():

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
    temp_data = []
    processed_data = []
    counter = 0

    setup_signal_handler(
        temp_data=temp_data,
        processed_data=processed_data,
        counter=counter,
        logger=logger,
        temp_files_dir=TEMP_FILES_DIR,
    )
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
        processed_data, counter, stop_processing = process_rows_concurrently(
            df=df,
            api_token=API_TOKEN,
            max_threads=MAX_THREADS,
            save_interval=SAVE_INTERVAL,
            temp_dir=TEMP_FILES_DIR,
            logger=logger
        )

    finally:
        # Final cleanup and saving
        logger.info('Saving before exiting...')
        save_temp_data(processed_data, counter, logger, TEMP_FILES_DIR)

        # Merge temp files
        merge_temp_files(
            temp_dir=TEMP_FILES_DIR,
            original_df=df,
            final_path=FINAL_FILE,
            logger=logger
        )

        logger.info('Exiting...')
        sys.exit(0)

if __name__ == '__main__':
    main()

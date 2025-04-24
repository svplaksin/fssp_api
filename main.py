"""Main execution module for debt checker application."""

import os
import sys

from dotenv import load_dotenv

from logging_config import setup_logging
from utils import (
    FINAL_FILE,
    MAX_THREADS,
    SAVE_INTERVAL,
    TEMP_FILES_DIR,
    load_input_data,
    merge_temp_files,
    process_rows_concurrently,
    save_temp_data,
    setup_signal_handler,
)


def main():
    """Orchestrate the debt checking workflow.

    Workflow:
        1. Load environment configuration
        2. Setup logging and signal handling
        3. Load input data
        4. Process records concurrently
        5. Save final results

    Environment Variables:
        API_TOKEN: Required authentication token

    Exit Codes:
        0: Success
        1: Error
    """
    logger = setup_logging()
    load_dotenv()

    # Configuration
    api_token = os.getenv("API_TOKEN")
    if not api_token:
        logger.error(
            "API_TOKEN is not found. Please set the API_TOKEN environment variable."
        )
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

    df = None
    # Data pipeline
    try:
        df = load_input_data("numbers.xlsx", logger)
        processed_data, counter = process_rows_concurrently(
            df=df,
            api_token=api_token,
            max_threads=MAX_THREADS,
            save_interval=SAVE_INTERVAL,
            temp_dir=TEMP_FILES_DIR,
            logger=logger,
        )

    finally:
        logger.info("Saving before exiting...")
        save_temp_data(processed_data, counter, logger, TEMP_FILES_DIR)

        # Merge temp files
        merge_temp_files(
            temp_dir=TEMP_FILES_DIR,
            original_df=df,
            final_path=FINAL_FILE,
            logger=logger,
        )

        logger.info("Exiting...")
        sys.exit(0)


if __name__ == "__main__":
    main()

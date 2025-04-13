import os
import sys

import pandas as pd


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
    """Save data to temporary file"""
    try:
        if data:
            filename = f'numbers_with_debt_temp_{counter}.xlsx'
            full_path = os.path.join(temp_files_dir, filename)
            temp_df = pd.DataFrame(data)
            save_dataframe_to_excel(temp_df, full_path, index=False, logger=logger)
            logger.info(f'Data saved to {full_path} after processing {counter} API calls')
        else:
            logger.info('No data to save')
    except Exception as e:
        logger.exception(f"Error saving temporary file: {e}")


def signal_handler(sig, frame, temp_data, counter, logger,temp_files_dir):
    logger.info('Received termination signal. Saving data...')
    save_temp_data(temp_data, counter, logger, temp_files_dir)
    logger.info('Exiting...')
    sys.exit(0)

import json
import os
from dotenv import load_dotenv
import pandas as pd
import requests
import time
from logging_config import setup_logging
from api_client import get_debt_amount


# Set up logging
logger = setup_logging()

# Load environment variables
load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')
if not API_TOKEN:
    logger.error('API_TOKEN is not found')
    exit()

# Define the temporary files directory (relative to the current directory)
TEMP_FILES_DIR = "temp_files"

# Make sure the temporary files directory exists
os.makedirs(TEMP_FILES_DIR, exist_ok=True)

#Load the Excel file
try:
    df = pd.read_excel('numbers.xlsx')
    logger.info(f'File loaded successfully. Found {len(df)} numbers')
except FileNotFoundError:
    logger.error('Error: Numbers file not found')
    exit()
except Exception as e:
    logger.exception(f'Error reading Excel file: {e}')
    exit()

# Ensure the 'Debt Amount' column exists
if 'Debt Amount' not in df.columns:
    df['Debt Amount'] = None  # Initialize with None
    logger.info("Created new column 'Debt Amount'")

# Set the interval for saving DataFrame
SAVE_INTERVAL = 5 # Save every 20 requests
counter = 0

# Prepare a list to store the rows for temporary saving
temp_data = []

#Iterate through the Dataframe rows
for index, row in df.iterrows():
    num = str(row.iloc[0])  # First column: Number
    existing_debt = row.iloc[1] # Second column: Existing debt amount

    # Check if a debt amount already exists
    if pd.isna(existing_debt): # Check if the value is NaN (missing)
        debt_amount = get_debt_amount(num, API_TOKEN, logger)

        if debt_amount is not None:
            df.loc[index, 'Debt Amount'] = debt_amount  # Update the DataFrame
            temp_data.append(df.loc[[index]]) # Add the row to the temp_data list
            logger.info(f'Found and updated debt amount for number {num} at index {index}: {debt_amount}')
            counter += 1 # Increment a counter ONLY when a new API request is made
        else:
            df.loc[index, 'Debt Amount'] = None  # Keep it None if Error occurred
            temp_data.append(df.loc[[index]])
            logger.info(f'No debt found for index {index}, setting to None')

        time.sleep(0.5)  # Add a small delay to avoid overwhelming the API

    # Save temporary data every SAVE_INTERVAL API calls
    if counter % SAVE_INTERVAL == 0 and counter > 0:
        try:
            filename = f'numbers_with_debt_temp_{counter}.xlsx'
            # Create the full path to the temporary file
            full_path = os.path.join(TEMP_FILES_DIR, filename)

            temp_df = pd.concat(temp_data) # Create a DataFrame from the temp_data list
            temp_df.to_excel(full_path, index=True)

            logger.info(f'Data saved to {full_path} after processing {counter} API calls.')

            temp_data = [] # Clear the temp_data for next save
        except Exception as e:
            logger.exception(f'Error saving to temporary Excel file: {e}')

# Save the final result after processing all rows
try:
    df.to_excel('numbers_with_debt.xlsx', index=False)
    logger.info(f'Data saved to numbers_with_debt.xlsx')
except Exception as e:
    logger.exception(f'Error saving to Excel file: {e}')

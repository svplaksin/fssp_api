import os
import logging
from dotenv import load_dotenv
import pandas as pd
import requests
import time


# Logging settings
LOG_FILE = 'app.log'
LOG_LEVEL = logging.INFO

# Make a directory for logs
log_dir = os.path.dirname(LOG_FILE)
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    filename=LOG_FILE,
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%y %H:%M:%S',
)

logger = logging.getLogger(__name__)

load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')
PATH_FOR_TEMP_FILE = os.getenv('PATH_FOR_TEMP_FILE')

if not API_TOKEN:
    logger.error('API_TOKEN is not found')
    exit()

if not PATH_FOR_TEMP_FILE:
    logger.warning('PATH_FOR_TEMP_FILE is not found. Temp files will be saved into current directory.')
    PATH_FOR_TEMP_FILE = ''

def get_debt_amount(number, api_token):
    # Makes an API request for a given number and extracts the total debt amount.
    api_url = f'https://api-cloud.ru/api/fssp.php?type=ip&number={number}&api_token={api_token}'
    start_time = time.time()
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        end_time = time.time()
        elapsed_time = end_time - start_time

        logger.info(f'API request for number {number} took {elapsed_time:.2f} seconds')

        if data['status'] == 200:
            if data['count'] > 0:
                debt = float(data['records'][0]['sum'])
                return debt  # Extract total debt amount
            else:
                logger.info(f'No debt found for number {number}. (Not in FSSP database)')
                return 0
        else:
            logger.warning(f'API returned a non-200 status for number {number}')
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f'API request failed for number {number}: {e}')
        return None
    except KeyError as e:
        logger.error(f'KeyError: {e}. API response structure might have changed for number {number}')
        return None
    except ValueError as e:
        logger.error(f'ValueError. Could not convert debt amount to float for number {number}: {e}')
        return None
    except Exception as e:
        logger.exception(f'An unexpected error occurred for number {number}: {e}')
        return None


#Load the Excel file
numbers_file = os.getenv('PATH_TO_NUMBERS_FILE')
try:
    df = pd.read_excel(numbers_file)
    logger.info(f'File loaded successfully. Found {len(df)} numbers')
except FileNotFoundError:
    logger.error('Error: Numbers file not found')
except Exception as e:
    logger.exception(f'Error reading file {numbers_file}: {e}')
    exit()

# Ensure the 'Debt Amount' column exists
if 'Debt Amount' not in df.columns:
    df['Debt Amount'] = None  # Initialize with None
    logger.info("Created new column 'Debt Amount'")

# Set the interval for saving DataFrame
SAVE_INTERVAL = 20 # Save every 20 requests
counter = 0

# Prepare a list to store the rows for temporary saving
temp_data = []

#Iterate through the Dataframe rows
for index, row in df.iterrows():
    num = str(row.iloc[0])  # Get the number from the first column as a string
    existing_debt = row.iloc[1] # Second column: Existing debt amount

    # Check if a debt amount already exists
    if pd.isna(existing_debt): # Check if the value is NaN (missing)
        debt_amount = get_debt_amount(num, API_TOKEN)

        if debt_amount is not None:
            df.loc[index, 'Debt Amount'] = debt_amount  # Update the DataFrame
            temp_data.append(df.loc[[index]]) # Add the row to the temp_data list
            logger.info(f'Found and updated debt amount for index {index}: {debt_amount}')
            counter += 1 # Increment a counter ONLY when a new API request is made
        else:
            df.loc[index, 'Debt Amount'] = None  # Keep it None id Error occurred
            temp_data.append(df.loc[[index]])
            logger.info(f'No debt found for index {index}, setting to None')

        time.sleep(3)  # Add a small delay to avoid overwhelming the API


    if counter % SAVE_INTERVAL == 0 and counter > 0:
        try:
            filename = f'numbers_with_debt_temp_{counter}.xlsx'
            # Correctly construct the full path
            if PATH_FOR_TEMP_FILE:
                os.makedirs(PATH_FOR_TEMP_FILE, exist_ok=True)
                full_path = os.path.join(PATH_FOR_TEMP_FILE, filename)
            else:
                full_path = filename # Save to current directory

            temp_df = pd.concat(temp_data) # Create a DataFrame from the temp_data list
            temp_df.to_excel(full_path, index=True)
            logger.info(f'Data saved to {full_path} after processing {counter} API calls.')
            temp_data = [] # Clear the temp_data for next save
        except Exception as e:
            logger.exception(f'Error saving to temporary Excel file: {e}')

# Save the updated DataFrame to a new Excel file
path_for_new_file = os.getenv('PATH_FOR_NEW_FILE')
try:
    df.to_excel(path_for_new_file, index=True)
    logger.info(f'Data saved to {path_for_new_file}')
except Exception as e:
    logger.exception(f'Error saving to Excel file: {e}')

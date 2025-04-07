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

if not API_TOKEN:
    logger.error('API_TOKEN is not found')
    exit()


def get_debt_amount(number, api_token):
    # Makes an API request for a given number and extracts the total debt amount.
    api_url = f'https://api-cloud.ru/api/fssp.php?type=ip&number={number}&api_token={api_token}'
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()

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

#Create a new column to store the debt amounts
df['Debt_Amount'] = None  # Initialize with None

#Iterate through the numbers in the first column and make API requests
for index, row in df.iterrows():
    num = str(row.iloc[0])  # Get the number from the first column as a string
    debt_amount = get_debt_amount(num, API_TOKEN)

    if debt_amount is not None:
        df.loc[index, 'Debt_Amount'] = debt_amount  # Store the debt amount in the new column
    else:
        df.loc[index, 'Debt_Amount'] = None  # Keep it None id Error occurred

    time.sleep(0.5)  # Add a small delay to avoid overwhelming the API

# Save the updated DataFrame to a new Excel file
path_for_new_file = os.getenv('PATH_FOR_NEW_FILE')
try:
    df.to_excel(path_for_new_file, index=False)
    logger.info(f'Data saved to {path_for_new_file}')
except Exception as e:
    logger.exception(f'Error saving to Excel file: {e}')

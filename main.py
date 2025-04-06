import os
from dotenv import load_dotenv
import pandas as pd
import requests
import time

load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')

if not API_TOKEN:
    print('API_TOKEN is not found')
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
                return float(data['records'][0]['sum'])  # Extract total debt amount
            else:
                print(f'No debt found for number {number}')
                return 0
        else:
            print(f'API returned a non-200 status for number {number}')
            return None
    except requests.exceptions.RequestException as e:
        print(f'API request failed for number {number}: {e}')
        return None
    except KeyError as e:
        print(f'KeyError: {e}. API response structure might have changed for number {number}')
        return None
    except ValueError as e:
        print(f'ValueError. Could not convert debt amount to float for number {number}: {e}')
        return None
    except Exception as e:
        print(f'An unexpected error occurred for number {number}: {e}')
        return None


#Load the Excel file
numbers_file = os.getenv('PATH_TO_NUMBERS_FILE')
try:
    df = pd.read_excel(numbers_file)
except FileNotFoundError:
    print('Error: Numbers file not found')
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
    print('Data saved to excel file')
except Exception as e:
    print(f'Error saving to Excel file: {e}')

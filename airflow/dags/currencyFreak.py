import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
import s3fs
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


DOMAIN = "https://api.currencyfreaks.com/v2.0/rates/historical?apikey"
API_KEY = os.getenv("CURRENCY_FREAKS_API_KEY")
BASE_CURRENCY = "NGN"
TARGET_CURRENCIES = [
    'USD', 'EUR', 'GBP', 'CNY', 'JPY',
    'ZAR', 'CAD', 'AUD', 'GHS', 'XOF',
    'XAF', 'INR', 'CHF', 'RUB', 'AED'
]

# Create a dataframe to store the exchange rates
df = pd.DataFrame(columns=['date', 'base', 'target', 'rate'])

# Create an S3 filesystem object
s3 = s3fs.S3FileSystem()

# Constants for date range and wait time
START_DATE = datetime(2011, 1, 1)
END_DATE = datetime(2011, 1, 5)
WAIT_TIME = 2  # seconds between API calls


# default arguments for the DAG
default_args = {
    'owner': 'bideen',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1)
}


# Loop through dates
def generate_date_list(start, end):

    # Calculates days btw start and end (inclusive)
    days = (end - start).days + 1
    return [
        (start + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days)
    ]


# Fetch historical exchange rate with 1 Naira as base currency
def get_rates_for_date(date):
    """Fetches exchange rates for a specific date."""
    url = f'{DOMAIN}={API_KEY}&date={date}&base={BASE_CURRENCY}'

    r = requests.get(url)
    # Raise error if request fails
    r.raise_for_status()
    return r.json()


def get_data(**kwargs):
    date_list = generate_date_list(START_DATE, END_DATE)

    dates, bases, targets, brates = [], [], [], []

    # Loops through each date
    for date in date_list:
        print(f"Processing date: {date}")

        try:
            # Fetches all exchange rates for that day
            data = get_rates_for_date(date)
            rates = data.get('rates', {})

            # Fetches exchange rates for specific currencies
            for target in TARGET_CURRENCIES:
                rate = rates.get(target)

                # If rate is not available in endpoint, DO NOT include in CSV
                if rate is not None:
                    dates.append(data['date'])
                    bases.append(data['base'])
                    targets.append(target)
                    brates.append(rate)
                else:
                    print(f"Rate for {target} not found on {date}.")
                    continue

        # Error handling and rate limit
        except Exception as e:
            print(f"Error for {date}: {e}")
            continue
        time.sleep(WAIT_TIME)

    # Push lists to XCom
    ti = kwargs.get('ti')
    ti.xcom_push(key='dates', value=dates)
    ti.xcom_push(key='bases', value=bases)
    ti.xcom_push(key='targets', value=targets)
    ti.xcom_push(key='brates', value=brates)


def save_data_to_parquet(**kwargs):
    """Saves the collected data to a Parquet file."""
    ti = kwargs.get('ti')
    dates = ti.xcom_pull(task_ids='get_data', key='dates')
    bases = ti.xcom_pull(task_ids='get_data', key='bases')
    targets = ti.xcom_pull(task_ids='get_data', key='targets')
    brates = ti.xcom_pull(task_ids='get_data', key='brates')
    df = pd.DataFrame(
        {
            'date': dates, 'base': bases, 'target': targets, 'rate': brates
        })

    df['date'] = pd.to_datetime(df['date'])

    # Set date as index
    df.set_index('date', inplace=True)

    # Filename with date range and base currency
    filename = f"exchange_rates_{
        BASE_CURRENCY
        }_{
            START_DATE.strftime('%Y%m%d')
            }_to_{
                END_DATE.strftime('%Y%m%d')}.parquet"

    # Save DataFrame to Parquet file
    df.to_parquet(f's3://biko-bucket-567/{
        filename
        }', engine='fastparquet', compression='snappy')

    return "Sent to S3 bucket successfully"


with DAG('ingest_data', default_args=default_args, schedule='@daily') as dag:

    get_data_task = PythonOperator(
        task_id='get_data',
        python_callable=get_data,
        dag=dag
    )

    save_and_convert_to_parquet_task_to_s3 = PythonOperator(
        task_id='convert_to_parquet_and_save_to_s3',
        python_callable=save_data_to_parquet,
        dag=dag
    )

    # The task dependencies in the DAG
    get_data_task >> save_and_convert_to_parquet_task_to_s3

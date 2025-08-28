from datetime import datetime, timedelta

import requests
from airflow.sdk import Variable

# Constants for date range and wait time
START_DATE = datetime(2011, 1, 1)
END_DATE = datetime(2011, 1, 5)
WAIT_TIME = 2  # seconds between API calls

# default arguments for the DAG
default_args = {
    'owner': 'bideen',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'catchup': True,
    'backfill': True
}


DOMAIN = Variable.get("DOMAIN")
API_KEY = Variable.get("CURRENCY_FREAKS_API_KEY")
BASE_CURRENCY = "NGN"
TARGET_CURRENCIES = [
    'USD', 'EUR', 'GBP', 'CNY', 'JPY',
    'ZAR', 'CAD', 'AUD', 'GHS', 'XOF',
    'XAF', 'INR', 'CHF', 'RUB', 'AED'
]


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

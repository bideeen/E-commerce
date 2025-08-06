import requests
import time
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import s3fs


# Load environment variables
load_dotenv()

API_KEY = os.getenv("CURRENCY_FREAKS_API_KEY")
BASE_CURRENCY="NGN"
TARGET_CURRENCIES= ['USD', 'EUR', 'GBP', 'CNY', 'JPY','ZAR', 'CAD', 'AUD', 'GHS', 'XOF','XAF', 'INR', 'CHF', 'RUB', 'AED']

# Create a dataframe to store the exchange rates
df = pd.DataFrame(columns=['date','base', 'target', 'rate'])

# Create an S3 filesystem object
s3 = s3fs.S3FileSystem()

# Constants for date range and wait time
START_DATE = datetime(2011, 1, 1)
END_DATE = datetime(2011, 1, 5)
WAIT_TIME = 2  # seconds between API calls


# Loop through dates
def generate_date_list(start, end):

    days = (end - start).days + 1 # Calculates days btw start and end (inclusive)
    return [(start + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days)]



# Fetch historical exchange rate with 1 Naira as base currency
def get_rates_for_date(date):

    url = f'https://api.currencyfreaks.com/v2.0/rates/historical?apikey={API_KEY}&date={date}&base={BASE_CURRENCY}'
    
    r = requests.get(url)
    r.raise_for_status() # Raise error if request fails
    return r.json()

# Save historical exchange rate in CSV
def main():

    date_list = generate_date_list(START_DATE, END_DATE)

    filename = f"exchange_rates_{BASE_CURRENCY}_{START_DATE.strftime('%Y%m%d')}_to_{END_DATE.strftime('%Y%m%d')}.parquet"
    
    dates, bases, targets, brates = [], [], [], [] 

    # Loops through each date
    for date in date_list:
        print(f"Processing date: {date}") # Optional statement to track progress

        try:
            # Fetches all exchange rates for that day
            data = get_rates_for_date(date)
            # print(f"Data for {date}: {data}")  # Optional statement to track data fetched
            rates = data.get('rates', {})
            base = data.get('base', BASE_CURRENCY)

            # Fetches exchange rates for specific currencies
            for target in TARGET_CURRENCIES:
                rate = rates.get(target)

                # If rate is not available in endpint, DO NOT include in CSV
                if rate is not None:
                    dates.append(data['date'])
                    bases.append(data['base'])
                    targets.append(target)
                    brates.append(rate)
                else:
                    print(f"Rate for {target} not found on {date}.")
                    break


        # Error handling and rate limit
        except Exception as e:
            print(f"Error for {date}: {e}")
            break
        time.sleep(WAIT_TIME)
    
    df['date'] = dates
    df['base'] = bases
    df['target'] = targets
    df['rate'] = brates

    df['date'] = pd.to_datetime(df['date'])  # Convert date column to datetime
    
    df.set_index('date', inplace=True)  # Set date as index
    # df.to_parquet(filename, engine='fastparquet', compression='snappy')  # Save DataFrame to Parquet file
    # write to s3 bucket
    df.to_parquet(f's3://biko-bucket-567/{filename}', engine='fastparquet', compression='snappy')


if __name__ == '__main__':
    main()
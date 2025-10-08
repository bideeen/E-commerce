import time
from datetime import datetime, timedelta
import requests
import pandas as pd
import awswrangler as wr


# Constants
BASE_CURRENCY = "NGN"
TARGET_CURRENCIES = [
    'USD', 'EUR', 'GBP', 'CNY', 'JPY',
    'ZAR', 'CAD', 'AUD', 'GHS', 'XOF',
    'XAF', 'INR', 'CHF', 'RUB', 'AED'
]
WAIT_TIME = 2  # seconds between API calls
S3_BUCKET = "biko-bucket-567"

# generate list of dates
def generate_date_list(start_date, end_date):
    """
    Creates a list of all dates between start and end
    
    Example:
        start_date = datetime(2011, 1, 1)
        end_date = datetime(2011, 1, 3)
        Result: ['2011-01-01', '2011-01-02', '2011-01-03']
    """
    # Calculate how many days between start and end
    total_days = (end_date - start_date).days + 1
    
    # Create a list of all dates
    date_list = []
    for day_number in range(total_days):
        current_date = start_date + timedelta(days=day_number)
        date_string = current_date.strftime('%Y-%m-%d')
        date_list.append(date_string)
    
    print(f"Generated {len(date_list)} dates from {start_date.date()} to {end_date.date()}")
    return date_list

    

# fetch rates for a specific date
def get_rates_for_date(date, domain, api_key):
    """
    Fetch exchange rates for a specific date
    
    Args:
        date (str): Date in 'YYYY-MM-DD' format
        domain (str): API domain URL
        api_key (str): API key for authentication
        
    Returns:
        dict: JSON response with exchange rates
    """
    url = f'{domain}={api_key}&date={date}&base={BASE_CURRENCY}'
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


# Fetch data from multiple dates
def fetch_rates_for_date_range(start_date, end_date, domain, api_key):
    """
    Fetches exchange rates for ALL dates between start and end
    Returns a pandas DataFrame with columns: date, target, rate
    
    Example output:
        date        target  rate
        2011-01-01  USD     0.0065
        2011-01-01  EUR     0.0049
        2011-01-02  USD     0.0066
        ...
    """
    print(f"\n=== Starting to fetch rates ===")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    
    # Get list of all dates
    date_list = generate_date_list(start_date, end_date)
    
    # These lists will store all our data
    all_dates = []
    all_targets = []
    all_rates = []
    
    # Loop through each date
    for date in date_list:
        try:
            # Fetch data for this date
            data = get_rates_for_date(date, domain, api_key)
            rates = data.get('rates', {})
            
            # Extract rates for each target currency
            for target_currency in TARGET_CURRENCIES:
                rate = rates.get(target_currency)
                
                if rate is not None:
                    # Add to our lists
                    all_dates.append(data['date'])
                    all_targets.append(target_currency)
                    all_rates.append(rate)
                else:
                    print(f"    ⚠️  No rate found for {target_currency} on {date}")
            
        except Exception as e:
            print(f"    ❌ Error fetching data for {date}: {e}")
            continue
        
        # Wait before next API call (to avoid hitting rate limits)
        time.sleep(WAIT_TIME)
    
    # Create DataFrame
    df = pd.DataFrame({
        'date': all_dates,
        'target': all_targets,
        'rate': all_rates
    })
    
    # Convert date column to datetime type
    df['date'] = pd.to_datetime(df['date'])
    
    print(f"\n✅ Fetched {len(df)} total records")
    return df

# Upload df to s3
def upload_to_s3(df, start_date, end_date):
    """
    Uploads the DataFrame to S3 as a parquet file
    
    File will be named: exchange_rates_NGN_20110101_to_20110131.parquet
    """
    print(f"\n=== Uploading to S3 ===")
    
    # Filename with date range and base currency
    filename = f"exchange_rates_{
        BASE_CURRENCY
        }_{
            start_date.strftime('%Y%m%d')
            }_to_{
                end_date.strftime('%Y%m%d')}.parquet"

    # Storing data on Data Lake
    wr.s3.to_parquet(
        df=df,
        path=f's3://biko-bucket-567/{filename}'
    )
    
    print(f"✅ Successfully uploaded {len(df)} records to S3")
    
# main function to run the logic
def process_currency_data(start_date, end_date, domain, api_key):
    """
    MAIN FUNCTION: Does everything from start to finish
    1. Fetches data from API
    2. Creates DataFrame
    3. Uploads to S3
    
    This is the function your DAG will call!
    """
    print("\n" + "="*50)
    print("STARTING CURRENCY DATA PROCESSING")
    print("="*50)
    
    # Step 1: Fetch all the data
    df = fetch_rates_for_date_range(start_date, end_date, domain, api_key)
    
    # Step 2: Check if we got any data
    if df.empty:
        print("❌ No data was fetched. Stopping.")
        return {
            'status': 'failed',
            'records': 0,
            'message': 'No data fetched from API'
        }
    
    # Step 3: Upload to S3
    s3_path = upload_to_s3(df, start_date, end_date)
    
    # Step 4: Return summary
    print("\n" + "="*50)
    print("PROCESS COMPLETE!")
    print("="*50 + "\n")
    
    return {
        'status': 'success',
        'records': len(df),
        's3_path': s3_path,
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d')
    }
    


import time

import awswrangler as wr
import pandas as pd
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from Currency_freaks import main as cf


# Configuration
def currencyFreak_to_s3(**kwargs):
    date_list = cf.generate_date_list(cf.START_DATE, cf.END_DATE)

    dates, targets, brates = [], [], []

    # Loops through each date
    for date in date_list:
        print(f"Processing date: {date}")

        try:
            # Fetches all exchange rates for that day
            data = cf.get_rates_for_date(date)
            rates = data.get('rates', {})

            # Fetches exchange rates for specific currencies
            for target in cf.TARGET_CURRENCIES:
                rate = rates.get(target)

                # If rate is not available in endpoint, DO NOT include in CSV
                if rate is not None:
                    dates.append(data['date'])
                    targets.append(target)
                    brates.append(rate)
                else:
                    print(f"Rate for {target} not found on {date}.")
                    continue

        # Error handling and rate limit
        except Exception as e:
            print(f"Error for {date}: {e}")
            continue
        time.sleep(cf.WAIT_TIME)

    # Push lists to XCom
    df = pd.DataFrame(
        {
            'date': dates, 'target': targets, 'rate': brates
        })

    df['date'] = pd.to_datetime(df['date'])

    # Filename with date range and base currency
    filename = f"exchange_rates_{
        cf.BASE_CURRENCY
        }_{
            cf.START_DATE.strftime('%Y%m%d')
            }_to_{
                cf.END_DATE.strftime('%Y%m%d')}.parquet"

    # Storing data on Data Lake
    wr.s3.to_parquet(
        df=df,
        path=f's3://biko-bucket-567/{filename}'
    )

    return "Sent to S3 bucket successfully"


with DAG('ingest_data', default_args=cf.default_args, schedule='@daily') as dag:

    fecth_currencyFreak_to_s3 = PythonOperator(
        task_id='fecth_currencyFreak_to_s3',
        python_callable=currencyFreak_to_s3,
        dag=dag
    )

    # The task dependencies in the DAG
    fecth_currencyFreak_to_s3

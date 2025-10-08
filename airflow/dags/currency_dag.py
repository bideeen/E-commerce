import time
import datetime
import awswrangler as wr
import pandas as pd
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

# Logic moved to plugins for better modularity
import currency_logic 


# get api credentials from airflow variables
api_key = Variable.get("CURRENCY_FREAKS_API_KEY")
DOMAIN = Variable.get("DOMAIN")


# Default arguments for the DAG
default_args = {
    'owner': 'bideen',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': pd.Timedelta(minutes=5),
}


# Daily dag to fetch currency data and store in S3
def run_daily_load(**kwargs):
    """
    This function orchestrates the daily load of currency data.
    It fetches currency data, processes it, and stores it in S3.
    
    """
    # get execution date
    execution_date = kwargs.get('execution_date')
    
    print(f"\n📅 Processing data for: {execution_date.date()}")
    
    # Call the logic function to fetch and process data
    result = currency_logic.fetch_and_store_currency_data(api_key, DOMAIN, execution_date)

    if result:
        return f"✅ Successfully processed data for: {execution_date.date()}"
    else:
        return f"❌ Failed to process data for: {execution_date.date()}"
        

# Manual backfill function for historical data
def run_manual_backfill(**kwargs):
    """
    This function handles manual backfill of currency data for a specified date range.
    
    
    Change the dates below to backfill data for different periods.
    """
    
    # Change these dates to backfill different periods
    start_date = datetime.datetime(2023, 1, 1)
    end_date = datetime.datetime(2023, 1, 10)
    
    print(f"\n🔄 Starting backfill from {start_date.date()} to {end_date.date()}")
    
    result = currency_logic.backfill_currency_data(api_key, DOMAIN, start_date, end_date)
    
    if result:
        print(f"✅ Successfully backfilled data from {start_date.date()} to {end_date.date()}")
    else:
        print(f"❌ Failed to backfill data from {start_date.date()} to {end_date.date()}")
        
def run_flexible_backfill(**context):
    """
    This function lets you pass dates when triggering the DAG
    
    When you trigger this DAG in Airflow UI, add this JSON config:
    {
        "start_date": "2011-01-01",
        "end_date": "2011-12-31"
    }
    """
    # Get the configuration passed when DAG was triggered
    dag_run = context.get('dag_run')
    config = dag_run.conf if dag_run else {}
    
    # Check if user provided custom dates
    if config.get('start_date') and config.get('end_date'):
        # Use dates from config
        start_date = datetime.strptime(config['start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(config['end_date'], '%Y-%m-%d')
        print(f"\n📅 Using custom dates from config")
    else:
        # Default to a small range if no config provided
        start_date = datetime(2011, 1, 1)
        end_date = datetime(2011, 1, 7)
        print(f"\n📅 No config provided, using default dates")
    
    print(f"Processing: {start_date.date()} to {end_date.date()}")
    
    # Call our logic function
    result = currency_logic.process_currency_data(
        start_date=start_date,
        end_date=end_date,
        domain=DOMAIN,
        api_key=api_key
    )
    
    print(f"\n✅ Result: {result}")
    return result



# Define the DAG
# Daily at 6 AM
with DAG(
    dag_id='currency_daily_load',
    default_args=default_args,
    description='load of currency data daily',
    schedule='@daily',  # Daily at 6 AM
    start_date=datetime.datetime(2023, 1, 1),
    tags=['currency', 'daily_load']
) as daily_dag:
    daily_task = PythonOperator(
        task_id='fetch_daily_rates',
        python_callable=run_daily_load,
        dag=daily_dag
    )
    
    daily_task

# Manual backfill DAG
with DAG(
    dag_id='currency_manual_backfill',
    default_args=default_args,
    description='manual backfill of currency data',
    schedule=None,
    start_date=datetime.datetime(2023, 1, 1),
    tags=['currency', 'manual_backfill']
) as backfill_dag:
    backfill_task = PythonOperator(
        task_id='manual_backfill',
        python_callable=run_manual_backfill,
        dag=backfill_dag
    )
    
    backfill_task

# flexible backfill DAG
with DAG(
    dag_id='currency_flexible_backfill',
    default_args=default_args,
    description='Backfill with custom date range via config',
    start_date=datetime.datetime(2023, 1, 1),
    schedule=None,  # Manual trigger only
    tags=['currency', 'backfill', 'flexible']
) as flexible_dag:    
    flex_task = PythonOperator(
        task_id='run_flexible_backfill',
        python_callable=run_flexible_backfill,
        dag=flexible_dag
    )    
    
    flex_task

    
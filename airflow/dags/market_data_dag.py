import sys
import os

# Ensure the 'scripts' directory is in the path so Airflow can import local modules
sys.path.append('/opt/airflow')

from scripts.ingest_sentiment import fetch_fear_greed_csv
from scripts.ingest_coingecko import fetch_coingecko_prices_csv

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState, State
from airflow.utils import timezone
from airflow.models import DagRun
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

# --- 1. Global Variables & Configuration ---
BUCKET_NAME = 'crypto-raw-data-lake'
DBT_PROJECT_DIR = '/opt/airflow/dbt_crypto'

def task_fail_slack_alert(context):
    """
    Callback function to handle task failures (Placeholder for Slack/Email alerts).
    """
    print(f"Task {context.get('task_instance').task_id} failed. Sending alert...")

# --- 2. Custom Python Callables ---
def upload_sentiment_to_gcs_func():
    """
    Fetches Fear & Greed index via API and uploads the CSV string directly to GCS.
    Uses GCSHook for a more flexible, programmatic upload compared to LocalFilesystemToGCS.
    """
    csv_string = fetch_fear_greed_csv()
    hook = GCSHook(gcp_conn_id='google_cloud_default')
    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name='raw/sentiment.csv',
        data=csv_string,
        mime_type='text/csv'
    )

def upload_coingecko_to_gcs_func():
    """
    Fetches CoinGecko price data via API and uploads the CSV string to GCS.
    """
    csv_string = fetch_coingecko_prices_csv()
    hook = GCSHook(gcp_conn_id='google_cloud_default')
    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name='raw/coingecko.csv',
        data=csv_string,
        mime_type='text/csv'
    )

def get_most_recent_dag_run(logical_date, **kwargs):
    success_runs = DagRun.find(dag_id='binance_spark_ingestion_v1', state=State.SUCCESS)
    
    if success_runs:
        success_runs.sort(key=lambda x: x.execution_date, reverse=True)
        latest_success = success_runs[0].execution_date
        
        now = timezone.utcnow()
        if latest_success >= (now - timedelta(hours=1)):
            print(f"Found a successful DAG run within the last hour: {latest_success}")
            return latest_success
            
    print("No successful DAG run found within the last hour. Waiting for the next run...")
    return logical_date + timedelta(days=365)

default_args = {
    'owner': 'sbq',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert,
}

# --- 3. DAG Definition ---
with DAG(
    'market_metadata_ingestion_v1',
    default_args=default_args,
    start_date=datetime(2026, 2, 5),
    schedule_interval='@daily',
    catchup=False, 
    max_active_runs=1
) as dag:
    # Task 1. Wait for upstream
    wait_for_binance_data = ExternalTaskSensor(
        task_id='wait_for_binance_ingestion',
        external_dag_id='binance_spark_ingestion_v1',
        external_task_id=None,
        allowed_states=['success'],
        poke_interval=30,
        timeout=600,
        mode='reschedule',
        execution_date_fn=get_most_recent_dag_run,
        check_existence=True,
    )

    # Task 2 & 3: Ingest Metadata via Python Callables
    task_sentiment = PythonOperator(
        task_id='upload_sentiment_to_gcs',
        python_callable=upload_sentiment_to_gcs_func
    )

    task_coingecko = PythonOperator(
        task_id='upload_coingecko_to_gcs',
        python_callable=upload_coingecko_to_gcs_func
    )

    # Task 4 & 5: Transfer Landing Zone data to BigQuery Staging
    # Using 'WRITE_TRUNCATE' for metadata to refresh the latest 30-day window.
    gcs_to_bq_sentiment = GCSToBigQueryOperator(
        task_id='gcs_to_bq_sentiment',
        bucket=BUCKET_NAME,
        source_objects=['raw/sentiment.csv'],
        destination_project_dataset_table='project-64505.binance_data.market_sentiment',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        skip_leading_rows=1
    )

    gcs_to_bq_coingecko = GCSToBigQueryOperator(
        task_id='gcs_to_bq_coingecko',
        bucket=BUCKET_NAME,
        source_objects=['raw/coingecko.csv'],
        destination_project_dataset_table='project-64505.binance_data.market_prices',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        skip_leading_rows=1
    )

    # Task 6: Final dbt Transformation
    # Triggers the dbt model that joins Binance trades, Sentiment, and Prices.
    run_dbt_marts = BashOperator(
        task_id='run_dbt_crypto_marts',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select +fct_crypto_daily_metrics --profiles-dir .'
    )

    # --- 4. Pipeline Dependency Graph ---
    # 1. Wait for upstream -> 2. Parallel API Ingestion -> 3. Load to BQ -> 4. Run dbt Marts
    wait_for_binance_data >> [task_sentiment, task_coingecko]
    
    task_sentiment >> gcs_to_bq_sentiment
    task_coingecko >> gcs_to_bq_coingecko

    [gcs_to_bq_sentiment, gcs_to_bq_coingecko] >> run_dbt_marts
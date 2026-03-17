from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

# --- 1. Alerting & Callback Functions ---
def task_fail_slack_alert(context):
    """
    Placeholder for Slack/Email notifications on task failure.
    Uses the Airflow context to identify which task failed.
    """
    print(f"Task {context.get('task_instance').task_id} failed. Sending alert...")

# --- 2. Default Configurations ---
default_args = {
    'owner': 'sbq',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 5),
    'email_on_failure': False,
    # Retry logic: Retries 2 times if a task fails due to transient issues
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert,
}

# --- 3. DAG Definition ---
with DAG(
    'binance_spark_ingestion_v1',
    default_args=default_args,
    description='Download and process Binance data using Spark',
    schedule_interval='@daily',
    # catchup=True allows re-running historical data from the start_date
    catchup=True,
    max_active_runs=1,
    params={
        "symbol": "BTCUSDT",
        "data_dir": "/opt/airflow/scripts/data"
    }
) as dag:

    # Task 1: Execute the Spark Python script via Bash
    # Note: Using {{ macros.ds_add(ds, -1) }} to fetch the previous day's data,
    # as Binance typically releases daily aggregated trades for the completed prior day.
    run_spark_script = BashOperator(
        task_id='run_ingest_binance',
        bash_command="""
            python /opt/airflow/scripts/ingest_binance.py \
            --date {{ macros.ds_add(ds, -1) }} \
            --symbol {{ params.symbol }} \
            --output {{ params.data_dir }}
        """
    )

    # Task 2: Upload the processed Parquet files from local storage to GCS
    # This acts as the "Landing/Raw" zone in the Medallion Architecture.
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src='/opt/airflow/scripts/data/{{ params.symbol }}_{{ macros.ds_add(ds, -1) }}.parquet/*',
        dst='binance_raw/{{ params.symbol }}/{{ macros.ds_add(ds, -1) }}/',
        bucket='crypto-raw-data-lake',
        gcp_conn_id='google_cloud_default',
    )

    # Task 3: Load the data from GCS into a BigQuery table
    # Set to 'WRITE_APPEND' to incrementally build the history of trade data.
    # 'autodetect=True' allows BQ to infer the schema directly from the Parquet files.
    load_to_bq = GCSToBigQueryOperator(
        task_id='load_to_bq',
        bucket='crypto-raw-data-lake',
        source_objects=['binance_raw/{{ params.symbol }}/{{ macros.ds_add(ds, -1) }}/*.parquet'],
        destination_project_dataset_table='project-64505.binance_data.{{ params.symbol }}',
        source_format='PARQUET',
        write_disposition='WRITE_APPEND',
        autodetect=True,
        gcp_conn_id='google_cloud_default',
    )

    # --- 4. Pipeline Dependency Graph ---
    # Ingest -> Upload -> Load
    run_spark_script >> upload_to_gcs >> load_to_bq
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
import os
import requests
import logging
import json
from google.cloud import pubsub_v1
from google.cloud import storage

# Configuration
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
GCP_PROJECT = os.getenv("GCP_PROJECT")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC", "stock-data-topic")
BUCKET_NAME = os.getenv("BUCKET_NAME", "stock-data-backup")
STOCK_SYMBOLS = os.getenv("STOCK_SYMBOLS", "AAPL,GOOG,MSFT").split(",")
PYSPARK_SCRIPT_PATH = "/path/to/stream_pubsub_to_bigquery.py"  # Update with the local path to your PySpark script
BIGQUERY_TABLE = "gcp_project.stock.stock_data"

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# DAG configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["sridhaprakash@gmail.com"],  # Replace with your email address
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="stock_data_pipeline_local",
    default_args=default_args,
    description="Fetch stock data, publish to Pub/Sub, backup to GCS, and process via local PySpark",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def fetch_stock_data(stock_symbol):
        """Fetch stock data from Alpha Vantage."""
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": stock_symbol,
            "interval": "1min",
            "apikey": ALPHA_VANTAGE_API_KEY,
        }
        try:
            response = requests.get("https://www.alphavantage.co", params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            if "Time Series" in data:
                return data
            else:
                raise ValueError(f"Unexpected API response: {data}")
        except Exception as e:
            logging.error(f"Failed to fetch data for {stock_symbol}: {e}")
            raise

    def publish_to_pubsub(stock_symbol, data):
        """Publish stock data to Pub/Sub."""
        try:
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(GCP_PROJECT, PUBSUB_TOPIC)
            message = {"symbol": stock_symbol, "data": data}
            publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
            logging.info(f"Published data for {stock_symbol} to Pub/Sub topic {PUBSUB_TOPIC}")
        except Exception as e:
            logging.error(f"Failed to publish data for {stock_symbol}: {e}")
            raise

    def backup_to_gcs(stock_symbol, data):
        """Backup stock data to GCS."""
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"raw_data/{stock_symbol}.json")
            blob.upload_from_string(json.dumps(data), content_type="application/json")
            logging.info(f"Backed up data for {stock_symbol} to GCS bucket {BUCKET_NAME}")
        except Exception as e:
            logging.error(f"Failed to back up data for {stock_symbol}: {e}")
            raise

    def process_stock_data(stock_symbol):
        """Fetch, publish, and backup stock data."""
        data = fetch_stock_data(stock_symbol)
        publish_to_pubsub(stock_symbol, data)
        backup_to_gcs(stock_symbol, data)

    # Task: Fetch and process stock data
    fetch_and_process_tasks = []
    for stock_symbol in STOCK_SYMBOLS:
        fetch_and_process_tasks.append(
            PythonOperator(
                task_id=f"process_stock_data_{stock_symbol}",
                python_callable=process_stock_data,
                op_kwargs={"stock_symbol": stock_symbol},
            )
        )

    # Task: Run PySpark streaming job locally
    run_pyspark_streaming = BashOperator(
        task_id="run_pyspark_streaming",
        bash_command=f"""
        spark-submit \
        --master local[*] \
        --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.27.1 \
        {PYSPARK_SCRIPT_PATH} \
        --project_id {GCP_PROJECT} \
        --bigquery_table {BIGQUERY_TABLE}
        """,
    )

    # Task: Email notification for failure (optional)
    email_on_failure = EmailOperator(
        task_id="email_on_failure",
        to="sridhaprakash@gmail.com",
        subject="Airflow Task Failed",
        html_content="<p>A task in the DAG {{ dag.dag_id }} has failed. Please check the logs for details.</p>",
    )

    # Orchestrate tasks
    for task in fetch_and_process_tasks:
        task >> email_on_failure
    fetch_and_process_tasks >> run_pyspark_streaming >> email_on_failure

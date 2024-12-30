import os
import requests
import json
import logging
from google.cloud import pubsub_v1
from google.cloud import storage
from time import sleep
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Alpha Vantage API details
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
STOCK_SYMBOLS = os.getenv("STOCK_SYMBOLS", "AAPL,GOOG,MSFT").split(",")  # Default stocks

# Google Cloud details
GCP_PROJECT = os.getenv("GCP_PROJECT")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC", "stock-data-topic")
BUCKET_NAME = os.getenv("BUCKET_NAME", "stock-data-backup")

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Alpha Vantage API base URL
BASE_URL = "https://www.alphavantage.co"

def fetch_stock_data(stock_symbol):
    """Fetch stock data for a given stock symbol from Alpha Vantage."""
    #Intraday data refers to market data (such as price and volume) for very short time intervals, typically within the same trading day (minutes or hours).
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": stock_symbol,
        "interval": "1min",
        "apikey": ALPHA_VANTAGE_API_KEY,
    }
    retries = 3
    for attempt in range(retries):
        try:
            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()  # Raise an exception for HTTP errors
            data = response.json()
            if "Time Series" in data:
                return data
            else:
                raise ValueError(f"Unexpected API response: {data}")
        except Exception as e:
            logging.error(f"Attempt {attempt + 1}: Failed to fetch data for {stock_symbol}. Error: {e}")
            if attempt < retries - 1:
                sleep(2 ** attempt)  # Exponential backoff
            else:
                raise

def publish_to_pubsub(stock_symbol, data):
    """Publish stock data to Google Cloud Pub/Sub."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(GCP_PROJECT, PUBSUB_TOPIC)
    message = {"symbol": stock_symbol, "data": data}
    try:
    publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
    logging.info(f"Published data for {stock_symbol} to Pub/Sub topic {PUBSUB_TOPIC}")
    except Exception as e:
    logging.error(f"Failed to publish data for {stock_symbol}: {e}")


def backup_to_gcs(stock_symbol, data):
    """Backup stock data to Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"raw_data/{stock_symbol}.json")
    try:
    blob.upload_from_string(json.dumps(data), content_type="application/json")
    logging.info(f"Backed up data for {stock_symbol} to Cloud Storage bucket {BUCKET_NAME}")
    except Exception as e:
    logging.error(f"Failed to back up data for {stock_symbol}: {e}")

def process_stock_data(stock_symbol):
    """Fetch, publish, and backup stock data for a given stock symbol."""
    logging.info(f"Processing data for stock: {stock_symbol}")
    stock_data = fetch_stock_data(stock_symbol)
    publish_to_pubsub(stock_symbol, stock_data)
    backup_to_gcs(stock_symbol, stock_data)

if __name__ == "__main__":
    if not ALPHA_VANTAGE_API_KEY or not GCP_PROJECT:
        logging.error("Environment variables ALPHA_VANTAGE_API_KEY and GCP_PROJECT are required.")
        exit(1)

    for stock_symbol in STOCK_SYMBOLS:
        try:
            process_stock_data(stock_symbol)
        except Exception as e:
            logging.error(f"Failed to process data for {stock_symbol}. Error: {e}")

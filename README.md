Install Dependencies:
pip install requests google-cloud-pubsub google-cloud-storage python-dotenv

Set up GCP:
Enable Pub/Sub and Cloud Storage APIs.
Create the Pub/Sub topic (stock-data-topic) and Storage bucket (stock-data-backup).

Run the Script:
python stock_data_pipeline.py

**stock_data_pipeline.py**
For uploading every large data in gcp-
1.Using Streams or Chunks with GCS
from google.cloud import storage
import logging
import json
import io

def backup_large_data_to_gcs(stock_symbol, large_data):
    """
    Backup large stock data to Google Cloud Storage using a streaming approach.
    """
    try:
        # Initialize GCS client and bucket
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"raw_data/{stock_symbol}.json")
        
        # Convert large data to a JSON stream
        data_stream = io.StringIO()
        for chunk in json.JSONEncoder().iterencode(large_data):
            data_stream.write(chunk)
        
        # Reset the stream position to the beginning
        data_stream.seek(0)
        
        # Upload the JSON stream to GCS
        blob.upload_from_file(data_stream, content_type="application/json")
        logging.info(f"Backed up large data for {stock_symbol} to Cloud Storage bucket {BUCKET_NAME}")
    
    except Exception as e:
        logging.error(f"Failed to back up large data for {stock_symbol}: {e}")

Explanation
io.StringIO:

This creates an in-memory stream for handling JSON data.
It allows you to write data in chunks and treat it like a file object, which can then be streamed to GCS.
json.JSONEncoder().iterencode():

This method encodes the JSON data incrementally, generating smaller chunks of the JSON string.
It avoids converting the entire dataset into a single JSON string in memory, which is beneficial for large datasets.
seek(0):

Resets the stream pointer to the start of the stream so that the file can be read from the beginning during the upload process.
upload_from_file():

This method uploads the JSON stream directly to GCS.
It avoids loading the entire data into memory, making it suitable for large files.


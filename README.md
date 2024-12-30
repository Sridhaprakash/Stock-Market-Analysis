Install Dependencies:
pip install requests google-cloud-pubsub google-cloud-storage python-dotenv

Set up GCP:
Enable Pub/Sub and Cloud Storage APIs.
Create the Pub/Sub topic (stock-data-topic) and Storage bucket (stock-data-backup).

Run the Script:
python stock_data_pipeline.py

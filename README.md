****Workflow Diagram****

**Fetch Stock Data:**
	Fetch stock data using Alpha Vantage API.
	API provides intraday stock price data at 1-minute intervals.

**Publish to Pub/Sub:**
	The fetched data is published to a Google Cloud Pub/Sub topic for further processing.

**Backup to GCS:**
	Data is also backed up to a Google Cloud Storage (GCS) bucket for archival purposes.

**PySpark Streaming:**
	Data from Pub/Sub is streamed using PySpark.
	The data is processed and written to BigQuery for analysis.

**Calculate Weekly Metrics:**
	A SQL script (calculate_weekly_metrics.sql) calculates weekly aggregations (average, max, min prices, etc.) and stores results in a BigQuery table.

**Airflow DAG:**
	Orchestrates the entire process, managing retries, notifications, and dependencies.

**EXPLAINATION**
Stock Data Pipeline
This repository contains a data pipeline to fetch, process, and analyze stock market data. The pipeline is implemented using the following technologies:
	Airflow for orchestration
	PySpark for streaming data processing
	Google Cloud Platform for storage, messaging, and analytics
	BigQuery for data aggregation and querying
	Alpha Vantage API for fetching stock data
Pipeline Workflow
Fetch Stock Data:
	Data is fetched from the Alpha Vantage API for configured stock symbols.
	The intraday stock prices are retrieved at 1-minute intervals.
Publish to Pub/Sub:
	Fetched data is published to a Google Cloud Pub/Sub topic.

Backup to GCS:
	Fetched data is also backed up as JSON files to a GCS bucket.

Process with PySpark:
	PySpark reads data from Pub/Sub, processes it, and writes it to BigQuery.

Weekly Metrics Calculation:
A SQL script calculates weekly metrics like average, max, min prices, etc., and stores the results in a partitioned BigQuery table.

**Repository Structure**

/
|-- dags/
|   |-- stock_data_dag.py        # Airflow DAG to orchestrate the pipeline
|
|-- scripts/
|   |-- stock_data_pipeline.py   # Script to fetch data and publish to GCP
|   |-- stream_pubsub_to_bigquery.py  # PySpark script to stream data to BigQuery
|
|-- sql/
|   |-- calculate_weekly_metrics.sql  # SQL for calculating weekly metrics
|
|-- Dockerfile                   # Docker configuration (if applicable)
|-- requirements.txt             # Python dependencies
|-- README.md                    # Project documentation

**Prerequisites**

Environment Variables:
	Ensure the following environment variables are set:
	ALPHA_VANTAGE_API_KEY: API key for Alpha Vantage.
	GCP_PROJECT: Google Cloud project ID.
	PUBSUB_TOPIC: Pub/Sub topic name.
	BUCKET_NAME: GCS bucket name.
	STOCK_SYMBOLS: Comma-separated list of stock symbols to track (e.g., AAPL,GOOG,MSFT).
Google Cloud Services:
	Enable Pub/Sub, Cloud Storage, and BigQuery APIs.
Python Requirements:
Install dependencies using pip install -r requirements.txt.
PySpark:
Ensure PySpark is installed and accessible.
Airflow:
Set up Airflow and configure the DAG.

**Usage**
	Start the Airflow DAG:
	Deploy the stock_data_dag.py file to your Airflow DAGs directory.
	Trigger the DAG via the Airflow UI or CLI.
	Run PySpark Job:
	Use the stream_pubsub_to_bigquery.py script to process data from Pub/Sub to BigQuery.
	Weekly Metrics:
	Execute the SQL script calculate_weekly_metrics.sql to calculate weekly aggregations.

**Key Features**
1.	Scalable Design:Uses Google Cloud Pub/Sub for scalable messaging and GCS for data backup.
2.	Real-time Processing:PySpark handles real-time streaming of stock data.
3.	Advanced Aggregations:Weekly metrics include average, max, min, standard deviation, and transaction count.
4.	Error Handling:Retries and notifications for failed tasks are managed by Airflow.
5.	Troubleshooting


**Contributions are welcome! Please submit a pull request with a detailed description of your changes.**


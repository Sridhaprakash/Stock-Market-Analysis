from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType

# Initialize Spark session
spark = SparkSession.builder.appName("StockDataStreaming").getOrCreate()

# Define Pub/Sub subscription
PROJECT_ID = "gcp-stock-project"
SUBSCRIPTION_ID = "stock-data-subscription"
PUBSUB_SOURCE = f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}"

# Define schema
schema = StructType().add("symbol", StringType()).add("price", FloatType()).add("timestamp", StringType())

# Read data from Pub/Sub
streaming_data = (
    spark.readStream.format("pubsub")
    .option("project", PROJECT_ID)
    .option("subscription", SUBSCRIPTION_ID)
    .load()
)

# Process data
processed_data = streaming_data.select(from_json(col("data").cast("string"), schema).alias("data")).select("data.*")

# Write to BigQuery
processed_data.writeStream.format("bigquery").option("table", "gcp_project.stock.stock_data").start()

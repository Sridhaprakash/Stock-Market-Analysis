-- Declare variables for tables
DECLARE raw_data_table STRING DEFAULT 'your_project.your_dataset.raw_data';
DECLARE output_table STRING DEFAULT 'your_project.your_dataset.weekly_metrics';

-- Step 1: Validate raw data and handle NULL values
WITH validated_data AS (
  SELECT
    symbol,
     EXTRACT(WEEK FROM timestamp_column) AS week_number,
    price
  FROM
    `gcp_project.stock.stock_datav`
  WHERE
    symbol IS NOT NULL
    AND week_number IS NOT NULL
    AND price IS NOT NULL
),

-- Step 2: Calculate weekly metrics with advanced aggregations
aggregated_metrics AS (
  SELECT
    symbol,
    week_number,
    AVG(price) AS avg_price,
    MAX(price) AS max_price,
    MIN(price) AS min_price,
    COUNT(price) AS transaction_count,
    STDDEV(price) AS price_stddev
  FROM
    validated_data
  GROUP BY
    symbol, week_number
)

-- Step 3: Write to the output table (overwrites if already exists)
CREATE OR REPLACE TABLE `gcp_project.stock.stock_weekly_analysis`
PARTITION BY week_number
CLUSTER BY symbol AS
SELECT
  symbol,
  week_number,
  avg_price,
  max_price,
  min_price,
  transaction_count,
  price_stddev
FROM
  aggregated_metrics;

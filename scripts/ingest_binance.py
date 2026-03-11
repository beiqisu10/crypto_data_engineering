from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col
import argparse
import requests
import os
import zipfile

def download_and_process():
    # --- 1. Argument Parsing ---
    # Setup CLI arguments for flexibility (allows Airflow to pass dynamic dates)
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD format")
    parser.add_argument("--symbol", default="BTCUSDT", help="Crypto pair symbol")
    parser.add_argument("--output", default="/opt/airflow/scripts/data", help="Final destination for Parquet files")
    args = parser.parse_args()

    SYMBOL = args.symbol
    DATE = args.date
    
    # Binance Vision Public Data URL construction
    ZIP_URL = f"https://data.binance.vision/data/spot/daily/aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-{DATE}.zip"
    OUTPUT_PATH = f"{args.output}/{SYMBOL}_{DATE}.parquet"
    LOCAL_ZIP = f"/tmp/{SYMBOL}-{DATE}.zip"
    CSV_DIR = f"/tmp/{SYMBOL}-{DATE}_extracted/"
    
    # Standard schema for Binance aggregate trades
    column_names = [
      "agg_trade_id", "price", "quantity", "first_id", 
      "last_id", "timestamp", "is_buyer_maker", "is_best_match"
    ]
    
    # --- 2. Data Ingestion (Download & Extraction) ---
    print(f"Downloading {ZIP_URL}...")
    r = requests.get(ZIP_URL)
    r.raise_for_status() # Ensure the download was successful

    # Save the compressed ZIP locally
    with open(LOCAL_ZIP, 'wb') as f:
        f.write(r.content)

    print("Extracting ZIP...")
    with zipfile.ZipFile(LOCAL_ZIP, 'r') as zip_ref:
        zip_ref.extractall(CSV_DIR)

    # --- 3. Spark Processing ---
    # Initialize Spark Session for data transformation
    spark = SparkSession.builder \
        .appName("BinanceDataIngestion") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    # Load extracted CSV into Spark DataFrame
    # Binance CSVs usually don't have headers, so we infer schema
    df = spark.read.csv(CSV_DIR, header=False, inferSchema=True)

    # --- 4. Dynamic Schema Handling ---
    # Handle variations in Binance data formats (some dates may have 7 or 8 columns)
    actual_col_count = len(df.columns)
    if actual_col_count == 8:
        df = df.toDF(*column_names)
    elif actual_col_count == 7:
        df = df.toDF(*column_names[:7])
    else:
        raise ValueError(f"Unexpected column count: {actual_col_count}")
    
    # --- 5. Data Transformation & Cleaning ---
    # Convert Unix timestamp (milliseconds) to readable 'event_time'
    # Cast to double/1000000.0 is used for precise timestamp conversion
    cleaned_df = df.withColumn("event_time", from_unixtime(col("timestamp").cast("double") / 1000000.0)) \
                 .select("event_time", "price", "quantity", "is_buyer_maker")

    # Ensure output directory exists before writing
    os.makedirs(args.output, exist_ok=True)

    # --- 6. Loading to Data Lake ---
    # Save the cleaned data in Parquet format for optimized storage and downstream BigQuery usage
    cleaned_df.write.mode("overwrite").parquet(OUTPUT_PATH)
    
    print(f"Successfully saved to {OUTPUT_PATH}")
    
    # --- 7. Cleanup ---
    # Remove temporary ZIP files to keep the local disk clean
    os.remove(LOCAL_ZIP)
    
    # Stop Spark to free up resources
    spark.stop()

if __name__ == "__main__":
    download_and_process()
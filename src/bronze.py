
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name
)

from delta import configure_spark_with_delta_pip

# -----------------------------------------
# 1. PROJECT CONFIGURATION
# -----------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_PATH = PROJECT_ROOT / "data" / "raw"
BRONZE_PATH = PROJECT_ROOT / "data" / "bronze"

# -----------------------------------------
# 2. CREATE DELTA-ENABLED SPARK SESSION
# -----------------------------------------

builder = (
    SparkSession.builder
    .appName("OlistBronzeLayer")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\nBRONZE LAYER STARTED")

# -----------------------------------------
# 3. DEFINE DATASETS
# -----------------------------------------

datasets = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv"
}


# -----------------------------------------
# 4. READ AND WRITE BRONZE TABLES
# -----------------------------------------

for name, filename in datasets.items():

    source_path = RAW_PATH / filename
    target_path = BRONZE_PATH / name

    print(f"\nProcessing: {name}")

    # Read CSV with multiline and quote handling
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .csv(str(source_path))
    )

    # Add ingestion metadata
    bronze_df = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

   
    # Save data in Delta format
    bronze_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(str(target_path))

    print(f"Bronze table created: {name}")
    print(f"Records saved: {bronze_df.count()}")

print("\nBRONZE LAYER COMPLETED SUCCESSFULLY")

spark.stop()

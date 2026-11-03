
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    trim,
    col
)
from pyspark.sql.types import StringType

from delta import configure_spark_with_delta_pip

# -----------------------------------------
# 1. PROJECT CONFIGURATION
# -----------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BRONZE_PATH = PROJECT_ROOT / "data" / "bronze"
SILVER_PATH = PROJECT_ROOT / "data" / "silver"

# -----------------------------------------
# 2. CREATE SPARK SESSION
# -----------------------------------------

builder = (
    SparkSession.builder
    .appName("OlistSilverLayer")
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

print("\n================================")
print("SILVER LAYER STARTED")
print("================================")

# -----------------------------------------
# 3. DATASETS
# -----------------------------------------

datasets = [
    "customers",
    "geolocation",
    "order_items",
    "order_payments",
    "order_reviews",
    "orders",
    "products",
    "sellers",
    "category_translation"
]

# -----------------------------------------
# 4. CLEAN AND TRANSFORM DATA
# -----------------------------------------

for name in datasets:

    print(f"\nProcessing: {name}")

    source_path = BRONZE_PATH / name
    target_path = SILVER_PATH / name

    # Read Bronze Delta table
    df = (
        spark.read
        .format("delta")
        .load(str(source_path))
    )

    bronze_count = df.count()

    # Identify original business columns
    business_columns = [
        c for c in df.columns
        if not c.startswith("_")
    ]

    # Remove completely empty records
    cleaned_df = df.dropna(
        how="all",
        subset=business_columns
    )

    # Trim whitespace from string columns
    for field in cleaned_df.schema.fields:

        if isinstance(field.dataType, StringType):

            cleaned_df = cleaned_df.withColumn(
                field.name,
                trim(col(field.name))
            )

    # Remove exact duplicate business records
    cleaned_df = cleaned_df.dropDuplicates(
        business_columns
    )

    # Add Silver processing timestamp
    cleaned_df = cleaned_df.withColumn(
        "_silver_processed_at",
        current_timestamp()
    )

    # Save cleaned records as Delta
    (
      cleaned_df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save(str(target_path))
    )

    silver_count = cleaned_df.count()

    print(f"Bronze records: {bronze_count}")
    print(f"Silver records: {silver_count}")
    print(f"Duplicates/empty records removed: {bronze_count - silver_count}")
    print(f"Silver table saved: {name}")

# -----------------------------------------
# 5. COMPLETION
# -----------------------------------------

print("\n================================")
print("SILVER LAYER COMPLETED SUCCESSFULLY")
print("================================")

spark.stop()

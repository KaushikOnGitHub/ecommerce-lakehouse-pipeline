
from pathlib import Path
from pyspark.sql import SparkSession

# -----------------------------------------
# 1. PROJECT CONFIGURATION
# -----------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw"

# -----------------------------------------
# 2. CREATE SPARK SESSION
# -----------------------------------------

spark = (
    SparkSession.builder
    .appName("OlistDataIngestion")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n================================")
print("OLIST DATA INGESTION STARTED")
print("================================\n")

# -----------------------------------------
# 3. DEFINE SOURCE DATASETS
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
# 4. READ CSV FILES
# -----------------------------------------

dataframes = {}

for dataset_name, filename in datasets.items():

    file_path = RAW_DATA_PATH / filename

    if not file_path.exists():
        raise FileNotFoundError(
            f"Dataset not found: {file_path}"
        )

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(file_path))
    )

    dataframes[dataset_name] = df

    print(f"Dataset: {dataset_name}")
    print(f"Columns: {len(df.columns)}")
    print(f"Records: {df.count()}")
    print("-" * 40)

# -----------------------------------------
# 5. DISPLAY SAMPLE DATA
# -----------------------------------------

print("\nCUSTOMER DATA SAMPLE")

dataframes["customers"].show(5, truncate=False)

print("\nORDER DATA SAMPLE")

dataframes["orders"].show(5, truncate=False)

# -----------------------------------------
# 6. DISPLAY DATA INGESTION SUMMARY
# -----------------------------------------

print("\n================================")
print("INGESTION COMPLETED SUCCESSFULLY")
print("================================")

print("Total datasets loaded:", len(dataframes))

# -----------------------------------------
# 7. STOP SPARK
# -----------------------------------------

spark.stop()


from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)

from delta import configure_spark_with_delta_pip


# --------------------------------------------------
# 1. PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_PATH = PROJECT_ROOT / "data" / "silver"
GOLD_PATH = PROJECT_ROOT / "data" / "gold"

builder = (
    SparkSession.builder
    .appName("OlistDataQuality")
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


# --------------------------------------------------
# 2. PRIMARY KEY DEFINITIONS
# --------------------------------------------------

primary_keys = {
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id", "order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "category_translation": ["product_category_name"]
}


# --------------------------------------------------
# 3. DATA QUALITY CHECK FUNCTION
# --------------------------------------------------

results = []


def record_check(dataset, check_name, failed_records, total_records):
    status = "PASS" if failed_records == 0 else "FAIL"

    results.append((
        dataset,
        check_name,
        int(total_records),
        int(failed_records),
        status
    ))

    print(
        f"{dataset:<22} | "
        f"{check_name:<25} | "
        f"{status}"
    )


# --------------------------------------------------
# 4. VALIDATE SILVER TABLES
# --------------------------------------------------

print("\n======================================")
print("STARTING DATA QUALITY VALIDATION")
print("======================================\n")


for dataset, keys in primary_keys.items():

    print(f"\nChecking dataset: {dataset}")

    table_path = SILVER_PATH / dataset
    df = spark.read.format("delta").load(str(table_path))

    total_records = df.count()

    # Check 1: Dataset should not be empty
    record_check(
        dataset,
        "non_empty_dataset",
        0 if total_records > 0 else 1,
        total_records
    )

    # Check 2: Primary key columns must not be NULL
    null_condition = None

    for key in keys:
        condition = col(key).isNull()

        null_condition = (
            condition if null_condition is None
            else null_condition | condition
        )

    null_count = df.filter(null_condition).count()

    record_check(
        dataset,
        "null_primary_keys",
        null_count,
        total_records
    )

    
    # Check 3: Primary key combinations must be unique
    # Exclude records with NULL primary keys before checking duplicates

    valid_key_rows = df.filter(~null_condition)

    duplicate_count = (
        valid_key_rows
        .groupBy(*keys)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    # Investigate order review data
    if dataset == "order_reviews":

        print("\n--- REVIEW DATA INVESTIGATION ---")

        # Identify duplicated review IDs
        duplicate_ids = (
            valid_key_rows
            .groupBy("review_id")
            .agg(
                count("*").alias("row_count")
            )
            .filter(col("row_count") > 1)
        )

        print("\nNumber of duplicated review ID groups:")
        print(duplicate_ids.count())

        # Retrieve all records belonging to duplicated IDs
        duplicate_records = (
            valid_key_rows
            .join(
                duplicate_ids.select("review_id"),
                on="review_id",
                how="inner"
            )
            .orderBy("review_id", "order_id")
        )

        print("\nFull records belonging to duplicate review IDs:")

        duplicate_records.select(
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp"
        ).show(30, truncate=False)

        print("\nDuplicate review ID summary:")

        (
            duplicate_records
            .groupBy("review_id")
            .agg(
                count("*").alias("row_count"),
                countDistinct("order_id").alias("distinct_orders"),
                countDistinct("review_score").alias("distinct_scores"),
                countDistinct("review_creation_date").alias("distinct_creation_dates")
            )
            .orderBy(col("row_count").desc())
            .show(20, truncate=False)
        )

    # Record duplicate check for every dataset
    record_check(
        dataset,
        "duplicate_primary_keys",
        duplicate_count,
        total_records
    )


# --------------------------------------------------
# 5. BUSINESS RULE VALIDATIONS
# --------------------------------------------------

print("\n======================================")
print("BUSINESS RULE VALIDATIONS")
print("======================================\n")


# Order item prices and freight must not be negative
items = spark.read.format("delta").load(
    str(SILVER_PATH / "order_items")
)

invalid_items = items.filter(
    (col("price") < 0) | (col("freight_value") < 0)
).count()

record_check(
    "order_items",
    "negative_price_or_freight",
    invalid_items,
    items.count()
)


# Payment values must not be negative
payments = spark.read.format("delta").load(
    str(SILVER_PATH / "order_payments")
)

invalid_payments = payments.filter(
    col("payment_value") < 0
).count()

record_check(
    "order_payments",
    "negative_payment_value",
    invalid_payments,
    payments.count()
)


# --------------------------------------------------
# 6. SAVE DATA QUALITY REPORT
# --------------------------------------------------

schema = StructType([
    StructField("dataset", StringType(), False),
    StructField("check_name", StringType(), False),
    StructField("total_records", LongType(), False),
    StructField("failed_records", LongType(), False),
    StructField("status", StringType(), False)
])

quality_report = spark.createDataFrame(results, schema)

report_path = GOLD_PATH / "data_quality_results"

(
    quality_report.write
    .format("delta")
    .mode("overwrite")
    .save(str(report_path))
)


# --------------------------------------------------
# 7. FINAL SUMMARY
# --------------------------------------------------

print("\n======================================")
print("DATA QUALITY REPORT")
print("======================================\n")

quality_report.show(50, truncate=False)

total_checks = len(results)
failed_checks = sum(
    1 for result in results if result[4] == "FAIL"
)

print(f"Total checks performed: {total_checks}")
print(f"Passed checks: {total_checks - failed_checks}")
print(f"Failed checks: {failed_checks}")

print("\nDATA QUALITY VALIDATION COMPLETED")

spark.stop()

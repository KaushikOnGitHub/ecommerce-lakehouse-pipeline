
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    countDistinct,
    count,
    to_timestamp,
    date_format,
    desc,
    coalesce,
    lit
)

from delta import configure_spark_with_delta_pip

# -----------------------------------------
# 1. PROJECT CONFIGURATION
# -----------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SILVER_PATH = PROJECT_ROOT / "data" / "silver"
GOLD_PATH = PROJECT_ROOT / "data" / "gold"

# -----------------------------------------
# 2. CREATE SPARK SESSION
# -----------------------------------------

builder = (
    SparkSession.builder
    .appName("OlistGoldLayer")
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
print("GOLD LAYER STARTED")
print("================================")

# -----------------------------------------
# 3. READ SILVER TABLES
# -----------------------------------------

orders = spark.read.format("delta").load(
    str(SILVER_PATH / "orders")
)

order_items = spark.read.format("delta").load(
    str(SILVER_PATH / "order_items")
)

customers = spark.read.format("delta").load(
    str(SILVER_PATH / "customers")
)

products = spark.read.format("delta").load(
    str(SILVER_PATH / "products")
)

categories = spark.read.format("delta").load(
    str(SILVER_PATH / "category_translation")
)

# -----------------------------------------
# 4. PREPARE DELIVERED ORDER DATA
# -----------------------------------------

delivered_orders = (
    orders
    .filter(col("order_status") == "delivered")
    .withColumn(
        "purchase_timestamp",
        to_timestamp(col("order_purchase_timestamp"))
    )
    .filter(col("purchase_timestamp").isNotNull())
)

# Join orders with order items
sales = delivered_orders.join(
    order_items,
    on="order_id",
    how="inner"
)

# Calculate revenue including freight
sales = sales.withColumn(
    "total_revenue",
    coalesce(col("price"), lit(0)) +
    coalesce(col("freight_value"), lit(0))
)

# -----------------------------------------
# 5. GOLD TABLE 1: MONTHLY SALES
# -----------------------------------------

monthly_sales = (
    sales
    .withColumn(
        "sales_month",
        date_format(col("purchase_timestamp"), "yyyy-MM")
    )
    .groupBy("sales_month")
    .agg(
        sum("total_revenue").alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        count("order_item_id").alias("total_items_sold")
    )
    .orderBy("sales_month")
)

(
    monthly_sales.write
    .format("delta")
    .mode("overwrite")
    .save(str(GOLD_PATH / "monthly_sales"))
)

print("\nMONTHLY SALES")
monthly_sales.show(20, truncate=False)

# -----------------------------------------
# 6. GOLD TABLE 2: TOP PRODUCTS
# -----------------------------------------

top_products = (
    sales
    .groupBy("product_id")
    .agg(
        sum("total_revenue").alias("total_revenue"),
        count("order_item_id").alias("units_sold")
    )
    .orderBy(desc("total_revenue"))
)

(
    top_products.write
    .format("delta")
    .mode("overwrite")
    .save(str(GOLD_PATH / "top_products"))
)

print("\nTOP 10 PRODUCTS")
top_products.show(10, truncate=False)


# -----------------------------------------
# 7. GOLD TABLE 3: CUSTOMER ANALYTICS
# -----------------------------------------

customer_sales = (
    sales
    .join(
        customers.select("customer_id", "customer_unique_id"),
        on="customer_id",
        how="left"
    )
)

customer_analytics = (
    customer_sales
    .groupBy("customer_unique_id")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("total_revenue").alias("total_spent")
    )
    .orderBy(desc("total_spent"))
)

(
    customer_analytics.write
    .format("delta")
    .mode("overwrite")
    .save(str(GOLD_PATH / "customer_analytics"))
)

print("\nTOP 10 CUSTOMERS")
customer_analytics.show(10, truncate=False)

# -----------------------------------------
# 8. GOLD TABLE 4: CATEGORY SALES
# -----------------------------------------

category_sales = (
    sales
    .join(products, on="product_id", how="left")
    .join(
        categories,
        on="product_category_name",
        how="left"
    )
    .groupBy(
        coalesce(
            col("product_category_name_english"),
            col("product_category_name"),
            lit("unknown")
        ).alias("product_category")
    )
    .agg(
        sum("total_revenue").alias("total_revenue"),
        count("order_item_id").alias("units_sold"),
        countDistinct("order_id").alias("total_orders")
    )
    .orderBy(desc("total_revenue"))
)

(
    category_sales.write
    .format("delta")
    .mode("overwrite")
    .save(str(GOLD_PATH / "category_sales"))
)

print("\nTOP 10 PRODUCT CATEGORIES")
category_sales.show(10, truncate=False)

# -----------------------------------------
# 9. COMPLETION
# -----------------------------------------

print("\n================================")
print("GOLD LAYER COMPLETED SUCCESSFULLY")
print("================================")

print("Four business analytics tables created.")

spark.stop()

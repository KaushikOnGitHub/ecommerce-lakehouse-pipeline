
from pyspark.sql import SparkSession

# 1. Create Spark Session
spark = (
    SparkSession.builder
    .appName("EcommerceLakehouse")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

# 2. Display Spark information
print("Spark Session Started Successfully!")
print("Spark Version:", spark.version)

# 3. Create sample e-commerce data
data = [
    (101, "Kaushik", "Laptop", 65000),
    (102, "Rahul", "Mobile", 25000),
    (103, "Priya", "Headphones", 3000),
    (104, "Amit", "Keyboard", 2000),
    (105, "Neha", "Monitor", 15000)
]

columns = ["order_id", "customer_name", "product", "amount"]

# 4. Create Spark DataFrame
df = spark.createDataFrame(data, columns)

# 5. Display the data
print("E-Commerce Orders:")
df.show()

# 6. Calculate total sales
print("Total Sales:")
df.selectExpr("SUM(amount) AS total_sales").show()

# 7. Stop Spark
spark.stop()

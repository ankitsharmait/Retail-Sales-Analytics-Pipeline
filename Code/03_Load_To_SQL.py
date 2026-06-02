# Databricks notebook source
# MAGIC %run ./00_ADLS_Config

# COMMAND ----------

silver_df = spark.read.parquet(
    "abfss://silver@retailstorageankit2026.dfs.core.windows.net/retail_sales_cleaned"
)

# COMMAND ----------

#Load Gold Datasets
total_revenue = spark.read.parquet(
    "abfss://gold@retailstorageankit2026.dfs.core.windows.net/total_revenue"
)

revenue_by_category = spark.read.parquet(
    "abfss://gold@retailstorageankit2026.dfs.core.windows.net/revenue_by_category"
)

revenue_by_city = spark.read.parquet(
    "abfss://gold@retailstorageankit2026.dfs.core.windows.net/revenue_by_city"
)

product_performance = spark.read.parquet(
    "abfss://gold@retailstorageankit2026.dfs.core.windows.net/product_performance"
)

monthly_sales = spark.read.parquet(
    "abfss://gold@retailstorageankit2026.dfs.core.windows.net/monthly_sales"
)

category_city_sales = spark.read.parquet(
    "abfss://gold@retailstorageankit2026.dfs.core.windows.net/category_city_sales"
)

payment_analysis = spark.read.parquet(
    "abfss://gold@retailstorageankit2026.dfs.core.windows.net/payment_analysis"
)

# COMMAND ----------

#Create SQL Tables

from pyspark.sql import Row
from pyspark.sql.functions import avg

revenue_by_category.write.mode("overwrite").saveAsTable("revenue_by_category")

revenue_by_city.write.mode("overwrite").saveAsTable("revenue_by_city")

product_performance.write.mode("overwrite").saveAsTable("product_performance")

monthly_sales.write.mode("overwrite").saveAsTable("monthly_sales")

category_city_sales.write.mode("overwrite").saveAsTable("category_city_sales")

payment_analysis.write.mode("overwrite").saveAsTable("payment_analysis")

total_revenue.write.mode("overwrite").saveAsTable("total_revenue")


total_orders = silver_df.select("transaction_id").distinct().count()

spark.createDataFrame(
    [Row(total_orders=total_orders)]
).write.mode("overwrite").saveAsTable("total_orders")


aov = silver_df.agg(
    avg("sales_amount")
).collect()[0][0]

spark.createDataFrame(
    [Row(avg_order_value=float(aov))]
).write.mode("overwrite").saveAsTable("average_order_value")

# COMMAND ----------

spark.table("total_orders") \
    .write \
    .mode("overwrite") \
    .parquet(
        "abfss://gold@retailstorageankit2026.dfs.core.windows.net/total_orders"
    )

# COMMAND ----------

spark.table("average_order_value") \
    .write \
    .mode("overwrite") \
    .parquet(
        "abfss://gold@retailstorageankit2026.dfs.core.windows.net/average_order_value"
    )

# COMMAND ----------

spark.sql("SHOW TABLES").show(truncate=False)

# COMMAND ----------

display(
    spark.table("total_revenue")
)

# COMMAND ----------

display(
    spark.table("total_orders")
)

# COMMAND ----------

display(
    spark.table("average_order_value")
)

# COMMAND ----------

display(
    spark.table("revenue_by_category")
)

# COMMAND ----------

display(
    spark.table("revenue_by_city")
)

# COMMAND ----------

display(
    spark.table("product_performance")
)

# COMMAND ----------

display(
    spark.table("monthly_sales")
)

# COMMAND ----------

display(
    spark.table("payment_analysis")
)

# COMMAND ----------

display(
    spark.table("category_city_sales")
)

# COMMAND ----------

tables = [
    "total_revenue",
    "total_orders",
    "average_order_value",
    "revenue_by_category",
    "revenue_by_city",
    "product_performance",
    "monthly_sales",
    "payment_analysis",
    "category_city_sales"
]

for table in tables:

    spark.table(table) \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(f"abfss://gold@retailstorageankit2026.dfs.core.windows.net/csv_exports/{table}")

print("All CSV files exported successfully")

# COMMAND ----------


# Databricks notebook source
# MAGIC %run ./00_ADLS_Config

# COMMAND ----------

from pyspark.sql.functions import when, upper, trim, col

silver_df = spark.read.parquet(
    "abfss://silver@retailstorageankit2026.dfs.core.windows.net/retail_sales_cleaned"
)

silver_df = silver_df.withColumn(
    "category",
    upper(trim(col("category")))
)

silver_df = silver_df.withColumn(
    "category",
    when(col("category") == "ELEC", "ELECTRONICS")
    .when(col("category") == "FURN", "FURNITURE")
    .when(col("category") == "HOME", "HOME APPLIANCES")
    .when(col("category") == "CLOTH", "CLOTHING")
    .otherwise(col("category"))
)

silver_df.write.mode("overwrite").parquet(
    "abfss://silver@retailstorageankit2026.dfs.core.windows.net/retail_sales_cleaned"
)

# COMMAND ----------

# Read Silver Layer

silver_df = spark.read.parquet(
    "abfss://silver@retailstorageankit2026.dfs.core.windows.net/retail_sales_cleaned"
)

silver_df.printSchema()

# COMMAND ----------

# KPI 1:Total Revenue
from pyspark.sql.functions import sum

total_revenue = silver_df.agg(
    sum("sales_amount").alias("total_revenue")
)

display(total_revenue)

# COMMAND ----------

total_revenue.write.mode("overwrite").parquet(
    "abfss://gold@retailstorageankit2026.dfs.core.windows.net/total_revenue"
)

# COMMAND ----------

#KPI 2 : Revenue by Category
from pyspark.sql.functions import sum

category_sales = silver_df.groupBy(
    "category"
).agg(
    sum("sales_amount").alias("total_revenue")
)

# COMMAND ----------

from pyspark.sql.functions import col
category_sales = category_sales.orderBy(
    col("total_revenue").desc()
)

display(category_sales)

# COMMAND ----------

category_sales.write.mode("overwrite").parquet(
    "abfss://gold@retailstorageankit2026.dfs.core.windows.net/revenue_by_category"
)

# COMMAND ----------

#KPI 3 : Revenue by City
city_sales = silver_df.groupBy(
    "city"
).agg(
    sum("sales_amount").alias("total_revenue")
)

city_sales = city_sales.orderBy(
    col("total_revenue").desc()
)

display(city_sales)

# COMMAND ----------

city_sales.write.mode("overwrite").parquet(
    "abfss://gold@retailstorageankit2026.dfs.core.windows.net/revenue_by_city"
)

# COMMAND ----------

#KPI 4 : Product Performance
from pyspark.sql.functions import sum

product_sales = silver_df.groupBy(
    "product_name"
).agg(
    sum("quantity").alias("units_sold"),
    sum("sales_amount").alias("revenue")
)

product_sales = product_sales.orderBy(
    col("revenue").desc()
)

display(product_sales)

# COMMAND ----------

product_sales.write.mode("overwrite").parquet(
    "abfss://gold@retailstorageankit2026.dfs.core.windows.net/product_performance"
)

# COMMAND ----------

#KPI 5 : Monthly Revenue Trend
from pyspark.sql.functions import year
from pyspark.sql.functions import month

monthly_sales = silver_df.groupBy(
    year("transaction_date").alias("year"),
    month("transaction_date").alias("month")
).agg(
    sum("sales_amount").alias("total_revenue")
)

# COMMAND ----------

monthly_sales = monthly_sales.orderBy(
    "year",
    "month"
)

display(monthly_sales)

# COMMAND ----------

monthly_sales.write.mode("overwrite").parquet(
    "abfss://gold@retailstorageankit2026.dfs.core.windows.net/monthly_sales"
)

# COMMAND ----------

#KPI 6 : Payment Method Analysis
payment_analysis = silver_df.groupBy(
    "payment_method"
).agg(
    sum("sales_amount").alias("revenue")
)

display(payment_analysis)

# COMMAND ----------

payment_analysis.write.mode("overwrite").parquet(
    "abfss://gold@retailstorageankit2026.dfs.core.windows.net/payment_analysis"
)

# COMMAND ----------

#KPI 7 : Category + City Matrix
category_city_sales = silver_df.groupBy(
    "city",
    "category"
).agg(
    sum("sales_amount").alias("revenue")
)

display(category_city_sales)

# COMMAND ----------

category_city_sales.write.mode("overwrite").parquet(
    "abfss://gold@retailstorageankit2026.dfs.core.windows.net/category_city_sales"
)

# COMMAND ----------

print("Silver Records :", silver_df.count())

print("Revenue By Category :", category_sales.count())

print("Revenue By City :", city_sales.count())

print("Product Performance :", product_sales.count())

print("Monthly Sales :", monthly_sales.count())
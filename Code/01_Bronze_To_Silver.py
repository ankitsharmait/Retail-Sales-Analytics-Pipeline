# Databricks notebook source
# MAGIC %run ./00_ADLS_Config

# COMMAND ----------

# Read Files From Bronze

retail1_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(
        "abfss://bronze@retailstorageankit2026.dfs.core.windows.net/retail_data1.csv"
    )

retail2_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(
        "abfss://bronze@retailstorageankit2026.dfs.core.windows.net/retail_data2.csv"
    )
product_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(
        "abfss://bronze@retailstorageankit2026.dfs.core.windows.net/product_details.csv"
    )    

# COMMAND ----------

#Merge Retail Sources
retail_df = retail1_df.unionByName(retail2_df)

# COMMAND ----------

print("Total Raw Records:", retail_df.count())

# COMMAND ----------

#Remove Duplicate Transactions

retail_df = retail_df.dropDuplicates(["transaction_id"])

# COMMAND ----------

#Validation
duplicate_count = retail_df.groupBy("transaction_id") \
    .count() \
    .filter("count > 1") \
    .count()

print("Duplicate Transactions:", duplicate_count)

# COMMAND ----------

#Standardize Text Columns
from pyspark.sql.functions import upper, trim
retail_df = retail_df \
    .withColumn("product_name", upper(trim("product_name"))) \
    .withColumn("category", upper(trim("category"))) \
    .withColumn("city", upper(trim("city"))) \
    .withColumn("purchase_location", upper(trim("purchase_location"))) \
    .withColumn("payment_method", upper(trim("payment_method"))) \
    .withColumn("payment_status", upper(trim("payment_status")))

# COMMAND ----------

#Fix Date Format
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import to_date
from pyspark.sql.functions import col
retail_df = retail_df.withColumn(
    "transaction_date",
    regexp_replace(
        col("transaction_date"),
        "-",
        "/"
    )
)

# COMMAND ----------

#Convert to Date
retail_df = retail_df.withColumn(
    "transaction_date",
    to_date(
        col("transaction_date"),
        "M/d/yyyy"
    )
)

# COMMAND ----------

retail_df.select("transaction_date").show(10, False)

# COMMAND ----------

#Fix Data Types
retail_df = retail_df \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("quantity", col("quantity").cast("int")) \
    .withColumn("discount", col("discount").cast("double"))

# COMMAND ----------

retail_df.printSchema()

# COMMAND ----------

#Join Product Master
product_df = product_df.withColumnRenamed(
    "price",
    "master_price"
)

# COMMAND ----------

silver_df = retail_df.join(
    product_df.select(
        "product_id",
        "master_price"
    ),
    "product_id",
    "left"
)

# COMMAND ----------

#Handle Missing Prices 
silver_df = silver_df.withColumn(
    "price",
    coalesce(
        col("price"),
        col("master_price")
    )
)

# COMMAND ----------

print(
    "Null Prices:",
    silver_df.filter(
        col("price").isNull()
    ).count()
)

# COMMAND ----------

#Handle Invalid Quantity
silver_df = silver_df.filter(
    col("quantity") > 0
)

# COMMAND ----------

print(
    "Invalid Quantity:",
    silver_df.filter(
        col("quantity") <= 0
    ).count()
)

# COMMAND ----------

#PII Masking
from pyspark.sql.functions import sha2
from pyspark.sql.functions import concat
from pyspark.sql.functions import lit
#Email Hashing
silver_df = silver_df.withColumn(
    "email_masked",
    sha2(
        col("email"),
        256
    )
)
#phone masking
silver_df = silver_df.withColumn(
    "phone_masked",
    concat(
        col("phone").substr(1,2),
        lit("XXXXXX"),
        col("phone").substr(9,2)
    )
)

# COMMAND ----------

#Remove Original PII
silver_df = silver_df.drop(
    "email",
    "phone"
)

# COMMAND ----------

#Calculate Sales Amount
silver_df = silver_df.withColumn(
    "sales_amount",
    col("price") * col("quantity")
)

# COMMAND ----------

silver_df.select(
    "price",
    "quantity",
    "sales_amount"
).show(10, False)

# COMMAND ----------

#Final Data Quality Validation
print(
    "Total Records:",
    silver_df.count()
)

print(
    "Duplicate Transactions:",
    silver_df.groupBy("transaction_id")
    .count()
    .filter("count > 1")
    .count()
)

print(
    "Null Prices:",
    silver_df.filter(
        col("price").isNull()
    ).count()
)

print(
    "Invalid Quantity:",
    silver_df.filter(
        col("quantity") <= 0
    ).count()
)

# COMMAND ----------

#Save Curated Silver Layer
silver_df.write \
    .mode("overwrite") \
    .parquet(
        "abfss://silver@retailstorageankit2026.dfs.core.windows.net/retail_sales_cleaned"
    )

# COMMAND ----------

silver_check = spark.read.parquet(
    "abfss://silver@retailstorageankit2026.dfs.core.windows.net/retail_sales_cleaned"
)

silver_check.printSchema()
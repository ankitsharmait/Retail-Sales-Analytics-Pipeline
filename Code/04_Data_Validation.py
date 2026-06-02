# Databricks notebook source
# MAGIC %run ./00_ADLS_Config

# COMMAND ----------
# data validations
spark.table("revenue_by_category").show()

spark.table("revenue_by_city").show()

spark.table("product_performance").show()

spark.table("monthly_sales").show()

spark.table("payment_analysis").show()

spark.table("total_revenue").show()

spark.table("total_orders").show()

spark.table("average_order_value").show()
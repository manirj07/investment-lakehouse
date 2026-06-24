# Databricks notebook source
from pyspark.sql import functions as F

date_df = (
    spark.table(
        "adb_investment_platform_dev.investment_bronze.market_prices"
    )
    .select("date")
    .distinct()
)

# COMMAND ----------

dim_date_df = (
    date_df
    .withColumn(
        "date_key",
        F.date_format("date", "yyyyMMdd").cast("int")
    )
    .withColumn(
        "year",
        F.year("date")
    )
    .withColumn(
        "quarter",
        F.quarter("date")
    )
    .withColumn(
        "month",
        F.month("date")
    )
    .withColumn(
        "month_name",
        F.date_format("date", "MMMM")
    )
)

# COMMAND ----------

dim_date_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adb_investment_platform_dev.investment_gold.dim_date"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adb_investment_platform_dev.investment_gold.dim_date
# MAGIC LIMIT 10;

# COMMAND ----------


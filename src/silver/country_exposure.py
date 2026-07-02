def run():
    print("Running country_exposure")

# Databricks notebook source
from pyspark.sql import functions as F

weights_df = spark.table(
    "adb_investment_platform_dev.investment_silver.portfolio_weights"
)

security_df = spark.table(
    "adb_investment_platform_dev.investment_bronze.security_master"
)

country_exposure_df = (
    weights_df.alias("w")
    .join(
        security_df.alias("s"),
        on="ticker",
        how="inner"
    )
    .groupBy(
        "portfolio_id",
        "country"
    )
    .agg(
        F.round(
            F.sum("weight_pct"),
            2
        ).alias("country_weight_pct")
    )
)

# COMMAND ----------

country_exposure_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adb_investment_platform_dev.investment_silver.country_exposure"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adb_investment_platform_dev.investment_silver.country_exposure

# COMMAND ----------


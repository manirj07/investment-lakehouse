# Databricks notebook source
from pyspark.sql import functions as F

weights_df = spark.table(
    "adb_investment_platform_dev.investment_silver.portfolio_weights"
)

security_df = spark.table(
    "adb_investment_platform_dev.investment_bronze.security_master"
)

# COMMAND ----------

sector_exposure_df = (
    weights_df.alias("w")
    .join(
        security_df.alias("s"),
        on="ticker",
        how="inner"
    )
)

# COMMAND ----------

sector_exposure_df = (
    sector_exposure_df
    .groupBy(
        "portfolio_id",
        "sector"
    )
    .agg(
        F.round(
            F.sum("weight_pct"),
            2
        ).alias("sector_weight_pct")
    )
)

# COMMAND ----------

sector_exposure_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adb_investment_platform_dev.investment_silver.sector_exposure"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adb_investment_platform_dev.investment_silver.sector_exposure
# MAGIC ORDER BY sector_weight_pct DESC;

# COMMAND ----------


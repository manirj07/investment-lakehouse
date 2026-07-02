def run():
    print("Running performance_attribution")

# Databricks notebook source
from pyspark.sql import functions as F

returns_df = spark.table(
    "adb_investment_platform_dev.investment_silver.daily_returns"
)

weights_df = spark.table(
    "adb_investment_platform_dev.investment_silver.portfolio_weights"
)

performance_attribution_df = (
    returns_df.alias("r")
    .join(
        weights_df.select(
            "portfolio_id",
            "ticker",
            "weight_pct"
        ).alias("w"),
        on="ticker",
        how="inner"
    )
    .withColumn(
        "contribution_pct",
        F.round(
            (
                F.col("weight_pct")
                * F.col("daily_return_pct")
            ) / 100,
            6
        )
    )
)

# COMMAND ----------

performance_attribution_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adb_investment_platform_dev.investment_silver.performance_attribution"
    )
    

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     date,
# MAGIC     ticker,
# MAGIC     weight_pct,
# MAGIC     daily_return_pct,
# MAGIC     contribution_pct
# MAGIC FROM adb_investment_platform_dev.investment_silver.performance_attribution
# MAGIC ORDER BY date
# MAGIC LIMIT 20;

# COMMAND ----------


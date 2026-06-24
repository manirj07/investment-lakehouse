# Databricks notebook source
positions_df = spark.table(
    "adb_investment_platform_dev.investment_silver.portfolio_positions"
)

# COMMAND ----------

from pyspark.sql import functions as F

portfolio_totals_df = (
    positions_df
    .groupBy("portfolio_id")
    .agg(
        F.sum("market_value")
        .alias("portfolio_value")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate Weights

# COMMAND ----------

portfolio_weights_df = (
    positions_df.alias("p")
    .join(
        portfolio_totals_df.alias("t"),
        on="portfolio_id",
        how="inner"
    )
    .withColumn(
        "weight_pct",
        F.round(
            (F.col("market_value") /
             F.col("portfolio_value")) * 100,
            2
        )
    )
)

# COMMAND ----------

portfolio_weights_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adb_investment_platform_dev.investment_silver.portfolio_weights"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     market_value,
# MAGIC     weight_pct
# MAGIC FROM adb_investment_platform_dev.investment_silver.portfolio_weights
# MAGIC ORDER BY weight_pct DESC;
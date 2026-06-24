# Databricks notebook source
from pyspark.sql import functions as F

returns_df = spark.table(
    "adb_investment_platform_dev.investment_silver.daily_returns"
)

weights_df = spark.table(
    "adb_investment_platform_dev.investment_silver.portfolio_weights"
)

# COMMAND ----------

# MAGIC %md
# MAGIC join returns with weights

# COMMAND ----------

portfolio_returns_df = (
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
)

# COMMAND ----------

portfolio_returns_df = (
    portfolio_returns_df
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

# MAGIC %md
# MAGIC Aggregate To Portfolio Level

# COMMAND ----------

portfolio_daily_performance_df = (
    portfolio_returns_df
    .groupBy(
        "portfolio_id",
        "date"
    )
    .agg(
        F.round(
            F.sum("contribution_pct"),
            6
        ).alias("portfolio_return_pct")
    )
)

# COMMAND ----------

portfolio_daily_performance_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adb_investment_platform_dev.investment_silver.portfolio_daily_performance"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adb_investment_platform_dev.investment_silver.portfolio_daily_performance
# MAGIC ORDER BY date
# MAGIC LIMIT 10;

# COMMAND ----------


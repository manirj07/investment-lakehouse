def run():
    print("Running daily_returns")

# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

market_prices_df = spark.table(
    "adb_investment_platform_dev.investment_bronze.market_prices"
)

# COMMAND ----------

window_spec = (
    Window
    .partitionBy("ticker")
    .orderBy("date")
)

# COMMAND ----------

returns_df = (
    market_prices_df
    .withColumn(
        "previous_close",
        F.lag("close").over(window_spec)
    )
)

# COMMAND ----------

returns_df = (
    returns_df
    .withColumn(
        "daily_return_pct",
        F.round(
            (
                (F.col("close") - F.col("previous_close"))
                / F.col("previous_close")
            ) * 100,
            4
        )
    )
)

# COMMAND ----------

returns_df = returns_df.filter(
    F.col("previous_close").isNotNull()
)

# COMMAND ----------

returns_df = returns_df.select(
    "ticker",
    "date",
    "close",
    "previous_close",
    "daily_return_pct"
)

# COMMAND ----------

returns_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adb_investment_platform_dev.investment_silver.daily_returns"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adb_investment_platform_dev.investment_silver.daily_returns
# MAGIC WHERE ticker = 'AAPL'
# MAGIC ORDER BY date
# MAGIC LIMIT 10;
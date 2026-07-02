def run():
    print("Running portfolio_positions")

# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql.window import Window

market_price_df = spark.table("adb_investment_platform_dev.investment_bronze.market_prices")
holdings_df = spark.table("adb_investment_platform_dev.investment_bronze.holdings")

# COMMAND ----------

# DBTITLE 1,Cell 2
window_spec = Window.partitionBy("ticker") \
                    .orderBy(f.col("date").desc())

latest_prices_df =  (market_price_df.withColumn("row_num", f.row_number().over(window_spec))
                     .filter(f.col("row_num") == 1)
                     .drop("row_num"))


# COMMAND ----------

latest_prices_df.select(
    "ticker",
    "date",
    "close"
).show()

# COMMAND ----------

postfolio_positions_df = (holdings_df.alias("h").join(latest_prices_df.alias("p"), on ="ticker", how = "inner"))


# COMMAND ----------

portfolio_positions_df = (postfolio_positions_df.withColumn("market_value",f.round(f.col("shares") * f.col("close"),2)) \
    .withColumn("load_timestamp",f.current_timestamp()))

# COMMAND ----------

portfolio_positions_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adb_investment_platform_dev.investment_silver.portfolio_positions"
    )

# COMMAND ----------


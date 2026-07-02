def run():
    print("Running fact_portfolio_performance")

# Databricks notebook source

from pyspark.sql import functions as F

positions_df = spark.table(
    "adb_investment_platform_dev.investment_silver.portfolio_positions"
)

weights_df = spark.table(
    "adb_investment_platform_dev.investment_silver.portfolio_weights"
)

dim_security_df = spark.table(
    "adb_investment_platform_dev.investment_gold.dim_security"
)

dim_portfolio_df = spark.table(
    "adb_investment_platform_dev.investment_gold.dim_portfolio"
)

dim_date_df = spark.table(
    "adb_investment_platform_dev.investment_gold.dim_date"
)



# COMMAND ----------

fact_df = (
    positions_df.alias("p")
    .join(
        weights_df.select(
            "portfolio_id",
            "ticker",
            "weight_pct"
        ).alias("w"),
        ["portfolio_id", "ticker"],
        "inner"
    )
)

# COMMAND ----------

fact_df = (
    fact_df
    .join(
        dim_security_df.select(
            "security_key",
            "ticker"
        ),
        "ticker",
        "inner"
    )
)

# COMMAND ----------

fact_df = (
    fact_df
    .join(
        dim_portfolio_df.select(
            "portfolio_key",
            "portfolio_id"
        ),
        "portfolio_id",
        "inner"
    )
)

# COMMAND ----------

latest_date = (
    spark.table(
        "adb_investment_platform_dev.investment_bronze.market_prices"
    )
    .agg(F.max("date").alias("max_date"))
    .collect()[0]["max_date"]
)

# COMMAND ----------

date_key = int(
    latest_date.strftime("%Y%m%d")
)

# COMMAND ----------

fact_df = fact_df.withColumn(
    "date_key",
    F.lit(date_key)
)

# COMMAND ----------

fact_df = fact_df.select(
    "date_key",
    "portfolio_key",
    "security_key",
    "shares",
    "market_value",
    "weight_pct"
)

# COMMAND ----------

fact_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adb_investment_platform_dev.investment_gold.fact_portfolio_performance"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adb_investment_platform_dev.investment_gold.fact_portfolio_performance;

# COMMAND ----------


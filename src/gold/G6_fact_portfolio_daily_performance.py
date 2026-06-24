# Databricks notebook source
portfolio_daily_performance_df = spark.table(
    "adb_investment_platform_dev.investment_silver.portfolio_daily_performance"
)

portfolio_daily_performance_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adb_investment_platform_dev.investment_gold.fact_portfolio_daily_performance"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adb_investment_platform_dev.investment_gold.fact_portfolio_daily_performance
# MAGIC LIMIT 10;

# COMMAND ----------


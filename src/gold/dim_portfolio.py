def run():
    print("Running dim_portfolio")

# Databricks notebook source
portfolio_data = [
    (
        1,
        "P001",
        "Global Growth Fund",
        "John Smith"
    )
]

columns = [
    "portfolio_key",
    "portfolio_id",
    "portfolio_name",
    "portfolio_manager"
]

dim_portfolio_df = spark.createDataFrame(
    portfolio_data,
    columns
)

# COMMAND ----------

dim_portfolio_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adb_investment_platform_dev.investment_gold.dim_portfolio"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN adb_investment_platform_dev.investment_gold;

# COMMAND ----------


# Databricks notebook source
security_master = [
    ("AAPL", "Apple Inc", "Technology", "USA", "USD"),
    ("MSFT", "Microsoft", "Technology", "USA", "USD"),
    ("NVDA", "NVIDIA", "Technology", "USA", "USD"),
    ("SAP", "SAP SE", "Technology", "Germany", "EUR"),
    ("ALV.DE", "Allianz SE", "Financials", "Germany", "EUR")
]

columns = [
    "ticker",
    "security_name",
    "sector",
    "country",
    "currency"
]

security_master_df = spark.createDataFrame(
    security_master,
    columns
)

display(security_master_df)

# COMMAND ----------

security_master_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adb_investment_platform_dev.investment_bronze.security_master"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adb_investment_platform_dev.investment_bronze.security_master;

# COMMAND ----------


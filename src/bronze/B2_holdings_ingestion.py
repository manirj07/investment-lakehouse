# Databricks notebook source
holdings = [
    ("P001", "AAPL", 100),
    ("P001", "MSFT", 150),
    ("P001", "NVDA", 75),
    ("P001", "SAP", 200),
    ("P001", "ALV.DE", 100)
]
columns = [
    "portfolio_id",
    "ticker",
    "shares"
]
holdings_df  = spark.createDataFrame(holdings, columns)
display(holdings_df )

# COMMAND ----------

holdings_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("adb_investment_platform_dev.investment_bronze.holdings")

# COMMAND ----------

holdings_df.count()

# COMMAND ----------


# Databricks notebook source
# MAGIC %pip install yfinance

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import yfinance as yf
import pandas as pd

tickers = [
    "AAPL",
    "MSFT",
    "NVDA",
    "SAP",
    "ALV.DE"
]

all_data = []

for ticker in tickers:

    print(f"Loading {ticker}")

    df = yf.download(
        ticker,
        period="1y",
        auto_adjust=True,
        progress=False
    )

    # Flatten MultiIndex
    df.columns = [col[0] for col in df.columns]

    df = df.reset_index()

    df["ticker"] = ticker

    all_data.append(df)

market_df = pd.concat(
    all_data,
    ignore_index=True
)

market_df.columns = [
    c.lower()
    for c in market_df.columns
]

print(market_df.shape)
market_df.head()

# COMMAND ----------

print(market_df.columns)
print(market_df.shape)

# COMMAND ----------

spark_df = spark.createDataFrame(market_df)

from pyspark.sql.functions import current_timestamp

spark_df = spark_df.withColumn(
    "ingestion_timestamp",
    current_timestamp()
)

# COMMAND ----------

spark_df.groupBy("ticker").count().show()

# COMMAND ----------

spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adb_investment_platform_dev.investment_bronze.market_prices"
    )

# COMMAND ----------


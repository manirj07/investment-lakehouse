import yfinance as yf
import pandas as pd

from pyspark.sql.functions import current_timestamp

from common.spark_session import get_spark


def run():

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

    spark = get_spark()

    spark_df = spark.createDataFrame(market_df)

    spark_df = spark_df.withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )

    spark_df.groupBy("ticker").count().show()

    spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/opt/data/bronze/market_prices")

    print("Bronze market_prices loaded successfully")


if __name__ == "__main__":
    run()
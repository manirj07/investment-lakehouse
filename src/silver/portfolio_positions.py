from common.spark_session import get_spark
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number,
    col,
    round
)

def run():

    spark = get_spark()

    holdings_df = spark.read.format("delta").load(
        "/opt/data/bronze/holdings"
    )

    prices_df = spark.read.format("delta").load(
        "/opt/data/bronze/market_prices"
    )

    latest_price_window = Window.partitionBy(
        "ticker"
    ).orderBy(
        col("date").desc()
    )

    latest_prices_df = (
        prices_df
        .withColumn(
            "rn",
            row_number().over(latest_price_window)
        )
        .filter(col("rn") == 1)
        .select(
            "ticker",
            col("close").alias("latest_price")
        )
    )

    portfolio_df = (
        holdings_df
        .join(
            latest_prices_df,
            "ticker",
            "inner"
        )
        .withColumn(
            "market_value",
            col("quantity").cast("double")
            * col("latest_price")
        )
        .withColumn(
            "cost_basis",
            col("quantity").cast("double")
            * col("purchase_price").cast("double")
        )
        .withColumn(
            "unrealized_pnl",
            col("market_value")
            - col("cost_basis")
        )
    )

    portfolio_df = (
        portfolio_df
        .withColumn(
            "market_value",
            round("market_value", 2)
        )
        .withColumn(
            "cost_basis",
            round("cost_basis", 2)
        )
        .withColumn(
            "unrealized_pnl",
            round("unrealized_pnl", 2)
        )
    )

    portfolio_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/opt/data/silver/portfolio_positions")

    print("Portfolio positions created")

if __name__ == "__main__":
    run()
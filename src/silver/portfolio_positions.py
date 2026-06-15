from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_portfolio_positions(spark):

    market_prices_df = spark.table(
        "adb_investment_platform_dev.investment_bronze.market_prices"
    )

    holdings_df = spark.table(
        "adb_investment_platform_dev.investment_bronze.holdings"
    )

    window_spec = Window.partitionBy("ticker") \
                        .orderBy(F.col("date").desc())

    latest_prices_df = (
        market_prices_df
        .withColumn(
            "row_num",
            F.row_number().over(window_spec)
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    portfolio_positions_df = (
        holdings_df
        .join(
            latest_prices_df,
            on="ticker",
            how="inner"
        )
        .withColumn(
            "market_value",
            F.round(
                F.col("shares") * F.col("close"),
                2
            )
        )
        .withColumn(
            "load_timestamp",
            F.current_timestamp()
        )
    )

    portfolio_positions_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(
            "adb_investment_platform_dev.investment_silver.portfolio_positions"
        )

    return portfolio_positions_df
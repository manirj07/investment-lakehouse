from common.spark_session import get_spark
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col


def run():

    spark = get_spark()

    prices_df = spark.read.format("delta").load(
        "/opt/data/bronze/market_prices"
    )

    window_spec = Window.partitionBy("ticker").orderBy("date")

    returns_df = (
        prices_df
        .withColumn(
            "previous_close",
            lag("close").over(window_spec)
        )
        .withColumn(
            "daily_return_pct",
            (
                (col("close") - col("previous_close"))
                / col("previous_close")
            ) * 100
        )
    )

    returns_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/opt/data/silver/daily_returns")

    print("Daily returns created")


if __name__ == "__main__":
    run()
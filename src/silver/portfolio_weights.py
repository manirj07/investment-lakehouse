from common.spark_session import get_spark
from pyspark.sql import Window
from pyspark.sql.functions import sum, col, round

def run():

    spark = get_spark()

    positions_df = spark.read.format("delta").load(
        "/opt/data/silver/portfolio_positions"
    )

    portfolio_window = Window.partitionBy(
        "portfolio_id"
    )

    weights_df = (
        positions_df
        .withColumn(
            "portfolio_value",
            sum("market_value").over(portfolio_window)
        )
        .withColumn(
            "weight_pct",
            (
                col("market_value")
                / col("portfolio_value")
            ) * 100
        )
        .withColumn(
            "weight_pct",
            round("weight_pct", 2)
        )
    )

    weights_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/opt/data/silver/portfolio_weights")

    print("Portfolio weights created")

if __name__ == "__main__":
    run()
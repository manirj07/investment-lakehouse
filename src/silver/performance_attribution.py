from common.spark_session import get_spark
from pyspark.sql.functions import col, round

def run():

    spark = get_spark()

    returns_df = spark.read.format("delta").load(
        "/opt/data/silver/daily_returns"
    )

    weights_df = spark.read.format("delta").load(
        "/opt/data/silver/portfolio_weights"
    )

    attribution_df = (
        returns_df
        .join(
            weights_df.select(
                "portfolio_id",
                "ticker",
                "weight_pct"
            ),
            "ticker"
        )
        .withColumn(
            "contribution_pct",
            round(
                (
                    col("daily_return_pct")
                    * col("weight_pct")
                ) / 100,
                4
            )
        )
    )

    attribution_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(
            "/opt/data/silver/performance_attribution"
        )

if __name__ == "__main__":
    run()
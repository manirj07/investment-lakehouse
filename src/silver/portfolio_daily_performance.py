from common.spark_session import get_spark
from pyspark.sql.functions import (
    col,
    sum,
    round
)

def run():

    spark = get_spark()

    returns_df = spark.read.format("delta").load(
        "/opt/data/silver/daily_returns"
    )

    weights_df = spark.read.format("delta").load(
        "/opt/data/silver/portfolio_weights"
    )

    portfolio_returns_df = (
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
            (
                col("daily_return_pct")
                * col("weight_pct")
            ) / 100
        )
        .groupBy(
            "portfolio_id",
            "date"
        )
        .agg(
            round(
                sum("contribution_pct"),
                4
            ).alias(
                "portfolio_return_pct"
            )
        )
    )

    portfolio_returns_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(
            "/opt/data/silver/portfolio_daily_performance"
        )

    print(
        "Portfolio daily performance created"
    )

if __name__ == "__main__":
    run()
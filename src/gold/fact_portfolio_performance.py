from common.spark_session import get_spark
from pyspark.sql.functions import (
    avg,
    max,
    min,
    round
)


def run():

    spark = get_spark()

    daily_df = spark.read.format("delta").load(
        "/opt/data/silver/portfolio_daily_performance"
    )

    summary_df = (
        daily_df
        .groupBy("portfolio_id")
        .agg(
            round(
                avg("portfolio_return_pct"),
                4
            ).alias("avg_daily_return_pct"),

            round(
                max("portfolio_return_pct"),
                4
            ).alias("best_day_return_pct"),

            round(
                min("portfolio_return_pct"),
                4
            ).alias("worst_day_return_pct")
        )
    )

    summary_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(
            "/opt/data/gold/fact_portfolio_performance"
        )

    print("fact_portfolio_performance created")


if __name__ == "__main__":
    run()
from common.spark_session import get_spark


def run():

    spark = get_spark()

    fact_df = spark.read.format("delta").load(
        "/opt/data/silver/portfolio_daily_performance"
    )

    fact_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(
            "/opt/data/gold/fact_portfolio_daily_performance"
        )

    print("fact_portfolio_daily_performance created")


if __name__ == "__main__":
    run()
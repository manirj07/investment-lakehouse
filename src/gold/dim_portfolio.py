from common.spark_session import get_spark


def run():

    spark = get_spark()

    positions_df = spark.read.format("delta").load(
        "/opt/data/silver/portfolio_positions"
    )

    dim_portfolio_df = (
        positions_df
        .select("portfolio_id")
        .distinct()
    )

    dim_portfolio_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/opt/data/gold/dim_portfolio")

    print("dim_portfolio created")


if __name__ == "__main__":
    run()
from common.spark_session import get_spark


def run():

    spark = get_spark()

    attribution_df = spark.read.format("delta").load(
        "/opt/data/silver/performance_attribution"
    )

    attribution_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(
            "/opt/data/gold/fact_performance_attribution"
        )

    print("fact_performance_attribution created")


if __name__ == "__main__":
    run()
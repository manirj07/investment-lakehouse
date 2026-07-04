from common.spark_session import get_spark


def run():

    spark = get_spark()

    df = spark.read.option("header", "true").csv(
        "/opt/data/source/esg_scores.csv"
    )

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/opt/data/bronze/esg_scores")

    print("Bronze ESG scores loaded")


if __name__ == "__main__":
    run()
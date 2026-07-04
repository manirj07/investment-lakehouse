from common.spark_session import get_spark

def run():

    spark = get_spark()

    df = spark.read.option("header", "true").csv(
        "/opt/data/source/security_master.csv"
    )

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/opt/data/bronze/security_master")

    print("Bronze security master loaded")


if __name__ == "__main__":
    run()
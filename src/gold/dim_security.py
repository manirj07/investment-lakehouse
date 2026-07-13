from common.spark_session import get_spark

def run():
    spark = get_spark()

    security_df = spark.read.format("delta").load(
        "/opt/data/bronze/security_master"
    )

    dim_security_df = (
        security_df
        .dropDuplicates(["ticker"])
    )

    dim_security_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/opt/data/gold/dim_security")

    print("dim_security created")


if __name__ == "__main__":
    run()
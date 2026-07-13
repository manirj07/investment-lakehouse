from common.spark_session import get_spark

spark = get_spark()

print(
    spark.sparkContext.getConf().get(
        "spark.jars.packages",
        "NOT_FOUND"
    )
)

spark.stop()
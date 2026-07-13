from common.spark_session import get_spark

spark = get_spark()

df = spark.createDataFrame(
    [(1, "test")],
    ["id", "name"]
)

(
    df.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/airflow")
    .option("dbtable", "reporting.jdbc_test")
    .option("user", "airflow")
    .option("password", "airflow")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

print("SUCCESS")
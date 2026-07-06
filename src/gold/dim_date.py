from common.spark_session import get_spark
from pyspark.sql.functions import (
    year,
    month,
    quarter,
    dayofmonth,
    date_format
)


def run():

    spark = get_spark()

    daily_df = spark.read.format("delta").load(
        "/opt/data/silver/portfolio_daily_performance"
    )

    dim_date_df = (
        daily_df
        .select("date")
        .distinct()
        .withColumn("year", year("date"))
        .withColumn("month", month("date"))
        .withColumn("quarter", quarter("date"))
        .withColumn("day", dayofmonth("date"))
        .withColumn(
            "day_name",
            date_format("date", "EEEE")
        )
    )

    dim_date_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/opt/data/gold/dim_date")

    print("dim_date created")


if __name__ == "__main__":
    run()
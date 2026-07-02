from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark():

    builder = (
        SparkSession.builder
        .appName("InvestmentLakehouse")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark
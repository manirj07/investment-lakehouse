from common.spark_session import get_spark
from pyspark.sql.functions import sum, col, round

def run():
    spark = get_spark()

    weights_df = spark.read.format("delta").load(
        "/opt/data/silver/portfolio_weights"
    )

    security_df = spark.read.format("delta").load(
        "/opt/data/bronze/security_master"
    )

    country_df = (
        weights_df
        .join(security_df, "ticker")
        .groupBy("portfolio_id", "country")
        .agg(
            sum("market_value").alias("country_market_value")
        )
    )

    total_df = (
        weights_df
        .groupBy("portfolio_id")
        .agg(
            sum("market_value").alias("portfolio_value")
        )
    )

    result_df = (
        country_df
        .join(total_df, "portfolio_id")
        .withColumn(
            "country_weight_pct",
            round(
                col("country_market_value")
                / col("portfolio_value") * 100,
                2
            )
        )
    )

    result_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/opt/data/silver/country_exposure")

if __name__ == "__main__":
    run()
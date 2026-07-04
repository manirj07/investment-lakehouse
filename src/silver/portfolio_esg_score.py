from common.spark_session import get_spark
from pyspark.sql.functions import col, sum, round

def run():

    spark = get_spark()

    weights_df = spark.read.format("delta").load(
        "/opt/data/silver/portfolio_weights"
    )

    esg_df = spark.read.format("delta").load(
        "/opt/data/bronze/esg_scores"
    )

    result_df = (
        weights_df
        .join(esg_df, "ticker")
        .withColumn(
            "weighted_score",
            col("weight_pct") * col("esg_score")
        )
        .groupBy("portfolio_id")
        .agg(
            round(
                sum("weighted_score") / 100,
                2
            ).alias("portfolio_esg_score")
        )
    )

    result_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/opt/data/silver/portfolio_esg_score")

if __name__ == "__main__":
    run()
from common.spark_session import get_spark

spark = get_spark()
df = spark.read.format("delta").load(
    "/opt/data/silver/portfolio_daily_performance"
)

df.orderBy("date").show(20, False)
def run():
    print("Running esg_scores_ingestion")

# Databricks notebook source
esg_data = [
    ("AAPL", 80, 75, 85),
    ("MSFT", 85, 90, 88),
    ("NVDA", 78, 80, 82),
    ("SAP", 90, 88, 92),
    ("ALV.DE", 87, 85, 90)
]

columns = [
    "ticker",
    "environmental_score",
    "social_score",
    "governance_score"
]

esg_df = spark.createDataFrame(
    esg_data,
    columns
)

# COMMAND ----------

from pyspark.sql import functions as F

esg_df = (
    esg_df
    .withColumn(
        "esg_score",
        F.round(
            (
                F.col("environmental_score")
                + F.col("social_score")
                + F.col("governance_score")
            ) / 3,
            2
        )
    )
)

# COMMAND ----------

esg_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adb_investment_platform_dev.investment_bronze.esg_scores"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adb_investment_platform_dev.investment_bronze.esg_scores;

# COMMAND ----------


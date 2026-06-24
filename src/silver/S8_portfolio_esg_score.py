# Databricks notebook source
weights_df = spark.table(
    "adb_investment_platform_dev.investment_silver.portfolio_weights"
)

esg_df = spark.table(
    "adb_investment_platform_dev.investment_bronze.esg_scores"
)

# COMMAND ----------

portfolio_esg_df = (
    weights_df.alias("w")
    .join(
        esg_df.alias("e"),
        on="ticker",
        how="inner"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate Weighted ESG Contribution

# COMMAND ----------

from pyspark.sql import functions as F

portfolio_esg_df = (
    portfolio_esg_df
    .withColumn(
        "weighted_esg",
        F.round(
            (
                F.col("weight_pct")
                * F.col("esg_score")
            ) / 100,
            4
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Portfolio ESG Score

# COMMAND ----------

portfolio_esg_score_df = (
    portfolio_esg_df
    .groupBy("portfolio_id")
    .agg(
        F.round(
            F.sum("weighted_esg"),
            2
        ).alias("portfolio_esg_score")
    )
)

# COMMAND ----------

portfolio_esg_score_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adb_investment_platform_dev.investment_silver.portfolio_esg_score"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adb_investment_platform_dev.investment_silver.portfolio_esg_score;

# COMMAND ----------


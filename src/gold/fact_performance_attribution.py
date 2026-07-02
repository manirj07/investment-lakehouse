def run():
    print("Running fact_performance_attribution")

# Databricks notebook source
performance_attribution_df = spark.table(
    "adb_investment_platform_dev.investment_silver.performance_attribution"
)

performance_attribution_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adb_investment_platform_dev.investment_gold.fact_performance_attribution"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adb_investment_platform_dev.investment_gold.fact_performance_attribution
# MAGIC LIMIT 10;

# COMMAND ----------


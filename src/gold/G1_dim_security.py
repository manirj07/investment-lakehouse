# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

security_df = spark.table("adb_investment_platform_dev.investment_bronze.security_master")

window_spec = Window.orderBy("ticker")

dim_security_df = (security_df.withColumn("security_key", F.row_number().over(window_spec))
                   .select(
                       "security_key",
                        "ticker",
                        "security_name",
                        "sector",
                        "country",
                        "currency"
                   )
                  )


# COMMAND ----------

dim_security_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("adb_investment_platform_dev.investment_gold.dim_security")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adb_investment_platform_dev.investment_gold.dim_security;

# COMMAND ----------


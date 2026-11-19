# Databricks notebook source
# Databricks notebook source
# COMMAND ----------
# DBTITLE 1,Domain Splitting & Full Name Concatenation
from pyspark.sql.functions import col, concat_ws, element_at, split
storage_account = "05storageac"
silver_customers_path = (
f"abfss://silver@{storage_account}.dfs.core.windows.net/customers"
)
df_customers = spark.read.format("parquet").load(
f"abfss://bronze@{storage_account}.dfs.core.windows.net/customers"
).drop("_rescued_data")


# Extract domain using PySpark array indexing
df_enriched = df_customers.withColumn(
"email_domain", element_at(split(col("email"), "@"), 2)
).withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))


# Write to Silver Delta Lake & Register Managed Catalog Table
df_enriched.write.format("delta").mode("overwrite").save(silver_customers_path)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS 05_catalog.silver.customer_silver
USING DELTA
LOCATION '{silver_customers_path}'
""")

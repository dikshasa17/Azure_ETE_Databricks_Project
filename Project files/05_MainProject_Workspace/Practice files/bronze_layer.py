# Databricks notebook source
# Databricks notebook source
# COMMAND ----------
# DBTITLE 1,Dynamic Parameterization Setup
from databricks.sdk.runtime import dbutils
# Widget parameter for incoming target folder/file
dbutils.widgets.text("file_name", "", "Input File Name")
p_file_name = dbutils.widgets.get("file_name")
storage_account = "05storageac"
source_path = f"abfss://source@{storage_account}.dfs.core.windows.net/{p_file_name}"
bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/{p_file_name}"
checkpoint_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/checkpoint_{p_file_name}"




# COMMAND ----------

# COMMAND ----------
# DBTITLE 2,Auto Loader PySpark Structured Streaming Read
from pyspark.sql.functions import col
# Read stream using cloudFiles Auto Loader
df_stream = (
spark.readStream.format("cloudfiles")
.option("cloudfiles.format", "parquet")
.option("cloudfiles.schemaLocation", checkpoint_path)
.load(source_path)
)

# COMMAND ----------

# COMMAND ----------
# DBTITLE 3,Idempotent Stream Write with RocksDB State Tracking
# Write stream incrementally using availableNow=True (2026 modern trigger)
query = (
df_stream.writeStream.format("parquet")
.outputMode("append")
.option("checkpointLocation", checkpoint_path)
.option("path", bronze_path)
.trigger(
availableNow=True
) # Replaces legacy trigger(once=True) for modern micro-batches
.start()
)
query.awaitTermination()

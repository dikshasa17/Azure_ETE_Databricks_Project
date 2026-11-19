# Databricks notebook source
df = spark.read.table("05_catalog.bronze.regions")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df.write.format("delta")\
            .mode("overwrite")\
            .save("abfss://silver@05storageac.dfs.core.windows.net/regions")

# COMMAND ----------

df = spark.read.format("delta")\
            .load("abfss://silver@05storageac.dfs.core.windows.net/products")

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS 05_catalog.silver.regions_silver 
# MAGIC USING DELTA 
# MAGIC LOCATION 'abfss://silver@05storageac.dfs.core.windows.net/regions'

# COMMAND ----------


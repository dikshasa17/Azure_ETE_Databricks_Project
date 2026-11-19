# Databricks notebook source
# Read managed bronze table directly via spark.read.table
df_regions = spark.read.table("05_catalog.bronze.regions").drop(
"_rescued_data"
)
storage_account = "05storageac"
silver_regions_path = (
f"abfss://silver@{storage_account}.dfs.core.windows.net/regions"
)
df_regions.write.format("delta").mode("overwrite").save(silver_regions_path)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS 05_catalog.silver.region_silver
USING DELTA
LOCATION '{silver_regions_path}'
""")

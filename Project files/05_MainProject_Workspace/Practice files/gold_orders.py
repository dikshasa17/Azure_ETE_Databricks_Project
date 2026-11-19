# Databricks notebook source
# Databricks notebook source
# COMMAND ----------
# DBTITLE 1, Import Delta Table & PySpark Libraries
from delta.tables import DeltaTable
from pyspark.sql.functions import col

# COMMAND ----------
# DBTITLE 2, Read Silver Orders & Active Gold Dimension Tables
# Read Silver Orders
df_orders = spark.read.table("05_catalog.silver.order_silver")

# Read Active Gold Customer Dimension (SCD Type 1)
df_dim_customers = spark.read.table("05_catalog.gold.dim_customers") \
    .select("dim_customer_key", col("customer_id").alias("dim_customer_id"))

# Read Active Gold Product Dimension (SCD Type 2 active records where __end_at IS NULL)
df_dim_products = spark.read.table("05_catalog.gold.dim_products") \
    .filter(col("__end_at").isNull()) \
    .select("product_id", col("product_id").alias("dim_product_id"))

# COMMAND ----------
# DBTITLE 3, Perform Surrogate Key Lookups via Left Outer Joins
df_fact = df_orders \
    .join(df_dim_customers, df_orders.customer_id == df_dim_customers.dim_customer_id, "left") \
    .join(df_dim_products, df_orders.product_id == df_dim_products.dim_product_id, "left")

# COMMAND ----------
# DBTITLE 4, Column Cleanup (Drop Natural Business Keys)
# Fact tables store surrogate keys & numerical metrics, dropping natural string/ID columns
df_fact_clean = df_fact.drop("customer_id", "product_id", "dim_customer_id", "dim_product_id")

# COMMAND ----------
# DBTITLE 5, Idempotent Delta Lake MERGE / Upsert into Gold Layer
target_table_name = "05_catalog.gold.fact_orders"

if not spark.catalog.tableExists(target_table_name):
    # Initial Load: Register and write managed Delta table in Gold schema
    df_fact_clean.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(target_table_name)
else:
    # Incremental Load: Perform Delta Lake Merge (Upsert)
    target_table = DeltaTable.forName(spark, target_table_name)
    
    target_table.alias("trg") \
        .merge(
            df_fact_clean.alias("src"),
            "trg.order_id = src.order_id AND trg.dim_customer_key = src.dim_customer_key"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
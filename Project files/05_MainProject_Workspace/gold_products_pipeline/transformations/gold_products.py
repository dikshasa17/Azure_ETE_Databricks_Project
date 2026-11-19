# Databricks notebook source
# Databricks notebook source
# COMMAND ----------
# DBTITLE 1,Import Delta Live Tables Framework
import dlt
from pyspark.sql.functions import col
# COMMAND ----------
# DBTITLE 2,Staging Streaming Table with Expectations
my_rules = {
"rule_valid_product_id": "product_id IS NOT NULL",
"rule_valid_price": "price > 0",
}
@dlt.table(name="dim_products_stage", comment="Staging table for products")
@dlt.expect_all_or_drop(my_rules) # Drops records violating data quality rules
def dim_products_stage():
    return spark.readStream.table("05_catalog.silver.product_silver")
# COMMAND ----------
# DBTITLE 3,Create Empty Target Streaming Table
dlt.create_streaming_table(
name="dim_products",
comment="Gold Dimension Products Table with SCD Type 2 History",
)
# COMMAND ----------
# DBTITLE 4,Apply SCD Type 2 Changes Declaratively
dlt.apply_changes(
target="dim_products",
source="dim_products_stage",
keys=["product_id"],
sequence_by=col("product_id"), # Sequence key for state management
stored_as_scd_type=2, # Enables SCD Type 2 history tracking automatically
)

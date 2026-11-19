# Databricks notebook source
# Databricks notebook source
# COMMAND ----------
# DBTITLE 1,Create Persistent Unity Catalog Functions
# Create SQL Scalar Function in Catalog
spark.sql("""
CREATE OR REPLACE FUNCTION 05_catalog.bronze.discount_funk(p_price DOUBLE)
RETURNS DOUBLE
LANGUAGE SQL
COMMENT 'Applies a 10% discount to product price'
RETURN p_price * 0.90
""")


# Create Python Function in Catalog
spark.sql("""
CREATE OR REPLACE FUNCTION 05_catalog.bronze.upper_funk(p_brand STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
return p_brand.upper() if p_brand else None
$$
""")
# COMMAND ----------
# DBTITLE 2,Apply Catalog Functions via expr() in PySpark
from pyspark.sql.functions import expr
storage_account = "05storageac"
df_products = spark.read.format("parquet").load(
f"abfss://bronze@{storage_account}.dfs.core.windows.net/products"
).drop("_rescued_data")
# Invoke Unity Catalog functions inside PySpark using expr()
df_silver_products = df_products.withColumn(
"discounted_price", expr("05_catalog.bronze.discount_funk(price)")
).withColumn("brand_upper", expr("05_catalog.bronze.upper_funk(brand)"))
silver_products_path = (
f"abfss://silver@{storage_account}.dfs.core.windows.net/products"
)
df_silver_products.write.format("delta").mode("overwrite").save(
silver_products_path
)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS 05_catalog.silver.product_silver
USING DELTA
LOCATION '{silver_products_path}'
""")

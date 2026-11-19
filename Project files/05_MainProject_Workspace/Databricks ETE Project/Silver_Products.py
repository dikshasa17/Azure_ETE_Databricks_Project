# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import * 

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet")\
          .load("abfss://bronze@05storageac.dfs.core.windows.net/products")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Functions**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION 05_catalog.bronze.discount_func(p_price DOUBLE) 
# MAGIC RETURNS DOUBLE  
# MAGIC LANGUAGE SQL 
# MAGIC RETURN p_price * 0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, price ,05_catalog.bronze.discount_func(price) as discounted_price
# MAGIC FROM products

# COMMAND ----------

df = df.withColumn("discounted_price",expr("05_catalog.bronze.discount_func(price)"))
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION 05_catalog.bronze.upper_func(p_brand STRING)
# MAGIC RETURNS STRING 
# MAGIC LANGUAGE PYTHON 
# MAGIC AS 
# MAGIC $$ 
# MAGIC     return p_brand.upper()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_id, brand, 05_catalog.bronze.upper_func(brand) as brand_upper
# MAGIC FROM products

# COMMAND ----------

df.write.format("delta")\
            .mode("overwrite")\
            .option("path","abfss://silver@05storageac.dfs.core.windows.net/products")\
            .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS 05_catalog.silver.products_silver 
# MAGIC USING DELTA 
# MAGIC LOCATION 'abfss://silver@05storageac.dfs.core.windows.net/products'

# COMMAND ----------

# DBTITLE 1,Create table function for orders by year
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION 05_catalog.bronze.tablefunc(p_year DOUBLE)
# MAGIC RETURNS TABLE(order_id STRING, year DOUBLE)
# MAGIC LANGUAGE SQL
# MAGIC RETURN 
# MAGIC ( SELECT order_id, year FROM 05_catalog.silver.orders_silver
# MAGIC   WHERE year = p_year )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 05_catalog.bronze.tablefunc(2024)

# COMMAND ----------


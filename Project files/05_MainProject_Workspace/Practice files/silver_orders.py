# Databricks notebook source
# Databricks notebook source
# COMMAND ----------
# DBTITLE 1,Data Reading & Column Cleanup
from pyspark.sql.functions import col, dense_rank, rank, row_number, to_timestamp, year
from pyspark.sql.window import Window


storage_account = "05storageac"
bronze_orders_path = (
f"abfss://bronze@{storage_account}.dfs.core.windows.net/orders"
)
silver_orders_path = (
f"abfss://silver@{storage_account}.dfs.core.windows.net/orders"
)
# Read Bronze Parquet data
df_bronze_orders = spark.read.format("parquet").load(bronze_orders_path)


# Drop raw metadata column
df_orders = df_bronze_orders.drop("_rescued_data")


# COMMAND ----------
# DBTITLE 2,Date Transformations
df_transformed = df_orders.withColumn(
"order_date", to_timestamp(col("order_date"))
).withColumn("year", year(col("order_date")))


# COMMAND ----------
# DBTITLE 3,Object-Oriented PySpark Class for Window Functions
class SparkWindowTransformers:
    # Reusable Python class encapsulating PySpark Window operations
    def __init__(self, df):
        self.df = df


    def add_dense_rank(self, partition_col, order_col, target_col_name):
        window_spec = Window.partitionBy(partition_col).orderBy(
            col(order_col).desc()
        )
        return self.df.withColumn(target_col_name, dense_rank().over(window_spec))


    def add_rank(self, partition_col, order_col, target_col_name):
        window_spec = Window.partitionBy(partition_col).orderBy(
            col(order_col).desc()
        )
        return self.df.withColumn(target_col_name, rank().over(window_spec))


    def add_row_number(self, partition_col, order_col, target_col_name):
        window_spec = Window.partitionBy(partition_col).orderBy(
            col(order_col).desc()
        )
        return self.df.withColumn(target_col_name, row_number().over(window_spec))


# Instantiate class and invoke ranking method
window_ops = SparkWindowTransformers(df_transformed)
df_ranked = window_ops.add_dense_rank(
partition_col="year",
order_col="total_amount",
target_col_name="dense_rank_flag",
)


# COMMAND ----------
# DBTITLE 4,Write Delta Table & Register Catalog Entity
df_transformed.write.format("delta").mode("overwrite").save(silver_orders_path)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS 05_catalog.silver.order_silver
USING DELTA
LOCATION '{silver_orders_path}'
""")


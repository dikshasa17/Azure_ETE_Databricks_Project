# Databricks notebook source
# Databricks notebook source
# COMMAND ----------
# DBTITLE 1,Initialization & Parameter Handling
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, lit, max as spark_max, monotonically_increasing_id


dbutils.widgets.text("init_load_flag", "0", "Init Load Flag (1=Init, 0=Incr)")
init_load_flag = int(dbutils.widgets.get("init_load_flag"))
silver_table = "05_catalog.silver.customer_silver"
gold_target_path = (
"abfss://gold@05storageac.dfs.core.windows.net/dim_customers"
)
gold_table_name = "05_catalog.gold.dim_customers"
df_silver = spark.read.table(silver_table).dropDuplicates(["customer_id"])


# COMMAND ----------
# DBTITLE 2,SCD1 PySpark Upsert Engine
table_exists = spark.catalog.tableExists(gold_table_name)
if not table_exists or init_load_flag == 1:


    # INITIAL LOAD PATH: Assign surrogate keys starting from 1
    df_gold_init = (
        df_silver.withColumn(
            "dim_customer_key", monotonically_increasing_id() + 1
        )
        .withColumn("create_date", current_timestamp())
        .withColumn("update_date", current_timestamp())
    )
    df_gold_init.write.format("delta").mode("overwrite").option(
        "path", gold_target_path
    ).saveAsTable(gold_table_name)
else:


    # INCREMENTAL LOAD PATH (UPSERT)
    df_existing_gold = spark.read.table(gold_table_name)
    # Fetch Max Existing Surrogate Key
    max_key_val = (
        df_existing_gold.select(
            spark_max("dim_customer_key").alias("max_key")
        ).collect()[0]["max_key"]
        or 0
    )


    # Join silver source with existing gold dimension
    df_joined = df_silver.join(
        df_existing_gold.select(
            col("customer_id").alias("target_cid"),
            col("dim_customer_key").alias("target_key"),
            col("create_date").alias("target_create_date"),
        ),
        df_silver.customer_id == col("target_cid"),
        "left",
    )


    # Separate New Records (Not in Target)
    df_new_records = (
        df_joined.filter(col("target_cid").isNull())
        .drop("target_cid", "target_key", "target_create_date")
        .withColumn(
            "dim_customer_key", monotonically_increasing_id() + 1 + max_key_val
        )
        .withColumn("create_date", current_timestamp())
        .withColumn("update_date", current_timestamp())
    )


    # Separate Existing Records (Update fields, keep create_date)
    df_existing_records = (
        df_joined.filter(col("target_cid").isNotNull())
        .withColumn("dim_customer_key", col("target_key"))
        .withColumn("create_date", col("target_create_date"))
        .withColumn("update_date", current_timestamp())
        .drop("target_cid", "target_key", "target_create_date")
    )




    # Union New and Updated Records
    df_upsert_payload = df_new_records.unionByName(df_existing_records)
    # Perform Delta Lake Merge (SCD Type 1 Upsert)
    target_delta_table = DeltaTable.forName(spark, gold_table_name)
    (
        target_delta_table.alias("target")
        .merge(
            df_upsert_payload.alias("source"),
            "target.dim_customer_key = source.dim_customer_key",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


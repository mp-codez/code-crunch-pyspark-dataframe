# Databricks notebook source
dbutils.widgets.text('src_base_dir', '', label='Enter Source Base Dir')
src_base_dir = dbutils.widgets.get('src_base_dir')
dbutils.widgets.text('bronze_base_dir', '', label='Enter Target Base Dir')
bronze_base_dir = dbutils.widgets.get('bronze_base_dir')
dbutils.widgets.text('ds', '', label='Enter Dataset Name')
ds = dbutils.widgets.get('ds')

# COMMAND ----------

import json
def get_columns(schemas_file, ds_name):
    schemas_text = spark.read.text(schemas_file, wholetext=True).first().value
    schemas = json.loads(schemas_text)
    column_details = schemas[ds_name]
    columns = [col['column_name'] for col in sorted(column_details, key=lambda col: col['column_position'])]
    return columns

# COMMAND ----------

print(f'Processing {ds} data')
columns = get_columns(f'dbfs:{src_base_dir}/schemas.json', ds)  
df = spark.\
        read.\
            csv(f'{src_base_dir}/{ds}', inferSchema=True).\
                toDF(*columns)
df.write.\
    mode('overwrite').\
        parquet(f'{bronze_base_dir}/{ds}')

# COMMAND ----------

# MAGIC %fs ls dbfs:/public/retail_db_bronze/orders

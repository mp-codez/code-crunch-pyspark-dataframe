# Databricks notebook source
# MAGIC %fs ls dbfs:/public/retail_db/schemas.json

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from CSV.`dbfs:/public/retail_db/orders`

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from TEXT.`dbfs:/public/retail_db/schemas.json`

# COMMAND ----------

spark.read.text('dbfs:/public/retail_db/schemas.json', wholetext=True).show(truncate=False)

# COMMAND ----------

schemas_text=spark.read.text('dbfs:/public/retail_db/schemas.json', wholetext=True).first().value

# COMMAND ----------

schemas_text

# COMMAND ----------

import json
json.loads(schemas_text).keys()

# COMMAND ----------

json.loads(schemas_text)['orders']

# COMMAND ----------

column_details = json.loads(schemas_text)['orders']

# COMMAND ----------

sorted(column_details, key=lambda col:col['column_position'])

# COMMAND ----------

columns = [col['column_name'] for col in sorted(column_details, key=lambda col:col['column_position'])]
columns

# COMMAND ----------

df_orders = spark.read.csv('dbfs:/public/retail_db/orders')
df_orders

# COMMAND ----------

df_orders = spark.read.csv('dbfs:/public/retail_db/orders', inferSchema=True)
df_orders

# COMMAND ----------

df_orders = spark.read.csv('dbfs:/public/retail_db/orders', inferSchema=True).toDF(*columns)
df_orders

# COMMAND ----------

df_orders = spark.read.csv('dbfs:/public/retail_db/orders', inferSchema=True).toDF(*columns)
display(df_orders)

# COMMAND ----------

import json
def get_columns(schemas_file, ds_name):
    schema_text = spark.read.text(schemas_file, wholetext=True).first().value
    schemas = json.loads(schema_text)
    column_details = schemas[ds_name]
    columns = [col['column_name'] for col in sorted(column_details, key=lambda col:col['column_position'])]
    return columns

# COMMAND ----------

ds_list = [
    'departments', 'categories', 'products', 'customers', 'orders', 'order_items'
]
base_dir = 'dbfs:/public/retail_db'
tgt_base_dir = 'dbfs:/public/retail_db_parquet'

# COMMAND ----------

for ds in ds_list:
    print(f'Processing {ds} data')
    columns = get_columns(f'{base_dir}/schemas.json', ds)
    df = spark.read.csv(f'{base_dir}/{ds}', inferSchema = True).toDF(*columns)
    df.\
        write.\
        mode('overwrite').\
        parquet(f'{tgt_base_dir}/{ds}')

# COMMAND ----------

# MAGIC
# MAGIC %fs ls dbfs:/public/retail_db_parquet/order_items

# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/airlines    #total size:120 GB

# COMMAND ----------

# Pyspark Dataframes approach
airlines_schema = spark.\
    read.\
    csv(
        'dbfs:/databricks-datasets/airlines/part-00000',
        header=True,
        inferSchema=True
    ).\
    schema

# COMMAND ----------

airlines_df = spark.\
    read.\
    csv(
        'dbfs:/databricks-datasets/airlines/part-*',
        header=True,
        schema=airlines_schema
    )

# COMMAND ----------

airlines_df.\
    write.\
    partitionBy('Year', 'Month').\
    mode('overwrite').\
    parquet('dbfs:/FileStore/airlins')

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/airlins

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/airlins/Year=1989

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/airlins/Year=1989/Month=6/

# COMMAND ----------

files=[]
for f in dbutils.fs.ls('dbfs:/FileStore/airlins'):
    if f.name.startswith('Year='):
        for yf in dbutils.fs.ls(f.path):
            if yf.name.startswith('Month='):
                for mf in dbutils.fs.ls(yf.path):
                    if mf.name.endswith('snappy.parquet'):
                        files.append((mf.name, mf.size))

# COMMAND ----------

size = sum([f[1] for f in files])/(1024*1024*1024)

# COMMAND ----------

size

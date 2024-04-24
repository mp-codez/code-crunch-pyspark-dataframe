# Databricks notebook source
# MAGIC %sql
# MAGIC select *from csv.`dbfs:/databricks-datasets/airlines/part-00000`

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view airlines_v
# MAGIC using csv
# MAGIC options(
# MAGIC   path='dbfs:/databricks-datasets/airlines/part-00000',
# MAGIC   header=True,
# MAGIC   inferSchema=True
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from airlines_v;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from airlines_v;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct origin from airlines_v;

# COMMAND ----------

#With inferSchema
airlines_df = spark.read.csv('dbfs:/databricks-datasets/airlines/part-*', header=True, inferSchema=True)

# COMMAND ----------

airlines_df.count()

# COMMAND ----------

airlines_schema = spark.\
    read.\
    csv('dbfs:/databricks-datasets/airlines/part-00000', header=True, inferSchema=True).schema

# COMMAND ----------

airlines_schema

# COMMAND ----------

#With inferSchema
airlines_df = spark.read.csv('dbfs:/databricks-datasets/airlines/part-*', header=True, schema=airlines_schema)

# COMMAND ----------

airlines_df.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC #####Performance issues dealing with uncompressed CSV files without partitioning strategy
# MAGIC * Full table scan for all the operations (even for count)
# MAGIC * Higher amount of storage consumed as files are not compressed.
# MAGIC * Implementing archival strategy will become challenging.
# MAGIC
# MAGIC #####Review following details using Spark UI for below operations.
# MAGIC * SQL Plans
# MAGIC * Number of Jobs, stages with job and tasks with in each stage for each of the below operation.
# MAGIC * Total Exceution time (CPU Time)
# MAGIC * I/O Statistics

# COMMAND ----------

from pyspark.sql.functions import count, concat_ws

# COMMAND ----------

flights_by_month = airlines_df.\
    groupBy(concat_ws('-', 'Year', 'Month').alias('FlightMonth')).\
        agg(count('*').alias('FlightCount'))

# COMMAND ----------

flights_by_month.show()

# COMMAND ----------



# COMMAND ----------



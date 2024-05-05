# Databricks notebook source
airlines_df = spark.read.parquet('dbfs:/FileStore/airlins')

# COMMAND ----------

airlines_df.count()


# COMMAND ----------

from pyspark.sql.functions import count, concat_ws

# COMMAND ----------

# Count by Month
airlines_df.\
    groupBy(concat_ws('-', 'Year', 'Month').alias('FlightMonth')).\
    agg(count('*').alias('FlightCount')).\
    orderBy('FlightMonth').\
    show()                  

# COMMAND ----------

# MAGIC %md
# MAGIC #####Here are the reasons count on compressed parquet files ran significantly faster than count on uncompresses csv files.
# MAGIC * Compresses files takes lesser storage and hence I/O will be lesser.
# MAGIC * As parquet is columnar file format, the entire file content will not be read. Only relevant blocks with one of the not null column will be read.

# COMMAND ----------

airlines_df.\
    filter('Year = 2008').\
    groupBy(concat_ws('-', 'Year', 'Month').alias('FlightMonth')).\
    agg(count('*').alias('FlightCount')).\
    orderBy('FlightMonth').\
    show() 

# COMMAND ----------

# MAGIC %md
# MAGIC * airlins_df.filter("Year = '2008'").count()

# COMMAND ----------

# MAGIC %md 
# MAGIC #####Here are the reasons count on particular year ran significantly faster than count on uncompressses CSV files.
# MAGIC * As `Year` is partitioning key, only files related to 2008 files will be read.
# MAGIC * We have 25 Years of airlines data and the above count is only getting data related to one of the 25 years. Only 4% of the data will be read when compared to unpartioning files.

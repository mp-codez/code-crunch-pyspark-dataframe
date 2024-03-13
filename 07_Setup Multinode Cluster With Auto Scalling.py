# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/airlines

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.ls('dbfs:/databricks-datasets/airlines')

# COMMAND ----------

dbutils.fs.ls('dbfs:/databricks-datasets/airlines')[0].name

# COMMAND ----------

dbutils.fs.ls('dbfs:/databricks-datasets/airlines')[0].size

# COMMAND ----------

files = []

# COMMAND ----------

for f in dbutils.fs.ls('dbfs:/databricks-datasets/airlines'):
    if f.name.startswith('part-'):
        files.append((f.name, f.size))

# COMMAND ----------

files[:10]

# COMMAND ----------

# MAGIC %md
# MAGIC ####Ideal Clustor configuration with Performance tunning of cluster using Auto scaling.
# MAGIC * Frequency : Daily 
# MAGIC * SLA: 10 Mint
# MAGIC * Size: minimum 120 GB to Maximum 500 GB
# MAGIC * Cluster Configuration (Maximum capacity):
# MAGIC   * 16 Nodes (Each 32 GB RAM and 8 vCPUs)
# MAGIC   * Memory : 512 GB (16*32)
# MAGIC   * vCPUs  : 128 vCPUs (16*8)
# MAGIC * Cluster Configuration (Auto Scaling Capacity):
# MAGIC   * 4 Nodes to 16 Nodes (Each 32 GB RAM and 8 vCPUs)
# MAGIC   * Memory : 128 GB to 512 GB
# MAGIC   * vCPUs  : 32 vCPUs to 128 vCPUs
# MAGIC

# COMMAND ----------



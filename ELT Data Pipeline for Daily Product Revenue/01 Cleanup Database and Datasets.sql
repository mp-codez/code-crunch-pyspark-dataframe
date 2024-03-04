-- Databricks notebook source
DROP DATABASE IF EXISTS alquadri_retail_bronze CASCADE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm(dbutils.widgets.get('bronze_base_dir'), recurse=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm(dbutils.widgets.get('gold_base_dir'), recurse=True)

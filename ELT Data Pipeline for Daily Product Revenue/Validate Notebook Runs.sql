-- Databricks notebook source
-- MAGIC %run "./01 Cleanup Database and Datasets" $bronze_base_dir=/public/retail_db_bronze $gold_base_dir=/public/retail_db_gold

-- COMMAND ----------

-- MAGIC %run "./02 File Format Converter" $ds=orders $src_base_dir=/public/retail_db $bronze_base_dir=/public/retail_db_bronze

-- COMMAND ----------

-- MAGIC %run "./02 File Format Converter" $ds=order_items $src_base_dir=/public/retail_db $bronze_base_dir=/public/retail_db_bronze

-- COMMAND ----------

-- MAGIC %run "./03 Create Spark SQL Tables"  $table_name=orders  $bronze_base_dir=/public/retail_db_bronze

-- COMMAND ----------

use alquadri_retail_bronze

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %run "./04 Daily Product Revenue" $gold_base_dir=/public/retail_db_gold

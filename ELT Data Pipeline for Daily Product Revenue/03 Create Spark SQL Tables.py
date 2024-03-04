# Databricks notebook source
dbutils.widgets.text('bronze_base_dir', '', label='Enter Target Base Dir')
bronze_base_dir = dbutils.widgets.get('bronze_base_dir')
dbutils.widgets.text('table_name', '', label='Enter table Name')
table_name = dbutils.widgets.get('table_name')

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists alquadri_retail_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC use alquadri_retail_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS orders(
# MAGIC   order_id INT,
# MAGIC   order_date STRING,
# MAGIC   order_customer_id INT,
# MAGIC   order_status STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into orders
# MAGIC select *from parquet.`${bronze_base_dir}/orders/`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS order_items(
# MAGIC   order_item_id INT,
# MAGIC   order_item_order_id INT,
# MAGIC   order_item_product_id INT,
# MAGIC   order_item_quantity INT,
# MAGIC   order_item_subtotal FLOAT,
# MAGIC   order_item_product_price FLOAT
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into order_items
# MAGIC select * from parquet.`${bronze_base_dir}/order_items/`

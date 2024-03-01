# Databricks notebook source
# MAGIC %fs ls dbfs:/public/retail_db

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA alquadri_retail_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database() AS current_database_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC use alquadri_retail_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select *from TEXT.`/public/retail_db/orders/`

# COMMAND ----------

# MAGIC %sql 
# MAGIC select *from TEXT.`/public/retail_db/order_items/`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE orders(
# MAGIC   order_id INT,
# MAGIC   order_date STRING,
# MAGIC   order_customer_id INT,
# MAGIC   order_status STRING
# MAGIC ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into orders
# MAGIC select *from CSV.`/public/retail_db/orders/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ORDERS;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

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
# MAGIC select * from csv.`/public/retail_db/order_items/`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from order_items;

# Databricks notebook source
# MAGIC %md
# MAGIC ####Here are the details about the Apache Spark Catalyst Optimizer
# MAGIC * Unresolved Logical Plan
# MAGIC * Logical Plan
# MAGIC * Optimized Logical Plan
# MAGIC * Physical Plan
# MAGIC * Selected Physical Plan based on Cost Model
# MAGIC * Execution using Selected Physical Plan
# MAGIC
# MAGIC Whether the data processing logic is implemented using Spark SQL or Data Frame APIs (using Python or scala), the code will be compiled into byte code using RDDs and executed. Due to this reason, the performance will be same irrespective of the approach chosen to implement data processing logic.
# MAGIC
# MAGIC
# MAGIC <img src="https://www.databricks.com/wp-content/uploads/2018/05/Catalyst-Optimizer-diagram.png" width="800" height="400">
# MAGIC

# COMMAND ----------

# Reading orders data from CSV files to Spark Data Frame
orders = spark.\
    read.\
        csv(
            '/public/retail_db/orders',
            schema='order_id INT, order_date DATE, order_customer_id INT, order_status STRING'
        )

# COMMAND ----------

# Reading order_items data from CSV files to Spark Data Frame
order_items = spark.\
    read.\
        csv(
            '/public/retail_db/order_items',
            schema='''
            order_item_id INT, order_item_order_id INT, order_item_product_id INT,
            order_item_quantity INT, order_item_subtotal FLOAT, order_item_product_price FLOAT
            '''
        )

# COMMAND ----------

from pyspark.sql.functions import col, sum, round

# COMMAND ----------

# Compute daily product revenue
daily_product_revenue = orders.\
    filter('''order_status in ('COMPLETE', 'CLOSED')''').\
        join(order_items, orders['order_id']==order_items['order_item_order_id']).\
            groupBy('order_date', 'order_item_product_id').\
            agg(round(sum('order_item_subtotal'), 2).alias('revenue'))

# COMMAND ----------

daily_product_revenue.display()

# COMMAND ----------

daily_product_revenue.\
    write.\
        mode('overwrite').\
            parquet('/public/retail_db/daily_product_revenue')

# COMMAND ----------

help(daily_product_revenue.explain)

# COMMAND ----------

daily_product_revenue.explain(True)

# COMMAND ----------

daily_product_revenue.explain(True)

# COMMAND ----------

daily_product_revenue.explain('simple')

# COMMAND ----------

daily_product_revenue.explain('extended')

# COMMAND ----------

daily_product_revenue.explain('codegen')

# COMMAND ----------

daily_product_revenue.explain('cost')

# COMMAND ----------

daily_product_revenue.explain('formatted')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists alquadri_retail_db cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists alquadri_retail_db

# COMMAND ----------

# MAGIC %sql
# MAGIC use alquadri_retail_db

# COMMAND ----------

# MAGIC %sql
# MAGIC create table orders(
# MAGIC   order_id INT,
# MAGIC   order_date DATE, 
# MAGIC   order_customer_id INT,
# MAGIC   order_status string
# MAGIC ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into alquadri_retail_db.orders
# MAGIC select * from csv.`/public/retail_db/orders`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table order_items(
# MAGIC   order_item_id INT,
# MAGIC   order_item_order_id INT,
# MAGIC   order_item_product_id INT,
# MAGIC   order_item_quantity INT,
# MAGIC   order_item_subtotal float,
# MAGIC   order_item_product_price float
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into order_items
# MAGIC select * from csv.`/public/retail_db/order_items`

# COMMAND ----------

# MAGIC %sql
# MAGIC insert overwrite directory '/public/retail_db/daily_product_revenue'
# MAGIC using parquet
# MAGIC select o.order_date,
# MAGIC   oi.order_item_product_id,
# MAGIC   round(sum(oi.order_item_subtotal), 2) as revenue
# MAGIC from orders as o
# MAGIC   join order_items as oi
# MAGIC     on o.order_id = oi.order_item_order_id
# MAGIC where o.order_status in ('COMPLETE', 'CLOSED')
# MAGIC group by 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC explain
# MAGIC select o.order_date,
# MAGIC   oi.order_item_product_id,
# MAGIC   round(sum(oi.order_item_subtotal), 2) as revenue
# MAGIC from orders as o
# MAGIC   join order_items as oi
# MAGIC     on o.order_id = oi.order_item_order_id
# MAGIC where o.order_status in ('COMPLETE', 'CLOSED')
# MAGIC group by 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC explain extended
# MAGIC select o.order_date,
# MAGIC   oi.order_item_product_id,
# MAGIC   round(sum(oi.order_item_subtotal), 2) as revenue
# MAGIC from orders as o
# MAGIC   join order_items as oi
# MAGIC     on o.order_id = oi.order_item_order_id
# MAGIC where o.order_status in ('COMPLETE', 'CLOSED')
# MAGIC group by 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC explain formatted
# MAGIC select o.order_date,
# MAGIC   oi.order_item_product_id,
# MAGIC   round(sum(oi.order_item_subtotal), 2) as revenue
# MAGIC from orders as o
# MAGIC   join order_items as oi
# MAGIC     on o.order_id = oi.order_item_order_id
# MAGIC where o.order_status in ('COMPLETE', 'CLOSED')
# MAGIC group by 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC explain cost
# MAGIC select o.order_date,
# MAGIC   oi.order_item_product_id,
# MAGIC   round(sum(oi.order_item_subtotal), 2) as revenue
# MAGIC from orders as o
# MAGIC   join order_items as oi
# MAGIC     on o.order_id = oi.order_item_order_id
# MAGIC where o.order_status in ('COMPLETE', 'CLOSED')
# MAGIC group by 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC explain cache
# MAGIC select o.order_date,
# MAGIC   oi.order_item_product_id,
# MAGIC   round(sum(oi.order_item_subtotal), 2) as revenue
# MAGIC from orders as o
# MAGIC   join order_items as oi
# MAGIC     on o.order_id = oi.order_item_order_id
# MAGIC where o.order_status in ('COMPLETE', 'CLOSED')
# MAGIC group by 1, 2

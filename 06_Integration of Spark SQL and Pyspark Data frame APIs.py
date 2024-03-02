# Databricks notebook source
# MAGIC %md  ###Integration of Spark SQL and Pyspark Data Frame APIs
# MAGIC * Run Spark SQL Queries on top of Data Frames
# MAGIC * Write Data Frame to Spark Metastore Table.
# MAGIC * Read Data from Spark Metastore Table to Spark Data Frame.
# MAGIC * Process Data in Spark Metastore Tables using Data Frame APIs.
# MAGIC * Manage Spark Metastore Databases using Spark APIs

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database alquadri_retail_db cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC create database alquadri_retail_db

# COMMAND ----------

# MAGIC %sql
# MAGIC use alquadri_retail_db;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %fs ls dbfs:/public/retail_db

# COMMAND ----------

orders = spark.read.csv(
    'dbfs:/public/retail_db/orders',
    schema='order_id INT, order_date DATE, order_customer_id INT, order_status STRING'
)

# COMMAND ----------

orders.createOrReplaceTempView('orders_v')

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

display(spark.sql('''
            select order_status, 
            count(*) as order_count
            from orders_v
            group by order_status
            order by 2 desc
         ''')
)
        

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_status, 
# MAGIC count(*) as order_count
# MAGIC from orders_v
# MAGIC group by order_status
# MAGIC order by 2 desc

# COMMAND ----------

orders = spark.read.csv(
    'dbfs:/public/retail_db/orders',
    schema='order_id INT, order_date DATE, order_customer_id INT, order_status STRING'
)

# COMMAND ----------

from pyspark.sql.functions import count, lit

# COMMAND ----------

order_count_by_status = orders.\
        groupBy('order_status').\
            agg(count(lit(1)).alias('order_count'))

# COMMAND ----------

display(order_count_by_status)

# COMMAND ----------

help(order_count_by_status.write.saveAsTable)

# COMMAND ----------

order_count_by_status.write.saveAsTable('alquadri_retail_db.order_count_by_status', format="delta", mode='append')

# COMMAND ----------

"""# Rename the column names to remove any invalid characters
new_column_names = [col.replace(' ', '_').replace(',', '') for col in order_count_by_status.columns]
order_count_by_status = order_count_by_status.toDF(*new_column_names)
display(order_count_by_status)
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted order_count_by_status

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from order_count_by_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table order_count_by_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

help(order_count_by_status.write.insertInto)

# COMMAND ----------

order_count_by_status.write.insertInto('alquadri_retail_db.order_count_by_status')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table alquadri_retail_db.order_count_by_status(
# MAGIC   order_status string,
# MAGIC   order_count int
# MAGIC ) using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

order_count_by_status.write.insertInto('alquadri_retail_db.order_count_by_status')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from alquadri_retail_db.order_count_by_status

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE alquadri_retail_db CASCADE;
# MAGIC create DATABASE alquadri_retail_db;
# MAGIC use alquadri_retail_db;
# MAGIC
# MAGIC create TABLE orders (
# MAGIC   order_id int,
# MAGIC   order_date date,
# MAGIC   order_customer_id int,
# MAGIC   order_status STRING
# MAGIC );
# MAGIC
# MAGIC CREATE or replace TEMPORARY VIEW orders_v(
# MAGIC   order_id INT,
# MAGIC   order_date DATE,
# MAGIC   order_customer_id INT,
# MAGIC   order_status STRING
# MAGIC ) USING CSV
# MAGIC OPTIONS(
# MAGIC   PATH='dbfs:/public/retail_db/orders'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into orders
# MAGIC select * from orders_v;
# MAGIC
# MAGIC create table order_items (
# MAGIC   order_item_id INT,
# MAGIC   order_item_order_id INT,
# MAGIC   order_item_product_id INT,
# MAGIC   order_item_quantity INT,
# MAGIC   order_item_subtotal FLOAT,
# MAGIC   order_item_product_price FLOAT 
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE temporary VIEW order_items_v (
# MAGIC   order_item_id INT,
# MAGIC   order_item_order_id INT,
# MAGIC   order_item_product_id INT,
# MAGIC   order_item_quantity INT,
# MAGIC   order_item_subtotal FLOAT,
# MAGIC   order_item_product_price FLOAT 
# MAGIC ) using csv
# MAGIC options(
# MAGIC   path = 'dbfs:/public/retail_db/order_items'
# MAGIC );
# MAGIC
# MAGIC insert into order_items
# MAGIC select * from order_items_v;

# COMMAND ----------

orders_df = spark.read.table('alquadri_retail_db.orders')

# COMMAND ----------

order_items_df = spark.read.table('alquadri_retail_db.order_items')

# COMMAND ----------

from pyspark.sql.functions import round, sum, col
order_revenue_eachday_perproduct = orders_df.\
    filter("order_status in ('COMPLETE','CLOSED')").\
    join(order_items_df, orders_df['order_id'] == order_items_df['order_item_order_id']).\
    groupBy(orders_df['order_date'], order_items_df['order_item_product_id']).\
    agg(round(sum(order_items_df['order_item_subtotal']), 2).alias('order_revenue')).\
    orderBy(orders_df['order_date'], col('order_revenue').desc())


# COMMAND ----------

order_revenue_eachday_perproduct.write.saveAsTable('alquadri_retail_db.order_revenue_eachday_perproduct', mode='overwrite')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from order_revenue_eachday_perproduct;

# COMMAND ----------

spark

# COMMAND ----------

spark.catalog.listTables('alquadri_retail_db')

# COMMAND ----------

for table in spark.catalog.listTables('alquadri_retail_db'):
    if table.tableType == "Temporary":
        print(table.name)
        spark.catalog.dropTempView(table.name)

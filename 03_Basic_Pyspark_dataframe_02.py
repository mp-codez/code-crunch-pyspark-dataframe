# Databricks notebook source
orders_df = spark.read.csv('dbfs:/public/retail_db/orders', schema='order_id INT, order_date DATE, order_customer_id INT, order_status STRING')
display(orders_df)

# COMMAND ----------

from pyspark.sql.functions import date_format, cast

# COMMAND ----------

orders_df.\
    withColumn('order_month', date_format('order_date', 'yyyyMM').cast('int'))

# COMMAND ----------

display(orders_df.\
    withColumn('order_month', date_format('order_date', 'yyyyMM').cast('int')))

# COMMAND ----------

display(orders_df.select('order_status').distinct())

# COMMAND ----------

display(orders_df.filter('order_status in ("COMPLETE", "CLOSED")'))

# COMMAND ----------

orders_df.filter('order_status in ("COMPLETE", "CLOSED")').count()

# COMMAND ----------

# MAGIC %md
# MAGIC * Get all the COMPLETE or CLOSED orders which are placed in the month of 2014, January

# COMMAND ----------

order_result = orders_df.filter('order_status in ("COMPLETE", "CLOSED") AND date_format(order_date, "yyyyMM") = 201401')

# COMMAND ----------

display(order_result)

# COMMAND ----------

# MAGIC %md
# MAGIC * Get the count by status

# COMMAND ----------

from pyspark.sql.functions import count, col

# COMMAND ----------

display(
    orders_df.\
        groupBy('order_status').\
        agg(count('order_id').alias('order_count')).\
        orderBy(col('order_count').desc())
        )

# COMMAND ----------

# MAGIC %md
# MAGIC * order revenue by status

# COMMAND ----------

order_items_df = spark.read.csv(
    'dbfs:/public/retail_db/order_items',
    schema='''
        order_item_id INT, order_item_order_id INT, order_item_product_id INT,
        order_item_quantity INT, order_item_subtotal FLOAT, order_item_product_price FLOAT
    '''
    )

# COMMAND ----------

display(order_items_df)

# COMMAND ----------

order_items_df

# COMMAND ----------

#SELECT order_item_order_id, sum(order_item_subtotal) as order_revenue from order_items group by #order_item_order_id #ORDER BY order_item_order_id
#agg() => count, sum, min, max, some other aggregate functions
from pyspark.sql.functions import col, sum, round

display(order_items_df.\
    withColumn('order_item_subtotal', col('order_item_subtotal').cast('float')).\
    groupBy('order_item_order_id').\
    agg(round(sum('order_item_subtotal'), 2).alias('order_revenue')).\
    orderBy('order_item_order_id')
)

# COMMAND ----------

# MAGIC %md
# MAGIC * order_count by order_date and order_month

# COMMAND ----------

from pyspark.sql.functions import count, date_format
display(orders_df.\
    groupBy('order_date').\
    agg(count('order_id').alias('order_count')).\
    orderBy('order_date')
)

# COMMAND ----------

display(orders_df.\
    groupBy(date_format('order_date', 'yyyyMM').alias('order_month')).\
    agg(count('order_id').alias('order_count')).\
    orderBy('order_month')
)

# COMMAND ----------

help(orders_df.sort)

# COMMAND ----------

# MAGIC %md
# MAGIC * Sorting the data based on one column (ascending or descending)
# MAGIC * Composite Sorting - sorting based on multiple columns
# MAGIC * Dealing with Nulls
# MAGIC * 

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

c = col('order_customer_id')

# COMMAND ----------

c

# COMMAND ----------

display(
    orders_df.\
        orderBy(col('order_customer_id').desc())
)

# COMMAND ----------

#composite sorting
display(
    orders_df.\
    orderBy('order_customer_id', col('order_date').desc())
)

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/online_retail/data-001

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from csv.`dbfs:/databricks-datasets/online_retail/data-001/data.csv`

# COMMAND ----------

df = spark.read.csv('dbfs:/databricks-datasets/online_retail/data-001/data.csv', header=True, inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.filter('StockCode = 71053'))

# COMMAND ----------

display(
    df.filter('StockCode = 71053').\
        orderBy('CustomerID')
)

# COMMAND ----------

from pyspark.sql.functions import col
display(
    df.filter('StockCode = 71053').\
        orderBy(col('CustomerID').asc_nulls_last())
)

# COMMAND ----------

display(
    df.filter('StockCode = 71053').\
        orderBy(col('CustomerID').desc())
)

# COMMAND ----------

display(
    df.filter('StockCode = 71053').\
        orderBy(col('CustomerID').desc_nulls_first())
)

# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC * Inner Joins
# MAGIC * Outer Joins - Left Outer Joins, Right Outer Joins, Full Outer Joins
# MAGIC * Cross Joins 

# COMMAND ----------

# MAGIC %fs ls dbfs:/public/retail_db

# COMMAND ----------

orders_df = spark.\
    read.\
    csv(
        'dbfs:/public/retail_db/orders',
        schema='order_id INT, order_date DATE, order_customer_id INT, order_status STRING'
    )

# COMMAND ----------

display(orders_df)

# COMMAND ----------

orders_df.count()

# COMMAND ----------

order_items_df = spark.\
    read.\
    csv(
        'dbfs:/public/retail_db/order_items',
        schema='''
            order_item_id INT, order_item_order_id INT, order_item_product_id INT,
            order_item_quantity INT, order_item_subtotal FLOAT, order_item_product_price FLOAT
        '''
    )

# COMMAND ----------

display(order_items_df)

# COMMAND ----------

order_details_df = orders_df.join(order_items_df, orders_df['order_id'] == order_items_df['order_item_order_id'])

# COMMAND ----------

display(order_details_df)

# COMMAND ----------

display(order_details_df.select('order_id', 'order_date', 'order_customer_id', 'order_item_subtotal'))

# COMMAND ----------

display(order_details_df.select(orders_df['*'], order_items_df['order_item_subtotal']))

# COMMAND ----------

# MAGIC %md
# MAGIC * COMPUTE DAILY REVENUE FOR ORDERS WHICH ARE PLACED IN 2014 JANUARY AND ALSO THE ORDER_STATUS IN EITHER COMPLETE OR CLOSED

# COMMAND ----------

display(orders_df.filter("order_status in ('COMPLETE', 'CLOSED') and date_format(order_date, 'yyyyMM') = 201401"))

# COMMAND ----------

from pyspark.sql.functions import sum, round, col

# COMMAND ----------

display(orders_df.\
    filter("order_status in ('COMPLETE', 'CLOSED') and date_format(order_date, 'yyyyMM') = 201401").\
    join(order_items_df, orders_df['order_id'] == order_items_df['order_item_order_id']).\
    groupBy('order_date').\
    agg(round(sum('order_item_subtotal'), 2).alias('order_revenue')).\
    orderBy(col('order_revenue').desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC * Parent (orders_df) Child (order_item_df) Relationship
# MAGIC * order_df['order_id] and order_item_df['order_item_order_id] -> every order_item_order_id will be present in orders. you will not found any order_item_order_id which is not present in orders. However its possible, there might be order_id from orders which will be not present in order_items(order_item_order_id).

# COMMAND ----------

orders_df.count()

# COMMAND ----------

orders_df.select('order_id').distinct().count()

# COMMAND ----------

order_items_df.count()

# COMMAND ----------

order_details_df.select('order_item_id').distinct().count()

# COMMAND ----------

order_details_df.select('order_item_order_id').distinct().count()

# COMMAND ----------

display(
    orders_df.join(order_items_df, orders_df['order_id'] == order_items_df['order_item_order_id'], 'left').\
        select(orders_df['*'], order_items_df['order_item_id'], order_items_df['order_item_subtotal']).\
            orderBy(orders_df['order_id'])
)

# COMMAND ----------

joined_df = orders_df.join(order_items_df, orders_df['order_id'] == order_items_df['order_item_order_id'], 'left').\
        select(orders_df['*'], order_items_df['order_item_id'], order_items_df['order_item_subtotal']).\
            orderBy(orders_df['order_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC * find out order_id which dont have any order_item associtate. 

# COMMAND ----------

display(joined_df.filter('order_item_id is null'))

# COMMAND ----------

display(joined_df.filter('order_item_id is null').count())

# COMMAND ----------

display(
    order_items_df.join(orders_df, orders_df['order_id'] == order_items_df['order_item_order_id'], 'right').\
    select(orders_df['*'], order_items_df['order_item_subtotal'])
)

# COMMAND ----------

joined_df_right_jon = order_items_df.join(orders_df, orders_df['order_id'] == order_items_df['order_item_order_id'], 'right').\
    select(orders_df['*'], order_items_df['order_item_subtotal'])

# COMMAND ----------

display(joined_df_right_jon.filter('order_item_id is null'))

# COMMAND ----------

joined_df_right_jon.filter('order_item_id is null').count()

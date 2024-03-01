# Databricks notebook source
orders_df = spark.\
    read.\
    csv(
        'dbfs:/public/retail_db/orders',
        schema='order_id INT, order_date DATE, order_customer_id INT, order_status STRING'
    )

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

from pyspark.sql.functions import sum, round
daily_revenue_df = orders_df.\
    filter("order_status in ('COMPLETE','CLOSED')").\
    join(order_items_df, orders_df['order_id']==order_items_df['order_item_order_id']).\
    groupBy('order_date').\
    agg(round(sum('order_item_subtotal'),2).alias('revenue'))


# COMMAND ----------

display(daily_revenue_df.orderBy('order_date'))

# COMMAND ----------

daily_product_revenue_df = orders_df.\
    filter("order_status in ('COMPLETE','CLOSED')").\
    join(order_items_df, orders_df['order_id']==order_items_df['order_item_order_id']).\
    groupBy(orders_df['order_date'], order_items_df['order_item_product_id']).\
    agg(round(sum('order_item_subtotal'),2).alias('revenue'))


# COMMAND ----------

from pyspark.sql.functions import col
display(daily_product_revenue_df.orderBy('order_date', col('revenue').desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC * Global Ranks - orderBy
# MAGIC * Ranks with in each partition or group - partitionBy and orderBy
# MAGIC * df.select(rank().over(Window.orderBy(col('revenue').desc())))
# MAGIC * df.select(rank().over(Window.partitionBy('order_date').orderBy(col('revenue').desc())))

# COMMAND ----------

from pyspark.sql.functions import dense_rank, col
from pyspark.sql.window import Window

# COMMAND ----------

display(
    daily_product_revenue_df.\
    withColumn('drnk', dense_rank().over(Window.orderBy(col('revenue').desc()))).\
    orderBy('order_date')
)

# COMMAND ----------

#Top 5 product
display(
    daily_product_revenue_df.\
    withColumn('drnk', dense_rank().over(Window.orderBy(col('revenue').desc()))).\
    filter("drnk <= 5").\
    orderBy('order_date', col('revenue').desc())
)

# COMMAND ----------

#
display(
    daily_product_revenue_df.\
    withColumn('drnk', dense_rank().over(Window.partitionBy('order_date').orderBy(col('revenue').desc()))).\
    orderBy('order_date', col('revenue').desc())
)

# COMMAND ----------

#Top 5 product, each date
spec = Window.partitionBy('order_date').orderBy(col('revenue').desc())
display(
    daily_product_revenue_df.\
    withColumn('drnk', dense_rank().over(spec)).\
    filter("drnk <= 5").\
    orderBy('order_date', col('revenue').desc())
)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists alquadri_retail_db.student_scores(
# MAGIC   student_id int,
# MAGIC   student_score INT
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC insert overwrite alquadri_retail_db.student_scores values
# MAGIC (1, 980),
# MAGIC (2, 960),
# MAGIC (3, 960),
# MAGIC (4, 990),
# MAGIC (5, 920),
# MAGIC (6, 960),
# MAGIC (7, 980),
# MAGIC (8, 960),
# MAGIC (9, 940),
# MAGIC (10, 940)
# MAGIC

# COMMAND ----------

display(
    spark.read.table('alquadri_retail_db.student_scores').\
        orderBy(col('student_score').desc())
)

# COMMAND ----------

from pyspark.sql.functions import rank, dense_rank
spec = Window.orderBy(col('student_score').desc())
student_score =  spark.read.table('alquadri_retail_db.student_scores')
display(
    student_score.\
    withColumn('drnk', dense_rank().over(spec)).\
    withColumn('rnk', rank().over(spec)).\
    orderBy(col('student_score').desc())
)

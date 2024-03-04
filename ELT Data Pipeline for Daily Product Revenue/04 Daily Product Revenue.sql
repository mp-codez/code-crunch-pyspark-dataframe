-- Databricks notebook source
use alquadri_retail_bronze

-- COMMAND ----------

insert overwrite directory '${gold_base_dir}/daily_product_revenue'
using parquet
select o.order_date,
  oi.order_item_product_id,
  round(sum(oi.order_item_subtotal), 2) as revenue
from orders as o
  join order_items as oi
    on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by 1, 2

-- COMMAND ----------

select *from parquet.`${gold_base_dir}/daily_product_revenue`
order by 1, 2 desc

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/public/retail_db_gold/daily_product_revenue

-- COMMAND ----------



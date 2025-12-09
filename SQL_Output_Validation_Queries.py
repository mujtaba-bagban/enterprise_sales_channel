# Databricks notebook source
# DBTITLE 1,Profit By Year
# MAGIC %sql
# MAGIC SELECT YEAR(order_date) AS year,
# MAGIC        ROUND(SUM(profit), 2) AS total_profit
# MAGIC FROM sales_channel.fact_orders
# MAGIC GROUP BY YEAR(order_date)
# MAGIC ORDER BY year;
# MAGIC
# MAGIC --Below query can be used to validate. Since the data in the fact_profit_summary is already aggreagated this can be used in reporting
# MAGIC
# MAGIC /*%sql
# MAGIC SELECT year,
# MAGIC        ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM sales_channel.fact_profit_summary
# MAGIC GROUP BY year
# MAGIC ORDER BY year;*/
# MAGIC

# COMMAND ----------

# DBTITLE 1,Profit By year and category
# MAGIC %sql
# MAGIC SELECT YEAR(ord.order_date) AS year,
# MAGIC        prd.category,
# MAGIC        ROUND(SUM(ord.profit), 2) AS total_profit
# MAGIC FROM sales_channel.fact_orders ord
# MAGIC JOIN sales_channel.dim_products_details prd ON ord.product_id = prd.product_id
# MAGIC GROUP BY YEAR(ord.order_date), prd.category
# MAGIC ORDER BY year, prd.category;

# COMMAND ----------

# DBTITLE 1,Profit by Customer
# MAGIC %sql
# MAGIC SELECT cust.customer_id,cust.customer_name,ROUND(SUM(ord.profit), 2) AS total_profit
# MAGIC FROM sales_channel.fact_orders ord
# MAGIC JOIN sales_channel.dim_customers cust 
# MAGIC ON ord.customer_id = cust.customer_id
# MAGIC GROUP BY cust.customer_id,cust.customer_name
# MAGIC ORDER BY total_profit DESC;

# COMMAND ----------

# DBTITLE 1,Profit by Customer + Year
# MAGIC %sql
# MAGIC SELECT cust.customer_id, cust.customer_name,YEAR(ord.order_date) AS year,ROUND(SUM(ord.profit), 2) AS total_profit
# MAGIC FROM sales_channel.fact_orders ord
# MAGIC JOIN sales_channel.dim_customers cust ON ord.customer_id = cust.customer_id
# MAGIC GROUP BY cust.customer_id,cust.customer_name, YEAR(ord.order_date)
# MAGIC ORDER BY cust.customer_id,cust.customer_name, year;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Query to get distinct years and to validate if above dataset is correct.
# MAGIC SELECT distinct YEAR(order_date) AS year
# MAGIC FROM sales_channel.fact_orders
# MAGIC --GROUP BY YEAR(order_date)
# MAGIC ORDER BY year;
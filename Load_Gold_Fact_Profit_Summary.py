# Databricks notebook source
from pyspark.sql.functions import year, sum as _sum, col

# Load input fact orders table
fact_orders = spark.table("sales_channel.fact_orders")
## As per the problem defination, a profit summary table needs to be created with the following columns: year, category, sub_category, customer_id, total_profit
# As per the exercise instructions, we need to group by year, category, sub_category and customer
fact_profit_summary = (
    fact_orders
    .groupBy(
        year(col("order_date")).alias("year"),
        col("category"),
        col("sub_category"),
        col("customer_id")
    )
    .agg(_sum("profit").alias("total_profit"))
)

# Overwrite sales_channel.fact_profit_summary table
fact_profit_summary.write.mode("overwrite").format("delta").saveAsTable("sales_channel.fact_profit_summary")

print(f"fact_profit_summary table loaded successfully")

# COMMAND ----------

##--%sql
##--select * from sales_channel.fact_profit_summary where customer_id = 'NW-18400'
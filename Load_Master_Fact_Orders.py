# Databricks notebook source
spark.sql("use sales_channel")

# COMMAND ----------

from pyspark.sql.functions import to_date, round, trim, col

# Load bronze orders. This is a source table to create Fact_Orders Table
bronze_orders = spark.table("sales_channel.bronze_orders_landing")

# Load Customer, Customers Contact and Products dimensions required as per the requirements(problem statment)
dim_customers = spark.table("sales_channel.dim_customers")
dim_customers_contact = spark.table("sales_channel.dim_customers_contact")
dim_products = spark.table("sales_channel.dim_products_details")

# Filter the orders data where customer and product doesn't exist and load into master fact table
orders = (bronze_orders
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("order_date", to_date(trim(col("order_date")), "d/M/yyyy"))
    .withColumn("ship_date", to_date(trim(col("ship_date")), "d/M/yyyy"))
    .withColumn("ship_mode", trim(col("ship_mode")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("product_id", trim(col("product_id")))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("price", col("price").cast("double"))
    .withColumn("discount", col("discount").cast("int"))
    .withColumn("profit", round(col("profit").cast("double"), 2))
    .filter(col("customer_id").isNotNull() & col("product_id").isNotNull())
)
##Problem statement 

#Create an enriched table which has
		#1.	order information 
				#1.	Profit rounded to 2 decimal places
		#2.	Customer name and country
		#3.	Product category and sub category
  
# Join with customer , customer contact and product cleansed data
fact_orders = (orders
    .join(dim_customers, "customer_id", "left")
    .join(dim_customers_contact, "customer_id", "left")
    .join(dim_products, "product_id", "left")
    .select(
        "order_id","order_date","ship_date","ship_mode", ##order information
        "customer_id","customer_name","country","segment","region", ## customer information
        "email","phone","address","city","state","postal_code",
        "product_id","product_name","category","sub_category","unit_price",##product information
        "quantity","price","discount","profit"
    )
)
##fact_orders.show()
#Create and OverWrite to Delta Master Fact Orders table
### 
fact_orders.write.mode("overwrite").format("delta").saveAsTable("sales_channel.fact_orders")
print(f"fact_orders table is loaded successfully")

# COMMAND ----------

##%sql
##select * from sales_channel.master_fact_orders where phone is null or phone =''
##--%sql
##--select count(*) from sales_channel.fact_orders
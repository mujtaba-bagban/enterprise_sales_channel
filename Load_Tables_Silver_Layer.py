# Databricks notebook source
spark.sql("use sales_channel")

# COMMAND ----------

# DBTITLE 1,Load_Dim_Customers

## Silver: Load customers dimension table
## As a healthy design strategy, we have to enrich the bronze customers contact data with additional table to make sure PI columns are stored in different dim table.

from pyspark.sql.functions import col

bronze_customers = spark.table("sales_channel.bronze_customers_landing") ## Customer details without PI columns
#bronze_customer_contact = spark.table("sales_channel.bronze_customers_landing") ## Customer Contact details with PI columns
#bronze_products = spark.table("sales_channel.bronze_products_landing") ## Product details

##Filter records having null customer id and Drop Duplicates
dim_customers = (bronze_customers
    .select("customer_id","customer_name","country","segment","region")
    .filter(col("customer_id").isNotNull() & (col("customer_id") != ""))
    .dropDuplicates(["customer_id"]))
## Create and overwrite delta silver_dim_customers
dim_customers.write.mode("overwrite").format("delta").saveAsTable("sales_channel.dim_customers")

print(f"dim_customers table is loaded successfully")

# COMMAND ----------

# DBTITLE 1,Load_Dim_Customers_Contact
from pyspark.sql.functions import regexp_replace,col,trim

bronze_customers_contact = spark.table("sales_channel.bronze_customers_landing") ## Input Customer Contact details with PI columns

#Select the required PI columns to load customers_contact. Filter and Drop Duplicates
dim_customers_contact = (bronze_customers_contact
    .select(
        trim(col("customer_id")).alias("customer_id"),
        regexp_replace(trim(col("email")), "#ERROR!", "").alias("email"),
        regexp_replace(trim(col("phone")), "[^0-9]", "").alias("phone"),
        trim(col("address")).alias("address"),
        trim(col("city")).alias("city"),
        trim(col("state")).alias("state"),
        trim(col("postal_code")).alias("postal_code")
    )
    .filter(col("customer_id").isNotNull() & (col("customer_id") != ""))
    .dropDuplicates(["customer_id"])
)
## Create and overwrite delta dim_customers_contact
dim_customers_contact.write.mode("overwrite").format("delta").saveAsTable("sales_channel.dim_customers_contact")

print(f"dim_customers_contact table is loaded successfully")

# COMMAND ----------

# DBTITLE 1,SELECT_DIM_CUSTOMERS
##--%sql
##--select * from sales_channel.silver_dim_customers_contact

# COMMAND ----------

# DBTITLE 1,Load_Dim_Products
from pyspark.sql.functions import col
# Read product data from bronze table
bronze_products = spark.table("sales_channel.bronze_products_landing")
#Filer and Drop Duplicates based on product_id.
##Also state column is not included in products dim table
dim_products_details = (bronze_products
    .select("product_id","product_name","category","sub_category","unit_price")
    .filter(col("product_id").isNotNull() & (col("product_id") != ""))
    .dropDuplicates(["product_id"]))
## Create and overwrite dim_products_details
dim_products_details.write.mode("overwrite").format("delta").saveAsTable("sales_channel.dim_products_details")
print(f"dim_products_details table is loaded successfully")

# COMMAND ----------

# DBTITLE 1,Select_Silver_Dim_Products
##--%sql
##--SELECT * FROM  sales_channel.dim_products_details
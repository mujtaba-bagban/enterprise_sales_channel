# Databricks notebook source
# DBTITLE 1,Define paths
#Define the volume paths for each of the input data sets 
ORDERS_PATH = "/Volumes/workspace/sales_channel/input_data/Orders.json"
CUSTOMERS_PATH = "/Volumes/workspace/sales_channel/input_data/Customer.xlsx"
PRODUCTS_PATH = "/Volumes/workspace/sales_channel/input_data/Products.csv"

# COMMAND ----------

# DBTITLE 1,Use Sales_Channel Schema
#Use sales_channel schema to create the tables
spark.sql("USE sales_channel")

# COMMAND ----------

# DBTITLE 1,Load_Orders_Bronze_Table
from pyspark.sql.functions import col, trim
from pyspark.sql.utils import AnalysisException
##Read Orders json from above defined path
df_json = (spark.read
.format("json")
.option("multiline", "true")
.load(ORDERS_PATH))

try: ## implement try catch block to handle file not found exception
    #Rename the Columns to lower case and remove the space/s in the column names
    orders_bronze = (df_json 
        .withColumnRenamed("Row ID", "row_id")
        .withColumnRenamed("Order ID", "order_id")
        .withColumnRenamed("Order Date", "order_date")
        .withColumnRenamed("Ship Date", "ship_date")
        .withColumnRenamed("Ship Mode", "ship_mode")
        .withColumnRenamed("Customer ID", "customer_id")
        .withColumnRenamed("Product ID", "product_id")
        .withColumnRenamed("Quantity", "quantity")
        .withColumnRenamed("Price", "price")
        .withColumnRenamed("Discount", "discount")
        .withColumnRenamed("Profit", "profit"))
#Trim Strings
    for c in ["order_id","order_date","ship_date","ship_mode","customer_id","product_id"]:
        if c in orders_bronze.columns:
            orders_bronze = orders_bronze.withColumn(c, trim(col(c)))

## Load the delta bronze_orders_landing table with proper column names from raw data

    orders_bronze.write.mode("overwrite").format("delta").saveAsTable("sales_channel.bronze_orders_landing")
    print(f"bronze_orders_landing table is loaded successfully")

except FileNotFoundError as e:
    # Raise Error
    print(f"Error: Products file missing at {ORDERS_PATH}")

# COMMAND ----------

# DBTITLE 1,Load_Customer_Bronze_Table
## Read Customer XLSX file and load it into bronze_customers_landing table

%pip install openpyxl
######## %pip install openpyxl is already installed from Test_Create_DataBase_Test_Code NoteBook
######## To read/load the excel file from pandas we have to convert Pandas DataFrame to Spark DataFrame
from pyspark.sql.functions import col, trim
from pyspark.sql.utils import AnalysisException
import pyspark.pandas as ps
ps_df = ps.read_excel(
    CUSTOMERS_PATH,
    sheet_name="Worksheet", ##Worksheet name is Worksheet in input Customers XLSX file
    header=0,
    dtype=str
)
spark_df_customers = ps_df.to_spark()

try: ## implement try catch block to handle file not found exception
#Rename the columns to lower case and remove the space/s in the column names
    customers_bronze = (spark_df_customers
        .withColumnRenamed("Customer ID", "customer_id")
        .withColumnRenamed("Customer Name", "customer_name")
        .withColumnRenamed("Country", "country")
        .withColumnRenamed("City", "city")
        .withColumnRenamed("State", "state")
        .withColumnRenamed("Segment", "segment")
        .withColumnRenamed("Region", "region")
        .withColumnRenamed("Postal Code", "postal_code"))

    ## Trim Strings for below columns
    for c in ["customer_id","customer_name","country","segment","region","postal_code"]:
        if c in customers_bronze.columns:
            customers_bronze = customers_bronze.withColumn(c, trim(col(c)))
    ### Load and overwrite the delta bronze_customers_landing table with proper column names from raw data

    customers_bronze.write.mode("overwrite").format("delta").saveAsTable("sales_channel.bronze_customers_landing")
    print(f"bronze_customers_landing table is loaded successfully")
except FileNotFoundError as e:
    # Raise Error
    print(f"Error: Products file missing at {CUSTOMERS_PATH}")

# COMMAND ----------

# DBTITLE 1,Load_Products_Bronze_Table
from pyspark.sql.functions import col, trim
from pyspark.sql.utils import AnalysisException
PRODUCTS_PATH = "/Volumes/workspace/sales_channel/input_data/Products.csv"
###### Read Products CSV file. Because some products in the input data have commas in the name, we need to use the escape and quote options.
df_products = (spark.read
    .format("csv")
    .option("header", "true")     
    .option("inferSchema", "true") 
    .option("quote", "\"")             # treat double quotes as text in text
    .option("escape", "\"")            # escape quotes inside quoted strings
    .option("multiLine", "true") 
.load(PRODUCTS_PATH))
try: ## implement try catch block to handle file not found exception

##Rename the columns to lower case and remove the space/s in the column names
    products_bronze = (df_products
        .withColumnRenamed("Product ID", "product_id")
        .withColumnRenamed("Category", "category")
        .withColumnRenamed("Sub-Category", "sub_category")
        .withColumnRenamed("Product Name", "product_name")
        .withColumnRenamed("State", "state")
        .withColumnRenamed("Price per product", "unit_price"))

    # Trim strings for below column list
    for c in ["product_id","product_name","category","sub_category"]:
        if c in products_bronze.columns:
            products_bronze = products_bronze.withColumn(c, trim(col(c)))
    # Cast unit_price to double
    products_bronze = products_bronze.withColumn("unit_price", col("unit_price").cast("double"))

## Overwrite the delta bronze_products_landing table with proper column names from raw data
    products_bronze.write.mode("overwrite").format("delta").saveAsTable("sales_channel.bronze_products_landing")
    print(f"bronze_products_landing table is loaded successfully")
except FileNotFoundError as e: ## Raise filenotfound error
    # Raise Error
    print(f"Error: Products file missing at {PRODUCTS_PATH}")

# COMMAND ----------

# DBTITLE 1,Count_Products_Bronze_Table
# MAGIC %sql
# MAGIC select count(*) from sales_channel.bronze_products_landing
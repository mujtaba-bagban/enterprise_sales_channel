# Databricks notebook source
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

##Create Spark Session

spark = SparkSession.builder.appName("Sales_Channel_Data_Validation_Test_Suite").getOrCreate()

## Read Table data from each table(landing,dim and fact) and perform below test cases.
## 1.products_landing,customer_landing and orders_landing should have the respective core product fields,customer fields and order fields.
##      This ensures that input feeds are with expected structure. Failure on which there might be cases where successive data loads might fail.
## 2.Check no records have a null order_id in bronze_orders_landing. This can be extended to test product_id in bronze_products_landing table.
## 3.customer_id should be unique in bronze_customers_landing
## 4.Validate fact_orders profit total matches the raw bronze_orders_landing. A very important test case where the data will be validated in orders table for its correctness.
## 5.Validate that fact_profit_summary contains valid rows for year 2016.
## 6.Validate that fact_profit_summary has negative total profit for year 2016. There might be cases where profit is negative due to discounts.

## Generic Function to get table name
def get_table_name(name):
    return spark.table(name)

## Function to test mandatory columns for product data
def test_mandatory_product_columns():
    """bronze_products_landing should have the core product fields."""
    df = get_table_name("sales_channel.bronze_products_landing")
    required = {"product_id", "category", "sub_category", "unit_price"}
    missing = required - set(df.columns)
    assert not missing, f"Missing columns in products table: {missing}"
    print(f"Test Case Products Columns Check Executed")

## Function to test mandatory columns for customers data
def test_mandatory_customer_columns():
    """bronze_customers_landing should have the core customer fields."""
    df = get_table_name("sales_channel.bronze_customers_landing")
    required = {"customer_id", "customer_name","country","city","state","segment","region","postal_code"}
    missing = required - set(df.columns)
    assert not missing, f"Missing columns in customers table: {missing}"
    print(f"Test Case Customer Columns Check Executed")

## Function to test mandatory columns for orders data
def test_mandatory_orders_columns():
    """bronze_orders_landing should have the core order fields."""
    df = get_table_name("sales_channel.bronze_orders_landing")
    required = {"order_id", "order_date", "ship_date","ship_mode","customer_id","product_id","quantity","discount","profit","price"}
    missing = required - set(df.columns)
    assert not missing, f"Missing columns in orders table: {missing}"
    print(f"Test Case Orders Columns Check Executed")

## Function to test Unique Customers in customers data

def test_unique_customers():
    """Each customer_id should be unique in bronze_customers_landing."""
    df = get_table_name("sales_channel.bronze_customers_landing")
    total = df.count()
    distinct = df.select("customer_id").distinct().count()
    assert total == distinct, "Found duplicate customer_id values!"
    print(f"Test Case Unique Customers Check Executed")

## Function to test no records have a null order_id in bronze_orders_landing
def test_orders_id_is_null():
    """Orders must always have a order id value"""
    df = get_table_name("sales_channel.bronze_orders_landing")
    null_count = df.filter(col("order_id").isNull()).count() ## get null count for order_id
    assert null_count == 0, f"Found {null_count} orders with null Order_ID!"
    print(f"Test Case Null Order ID Check Executed")

## Function to Test fact_orders profit totals match the raw bronze_orders_landing
def test_fact_orders_profit_matches_bronze_order():
    """Validate fact_orders profit totals match the raw bronze_orders_landing."""
    bronze = get_table_name("sales_channel.bronze_orders_landing") ## Read bronze_orders_landing
    fact = get_table_name("sales_channel.fact_orders") ## Read fact_orders

    bronze_profit = bronze.groupBy("customer_id").agg(_sum("profit").alias("bronze_profit")) ## Group by customer and sum profit in bronze_orders_landing
    fact_profit = fact.groupBy("customer_id").agg(_sum("profit").alias("fact_profit")) ## Group by customer and sum profit in fact_orders

    join_on_cust_id = bronze_profit.join(fact_profit, "customer_id") ## Join both datasets on customer id
    mismatches = [r for r in join_on_cust_id.collect() if r["bronze_profit"] != r["fact_profit"]]
    assert not mismatches, f"Profit mismatches found: {mismatches}"
    print(f"Test Case Fact Orders Profit Check Executed")

## Function to Test that fact_profit_summary contains valid rows for year 2016.
def test_profit_summary_for_2016():
    """Check that fact_profit_summary contains valid rows for year 2016."""
    summary = get_table_name("sales_channel.fact_profit_summary")
    df_2016 = summary.filter(col("year") == 2016)

    assert df_2016.count() > 0, "No profit summary rows found for year 2016!"
    print(f"Test Case Fact Profit Summary for 2016 Check Executed")

## Function to Test Negative Total profit in fact_profit_summary for year 2016.
def test_negative_total_profit_for_2016():
    """Check that fact_profit_summary has negative total profit for year 2016."""
    summary = get_table_name("sales_channel.fact_profit_summary")
    df_2016 = summary.filter(col("year") == 2016)
    assert df_2016.filter(col("total_profit") < 0).count() > 0,  "No Negative total profit in found for year 2016 in profit summary!"
    print(f"Test Case For Negative Total Profit in Fact Profit Summary for 2016 year Executed")

# COMMAND ----------

test_mandatory_product_columns()
test_mandatory_customer_columns()
test_mandatory_orders_columns()
test_unique_customers()
test_orders_id_is_null()
test_fact_orders_profit_matches_bronze_order()
test_profit_summary_for_2016()
test_negative_total_profit_for_2016()
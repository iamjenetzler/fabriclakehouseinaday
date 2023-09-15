-- Databricks notebook source
-- MAGIC %md
-- MAGIC # This is a Sample Delta Live Table Pipeline Notebook
-- MAGIC    It runs as a triggered or continuously running pipeline under the workflows tab.   
-- MAGIC    You can only check syntax in this notebook and it must run under a pipeline.
-- MAGIC    
-- MAGIC CREATE TABLE sales_orders_raw
-- MAGIC USING JSON
-- MAGIC OPTIONS(path "/databricks-datasets/retail-org/sales_orders/");
-- MAGIC 
-- MAGIC CREATE TABLE sales_orders_raw
-- MAGIC USING JSON
-- MAGIC OPTIONS(path "/dbfs/FileStore/richjohn/deltademoasset/bronze/sales_orders/");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In cells 3 and 4 replace the YOUR_USER_NAME_HERE with your username.   Refer to 03.1 notebook cell #3 for created paths and variables

-- COMMAND ----------

--Replace /mnt definition below by replacing it with your credential name.   i.e. change richjohn to your credential name
CREATE STREAMING LIVE TABLE customers
COMMENT "The customers buying finished products, ingested from /databricks-datasets."
TBLPROPERTIES ("myCompanyPipeline.quality" = "mapping")
AS SELECT * FROM cloud_files("/dbfs/FileStore/<YOUR_USER_NAME_HERE>/deltademoasset/bronze/customers/", "csv");

-- COMMAND ----------

CREATE STREAMING LIVE TABLE sales_orders_raw
COMMENT "The raw sales orders, ingested from /databricks-datasets."
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze")
AS
SELECT * FROM cloud_files("/dbfs/FileStore/<YOUR_USER_NAME_HERE>/deltademoasset/bronze/sales_orders/", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

CREATE STREAMING LIVE TABLE sales_orders_cleaned(
  CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW
)

PARTITIONED BY (order_date)

COMMENT "The cleaned sales orders with valid order_number(s) and partitioned by order_datetime."
TBLPROPERTIES ("myCompanyPipeline.quality" = "silver")

AS
SELECT f.customer_id, f.customer_name, f.number_of_line_items, 
  TIMESTAMP(from_unixtime((cast(f.order_datetime as long)))) as order_datetime, 
  DATE(from_unixtime((cast(f.order_datetime as long)))) as order_date, 
  f.order_number, f.ordered_products, c.state, c.city, c.lon, c.lat, c.units_purchased, c.loyalty_segment
  FROM STREAM(LIVE.sales_orders_raw) f
  LEFT JOIN LIVE.customers c
      ON c.customer_id = f.customer_id
     AND c.customer_name = f.customer_name

-- COMMAND ----------

CREATE LIVE TABLE sales_order_in_la
COMMENT "Sales orders in LA."
TBLPROPERTIES ("myCompanyPipeline.quality" = "gold")

AS
SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, SUM(ordered_products_explode.price) as sales, SUM(ordered_products_explode.qty) as quantity, COUNT(ordered_products_explode.id) as product_count
FROM (
  SELECT city, order_date, customer_id, customer_name, EXPLODE(ordered_products) as ordered_products_explode
  FROM LIVE.sales_orders_cleaned 
  WHERE city = 'Los Angeles'
  )
GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr

-- COMMAND ----------

CREATE LIVE TABLE sales_order_in_chicago
COMMENT "Sales orders in Chicago."
TBLPROPERTIES ("myCompanyPipeline.quality" = "gold")

AS
SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, SUM(ordered_products_explode.price) as sales, SUM(ordered_products_explode.qty) as quantity, COUNT(ordered_products_explode.id) as product_count
FROM (
  SELECT city, order_date, customer_id, customer_name, EXPLODE(ordered_products) as ordered_products_explode
  FROM LIVE.sales_orders_cleaned 
  WHERE city = 'Chicago'
  )
GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Next go to workflows tab on left and create a pipeline that references this notebook
-- MAGIC 
-- MAGIC 1 Create a pipeline pointing to this notebook
-- MAGIC 
-- MAGIC 2 Enter a unique database schema for the pipeline with your name/credential
-- MAGIC 
-- MAGIC 3 Create a fixed szie cluster and set workers to zero (this will creae a singlenode cluser)
-- MAGIC 
-- MAGIC 4 Set the pipeline mode to "continuous"
-- MAGIC 
-- MAGIC 5 run the pipeline
-- MAGIC 
-- MAGIC 6 After pipeline is running go to notebook 03.4 to create some sample data and write new files to bronze folder.   This will demonstrate autoloader appending new data in a continuous manner.

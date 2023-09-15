# Databricks notebook source
# MAGIC %md
# MAGIC #This Notebook will create sample data to test delta live tables piplelines

# COMMAND ----------

# MAGIC %md
# MAGIC ##Query Count of sales_orders_cleaned

# COMMAND ----------

# MAGIC %md
# MAGIC Make sure you change schema name in next 2 cells to match schema you named when creating the pipeline

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT count(*) FROM <PUT YOUR SCHEMA NAME HEARE FROM YOUR DELTA LIVE TABLE PIPELINE YOU CREATED>.sales_orders_cleaned;
# MAGIC --SELECT count(*) FROM salesschemapipeline04132023.sales_orders_cleaned;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY <PUT YOUR SCHEMA NAME HEARE FROM YOUR DELTA LIVE TABLE PIPELINE YOU CREATED>.sales_orders_cleaned;
# MAGIC --DESCRIBE HISTORY salesschemapipeline04132023.sales_orders_cleaned;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Define are user specific variables again 

# COMMAND ----------

setup_responses = dbutils.notebook.run("../utils/Get-Metadata", 0).split()

local_data_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]

print(f"Local data path is {local_data_path}")
print(f"DBFS path is {dbfs_data_path}")
print(f"Database name is {database_name}")
      
#print("Local data path is {}".format(local_data_path))
#print("DBFS path is {}".format(dbfs_data_path))
#print("Database name is {}".format(database_name))

# COMMAND ----------

bronzePath = f"{local_data_path}bronze"
silverPath = f"{local_data_path}silver"
goldPath = f"{local_data_path}gold"

print(bronzePath)
print(silverPath)
print(goldPath)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath + "/sales_orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create a new schema and table that we will use to gather sample data

# COMMAND ----------

# MAGIC %python
# MAGIC SalesDataSchema = f"{database_name}_Sales"
# MAGIC 
# MAGIC query = f"CREATE SCHEMA IF NOT EXISTS {SalesDataSchema}"
# MAGIC print( query )
# MAGIC 
# MAGIC spark.sql( query)

# COMMAND ----------

# MAGIC %python
# MAGIC query = f"USE {SalesDataSchema};"
# MAGIC 
# MAGIC spark.sql( query )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS  sales_orders_raw;
# MAGIC 
# MAGIC CREATE TABLE sales_orders_raw
# MAGIC USING JSON
# MAGIC OPTIONS(path "/databricks-datasets/retail-org/sales_orders/");

# COMMAND ----------

# MAGIC %md
# MAGIC Now select a few rows, set at "limit 3" in next cell into a dataframe

# COMMAND ----------

sales_df = spark.sql("SELECT f.customer_id, f.customer_name, f.number_of_line_items,   TIMESTAMP(from_unixtime((cast(f.order_datetime as long)))) as order_datetime,   DATE(from_unixtime((cast(f.order_datetime as long)))) as order_date,   CAST(f.order_number as long) as order_number, f.ordered_products FROM sales_orders_raw f limit 3")
                     
display(sales_df)

sales_df.createOrReplaceTempView("sampledata")


# COMMAND ----------

# MAGIC %md
# MAGIC ##Set order_date to current_date and randomize to the order number

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write sample incremental Data to Bronze Sales folde
# MAGIC Repeat cells 18 through 19 to add new files to the bronze sales folder and watch sales_orders_cleaned table grow from the DLT pipeline
# MAGIC 
# MAGIC (Note, in this scenario we're appending to the sales_orders_cleaned table and NOT doing a merge or upsert)

# COMMAND ----------

from pyspark.sql.functions import col, to_date, current_date
from pyspark.sql.functions import rand
import random
# Import date class from datetime module
from datetime import date,datetime

random_order_number = random.randrange(999)

random_file_number = random.randrange(999999)

sales_df = sales_df.withColumn("order_date", current_date())
sales_df = sales_df.withColumn("order_number",col("order_number") + random_order_number)

display(sales_df)

#print(random_file_number)



# COMMAND ----------

# write the new file in the bronze sales_orders folder to watch autoload and the DLT pipeline pickup this new data

sales_df.write.json(f"{bronzePath}/sales_orders/sales{random_file_number}")
#sales_df.write.json(f"{bronzePath}/sales_orders/salesRICH999.json")


# COMMAND ----------

display(dbutils.fs.ls(bronzePath + "/sales_orders"))


# COMMAND ----------

# MAGIC %md
# MAGIC #Further Reading on Change Data Capture (CDC) and Upsert or Merge
# MAGIC 
# MAGIC [Change Data Capture](https://docs.databricks.com/delta-live-tables/cdc.html#requirements&language-sql)
# MAGIC 
# MAGIC [Simplifying Change Data Capture with DLT](https://www.databricks.com/blog/2022/04/25/simplifying-change-data-capture-with-databricks-delta-live-tables.html#:~:text=Earlier%20CDC%20solutions%20with%20delta%20tables%20were%20using,the%20same%20rows%20of%20the%20target%20Delta%20table.)

# COMMAND ----------

# MAGIC %md
# MAGIC #Schema Evolution
# MAGIC 
# MAGIC By including the mergeSchema option in your query, any columns that are present in the DataFrame but not in the target table are 
# MAGIC automatically added on to the end of the schema as part of a write transaction. Nested fields can also be added, and these fields 
# MAGIC will get added to the end of their respective struct columns as well.
# MAGIC 
# MAGIC Data engineers and scientists can use this option to add new columns (perhaps a newly tracked metric, or a column of this month’s sales figures) 
# MAGIC to their existing machine learning production tables without breaking existing models that rely on the old columns.
# MAGIC 
# MAGIC [Schema Evolution](https://www.databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html)

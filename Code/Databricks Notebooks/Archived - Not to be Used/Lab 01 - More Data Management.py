# Databricks notebook source
# MAGIC %md
# MAGIC #Learn the Databricks and Azure Data Lake Store File System
# MAGIC 
# MAGIC [Azure Data Lake Store Gen2](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)
# MAGIC 
# MAGIC [Hierarchical Name Space](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-namespace)
# MAGIC 
# MAGIC [Databricks File Ssytem](https://learn.microsoft.com/en-us/azure/databricks/dbfs/)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Set Path and Database Names into Variables

# COMMAND ----------

setup_responses = dbutils.notebook.run("./utils/Get-Metadata", 0).split()

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

# MAGIC %md
# MAGIC ##Set Student name and Storage Account name for Storage container name and account

# COMMAND ----------

studentName = 'instructorrj'
storageName = 'classadlsg2'


# COMMAND ----------

# Set medallion paths

bronzePath = f"abfss://{studentName}@{storageName}.dfs.core.windows.net/bronze"
silverPath = f"abfss://{studentName}@{storageName}.dfs.core.windows.net/silver"
goldPath = f"abfss://{studentName}@{storageName}.dfs.core.windows.net/gold"

print(bronzePath)
print(silverPath)
print(goldPath)

# COMMAND ----------

# MAGIC %md
# MAGIC #Create a mount point for your lakehouse

# COMMAND ----------

# DBTITLE 1,Set Access and Create Mount Point to ADLS Gen2
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{studentName}@{storageName}.dfs.core.windows.net/",
  mount_point = f"/mnt/{studentName}lakehouse",
  extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,Databricks dbutils.fs Commands for working with Files!
dbutils.fs.help()

# COMMAND ----------

# DBTITLE 1,Databricks comes with Sample Datasets
#Use dbutils commands to list contents of a folder in the databricks file system (DBFS) or folers in Azure Data Lake Store
display(dbutils.fs.ls('/databricks-datasets/'))

# COMMAND ----------

# MAGIC %md
# MAGIC #Reading and Writing Data between folders and file systems using Python
# MAGIC ##

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read Databricks sample datasets into a data frame

# COMMAND ----------

# DBTITLE 1,Read a gzip Dataset 
#
nyctaxidf = spark.read.option("Header","true").csv("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-01.csv.gz")
display(nyctaxidf)

# COMMAND ----------

# DBTITLE 1,Read Databricks Dataset IoT Devices JSON
df = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write datasets to a data lake path

# COMMAND ----------

# DBTITLE 1,Write IoT Devices JSON
df.write.json(f"{bronzePath}/iot_devices.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ###List folder/path contents

# COMMAND ----------

# DBTITLE 1,List filesystem
dbutils.fs.ls(f"{bronzePath}")

# COMMAND ----------

# DBTITLE 1,Read IoT Devices JSON from ADLS Gen2
df2 = spark.read.json(f"{bronzePath}/iot_devices.json")
display(df2)

# COMMAND ----------

# DBTITLE 1,List mount
dbutils.fs.ls(f"/mnt/{studentName}lakehouse") 

# COMMAND ----------

# DBTITLE 1,Read IoT Devices JSON from mount point
df2 = spark.read.json(f"/mnt/{studentName}lakehouse/bronze/iot_devices.json")
display(df2)

# COMMAND ----------

iotDBschema = f"{database_name}_IOTdb"

query = f"CREATE SCHEMA IF NOT EXISTS {iotDBschema}"
print( query )

spark.sql( query)

# COMMAND ----------

query = f"DROP TABLE IF EXISTS {iotDBschema}.tblIOTdata;"
print( query)

spark.sql( query)

# COMMAND ----------

# DBTITLE 1,Create a table from the IoT Devices DataFrame
df.write.saveAsTable(f"{iotDBschema}.tblIOTdata")

# COMMAND ----------

query = f"SELECT * FROM {iotDBschema}.tblIOTdata LIMIT 5;"
print( query)

display(spark.sql( query))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Copy select clause printed above and run here as a SQL language cell
# MAGIC -- i.e. SELECT * FROM richjohn_dliad_db_IOTdb.tblIOTdata LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Loop through all files in a mount point folder path

# COMMAND ----------

# DBTITLE 0,Loop Through All Files in a Mount Point Folder Path
files = dbutils.fs.ls(f"/mnt/{studentName}lakehouse/bronze/iot_devices.json")

for file in files:
    print(file.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create some sample sales data for delta live tables

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS  sales_orders_raw;
# MAGIC 
# MAGIC CREATE TABLE sales_orders_raw
# MAGIC USING JSON
# MAGIC OPTIONS(path "/databricks-datasets/retail-org/sales_orders/");

# COMMAND ----------

sales_df = spark.sql("SELECT * FROM sales_orders_raw limit 3")

display(sales_df)

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/retail-org/sales_orders/")

# COMMAND ----------

sales_df = spark.sql("SELECT f.customer_id, f.customer_name, f.number_of_line_items,   TIMESTAMP(from_unixtime((cast(f.order_datetime as long)))) as order_datetime,   DATE(from_unixtime((cast(f.order_datetime as long)))) as order_date,   CAST(f.order_number as long) as order_number, f.ordered_products FROM sales_orders_raw f limit 3")
                     
                     
display(sales_df)

sales_df.createOrReplaceTempView("sampledata")


#sales_df[3,'order_datetime']

# COMMAND ----------

# DBTITLE 1,Create  random order_date and order_number and update dataframe
import random
# Import date class from datetime module
from datetime import date

random_order_number = random.randrange(99999999)
print(random_order_number)
random_order_date = random.randrange(99999999)
print(random_order_date)

mydate = date.today()
print(mydate)

from pyspark.sql.functions import when
mynewdf = sales_df.withColumn("order_date",col("order_date").date.today())

mynewdf.show()



# COMMAND ----------

from pyspark.sql.functions import col, to_date, current_date
from pyspark.sql.functions import rand

random_order_number = random.randrange(999)

sales_df = sales_df.withColumn("order_date", current_date())
sales_df = sales_df.withColumn("order_number",col("order_number") + random_order_number)


display(sales_df)

#next steps:
#  copy sales dataset from databricks samples to misc folder in container for each student by reading to df and
#  writing df to the misc folder.
#  run the previous few cells and random generator to write new file to the misc foldr which will use autoloader
#  for the sales dlt pipeline.    View Results in Power BI refresh.
#  use autoloader queries to see the execution run!!!
#  Time travel!!




# COMMAND ----------


from pyspark.sql.functions import when
df3 = df.withColumn("gender", when(df.gender == "M","Male") \
      .when(df.gender == "F","Female") \
      .otherwise(df.gender))
df3.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from sampledata;
# MAGIC 
# MAGIC Update sampledata
# MAGIC set order_datetime = getdate();

# COMMAND ----------

# MAGIC %md
# MAGIC #Clean up Tables and Schemas

# COMMAND ----------

# DBTITLE 1,Drop workshop table(s)
query = f"DROP TABLE {iotDBschema}.tblIOTdata;"
print( query)

display(spark.sql( query))

# COMMAND ----------

# DBTITLE 1,Drop schema(s)
query = f"DROP SCHEMA  {iotDBschema};"

print( query)

display(spark.sql( query))

# COMMAND ----------

# DBTITLE 1,Unmount filesystem
dbutils.fs.unmount(f"/mnt/{studentName}lakehouse") 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Loop through all files in a folder

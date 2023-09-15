# Databricks notebook source
 # https://learn.microsoft.com/en-us/azure/databricks/storage/azure-storage

# COMMAND ----------

from pyspark.sql import functions as F
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Architecture and Reference Links
# MAGIC 
# MAGIC [Medallion Architecture Reference - Microsoft](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion)
# MAGIC 
# MAGIC [Medallion Architecture Reference - Databricks](https://www.databricks.com/glossary/medallion-architecture)
# MAGIC 
# MAGIC [DBUtils Reference](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-utils)
# MAGIC 
# MAGIC [Databricks Data Objects](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/data-objects)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Data flow diagram
# MAGIC <img src="https://github.com/MicrosoftLIAD/classroomB2/blob/main/Code/Databricks%20Notebooks/images/lakehousearchitecture.png?raw=true" style="width: 650px; max-width: 100%; height: auto" />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Environment Setup
# MAGIC 
# MAGIC We will be using [Databricks Notebooks workflow](https://docs.databricks.com/notebooks/notebook-workflows.html) element to set up environment for this exercise. 
# MAGIC 
# MAGIC `dbutils.notebook.run()` command will run another notebook and return its output to be used here.

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

# MAGIC %md
# MAGIC 
# MAGIC # Ingesting Data - Bronze
# MAGIC ### Bronze layer (raw data)

# COMMAND ----------

# DBTITLE 1,Update these variables
studentName = 'richjohn'
storageName = 'stgliadadls'


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Set medallion paths

# COMMAND ----------

# DBTITLE 1,Path Variables
# Set medallion paths

bronzePath = f"abfss://{studentName}@{storageName}.dfs.core.windows.net/bronze"
silverPath = f"abfss://{studentName}@{storageName}.dfs.core.windows.net/silver"
goldPath = f"abfss://{studentName}@{storageName}.dfs.core.windows.net/gold"

# COMMAND ----------

# List bronze files
dbutils.fs.ls(f"abfss://{studentName}@{storageName}.dfs.core.windows.net/bronze")

# COMMAND ----------

# List silver files
dbutils.fs.ls(f"abfss://{studentName}@{storageName}.dfs.core.windows.net/silver")


# COMMAND ----------

# List gold files
dbutils.fs.ls(f"abfss://{studentName}@{storageName}.dfs.core.windows.net/gold")

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks Sample datasets to experiment with in the databricks-datasets folder

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Use flights datasets as our bronze datasets

# COMMAND ----------

# review the files in the source data that we need to bring into our medallion architecture
display(dbutils.fs.ls('/databricks-datasets/flights'))

# COMMAND ----------

# get the information about this dataset
f = open('/dbfs/databricks-datasets/flights/README.md', 'r')
print(f.read())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Copy and load Flight Data
# MAGIC 
# MAGIC Read source files and copy into the Bronze folder

# COMMAND ----------

# DBTITLE 1,Copy flight data source data to Bronze
# first, make the directory where you want the source data to land. NOTE: we're using folders in our data lake storage defined in the {bronzePath} variable which was set up in previous cells.
# the following command appends a folder called /flights to our bronzePath folder structure.
dbutils.fs.mkdirs(f"{bronzePath}/flights")

# copy the source data into the bronze folder
dbutils.fs.cp ("dbfs:/databricks-datasets/flights/departuredelays.csv", f"{bronzePath}/flights/", True) 

# confirm the file is where it should be
dbutils.fs.ls(f"{bronzePath}/flights")

# COMMAND ----------

# DBTITLE 1,Load flight data into a dataframe
# Set a file path to load the flight data
flightBronzePath = f'{bronzePath}/flights/departuredelays.csv'

flights_bronze_sdf = spark.read.load(flightBronzePath, format="csv", inferSchema="true", header="true")

display( flights_bronze_sdf )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Copy and load Airport Data
# MAGIC 
# MAGIC Read source files and copy into the Bronze folder

# COMMAND ----------

# DBTITLE 1,Copy airport data into bronze
# Note that we can copy data files from Internet data sources using a URL:  https://ourairports.com/data/airports.csv

url = 'https://ourairports.com/data/airports.csv'

# copy the source data into the bronze folder
dbutils.fs.cp (url, f"{bronzePath}/flights/", True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load airports.csv into a dataframe

# COMMAND ----------

# DBTITLE 1,Load airport data into a dataframe
# Set a file path to load the airport data
airportBronzePath = f'{bronzePath}/flights/airports.csv'

airports_bronze_sdf = spark.read.load(airportBronzePath, format="csv", inferSchema="true", header="true")

display( airports_bronze_sdf )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Clean and Transform Data - Silver
# MAGIC 
# MAGIC ### Silver layer (cleansed and conformed data)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Transform Flight Data

# COMMAND ----------

# Inspect the flights schema
flights_bronze_sdf.printSchema()

# COMMAND ----------

# make sure all values are upper case for later comparison
flights_bronze_sdf = flights_bronze_sdf.withColumn("origin", F.upper(F.col("origin"))).withColumn("destination", F.upper(F.col("destination")))

display( flights_bronze_sdf )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Transform Airport Data

# COMMAND ----------

# Inspect the airports schema
airports_bronze_sdf.printSchema()

# COMMAND ----------

# make sure all values are upper case for later comparison
airports_bronze_sdf = airports_bronze_sdf.withColumn("ident", F.upper(F.col("ident")))

display( airports_bronze_sdf )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Convert dataframe type

# COMMAND ----------

# MAGIC %md
# MAGIC Sometimes we use Pandas dataframes because they are marginally faster in memory dataframes for small to medium sized datasets.  
# MAGIC They are used often by data scientists and data engineers because they provide powerful and simple data manipulation capabilities over 
# MAGIC spark in many cases.
# MAGIC 
# MAGIC Spark or regular dataframes are often used for large datasets because they scale across multiple nodes in a a multi-node spark cluster.   
# MAGIC Pandas dataframes run on only a single node in the cluster and will run into a ceiling quickly with very large datasets.

# COMMAND ----------

# DBTITLE 1,Convert spark dataframes to Pandas dataframes
# create the silver data by combining both dataframes now that they are 'cleaned'
# convert to pandas using the toPandas()

flights_bronze_pdf = flights_bronze_sdf.toPandas()
airports_bronze_pdf = airports_bronze_sdf.toPandas()



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Combine Flight and Airport Data

# COMMAND ----------

# DBTITLE 1,Make a few changes before the combined data is saved
# recall in earlier cell we executed: import pandas as pd

# combined the two dataframes
combined_silver_pdf = pd.merge(flights_bronze_pdf,airports_bronze_pdf, how='inner', left_on='origin', right_on='ident', indicator=False, copy=True, sort=True)

# rename a column since we are only aligning origin data
combined_silver_pdf.rename(columns={'name': 'origin_name'},inplace=True) 

# replacing na values 'No data'
# this is required because when you write delta tables, a NaN (Not a Number) is a special floating-point value that represents an undefined value.
combined_silver_pdf["gps_code"].fillna("No Data", inplace = True)
combined_silver_pdf["iata_code"].fillna("No Data", inplace = True)
combined_silver_pdf["local_code"].fillna("No Data", inplace = True)
combined_silver_pdf["home_link"].fillna("No Data", inplace = True)


display( combined_silver_pdf )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Writing Data
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Save the combined data sets in the parquet format in the silver folder

# COMMAND ----------

# DBTITLE 1,Save as Parquet
# create a spark DataFrame
combined_silver_sdf = spark.createDataFrame( combined_silver_pdf )

# save the dataframe to the silver folder
combined_silver_sdf.write.parquet(f"{silverPath}/flights/combinedflightdata.parquet", mode='overwrite') 
                                            


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Working with SQL Tables - Gold
# MAGIC 
# MAGIC ### Gold layer (curated business-level tables)
# MAGIC 
# MAGIC [Create and Manage Schemas Reference ](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/create-schemas)
# MAGIC 
# MAGIC [Create Tables Reference](https://learn.microsoft.com/en-us/azure/databricks/getting-started/dataframes-python#save-a-dataframe-to-a-table)
# MAGIC 
# MAGIC [Delta Best Practices](https://learn.microsoft.com/en-us/azure/databricks/delta/best-practices)

# COMMAND ----------

# DBTITLE 1,Create a flight dataset for gold
# Create the 'gold' data
combined_gold_sdf = combined_silver_sdf

# define the columns to be dropped
cols = ("delay","id","latitude_deg", "longitude_deg", "elevation_ft", "iso_country", "iso_region", "gps_code", "home_link", "wikipedia_link", "keywords")

# drop the columns we do not want in the table
combined_gold_sdf.drop(*cols).printSchema()


# COMMAND ----------

FlightDataSchema = f"{database_name}_Flight"

query = f"CREATE SCHEMA IF NOT EXISTS {FlightDataSchema}"
print( query )

spark.sql( query)

# COMMAND ----------

query = f"DROP TABLE IF EXISTS {FlightDataSchema}.flightData;"
print( query)

spark.sql( query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Managed Table
# MAGIC 
# MAGIC 
# MAGIC [Managed Table Reference](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/data-objects#--what-is-a-managed-table)

# COMMAND ----------

# DBTITLE 1,Create a Managed Table
# create a managed table from an existing dataframe

# combined_gold_sdf.write.option("goldPath", f"{goldPath}/flights/tables").saveAsTable("Flight.flightData")
combined_gold_sdf.write.option("goldPath", f"{goldPath}/flights/tables").saveAsTable(f"{FlightDataSchema}.flightData")




# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Describe History - Managed
# MAGIC 
# MAGIC [History Reference](https://learn.microsoft.com/en-us/azure/databricks/delta/history)

# COMMAND ----------


query = f"DESCRIBE HISTORY {FlightDataSchema}.flightData;"
print( query )

display (spark.sql( query ) )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Describe detail - Managed

# COMMAND ----------


display( spark.sql(f"DESCRIBE DETAIL {FlightDataSchema}.flightData;") )

# COMMAND ----------

display( spark.sql(f"Select count(*) from {FlightDataSchema}.flightdata;") )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create an UnManaged Table
# MAGIC 
# MAGIC [UnManaged Table Reference](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/data-objects#--what-is-an-unmanaged-table)

# COMMAND ----------

# DBTITLE 1,Save data in the gold container in the Delta format
# save the dataframe to the gold folder in Delta format

combined_gold_sdf.write.partitionBy("origin").format("delta").mode("overwrite").save(f"{goldPath}/flights/flightdata")



# COMMAND ----------


spark.sql(f"DROP TABLE IF EXISTS {FlightDataSchema}.flightDataUnmanaged;")


# COMMAND ----------

spark.sql(f"USE {FlightDataSchema};")

# COMMAND ----------

query = f"CREATE EXTERNAL TABLE IF NOT EXISTS flightDataUnmanaged USING DELTA LOCATION 'abfss://{studentName}@{storageName}.dfs.core.windows.net/gold/flights/flightdata'" 
print( query )

spark.sql( query )

# COMMAND ----------

# Hard coded with student 1 
# EXAMPLE - Do not execute

# %sql

# USE Flight;

# CREATE EXTERNAL TABLE IF NOT EXISTS Flight.flightDataUnmanaged 
# USING DELTA
# LOCATION 'abfss://student1@classadlsg2.dfs.core.windows.net/gold/flights/flightdata' 


# COMMAND ----------

display( spark.sql(f"Select count(*) from {FlightDataSchema}.flightDataUnmanaged;") )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Describe History - UnManaged

# COMMAND ----------

display( spark.sql(f"describe history {FlightDataSchema}.flightDataUnmanaged;") )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Describe Detail - UnManaged

# COMMAND ----------


display( spark.sql(f"DESCRIBE DETAIL {FlightDataSchema}.flightDataUnmanaged;") )

# COMMAND ----------

# MAGIC %md
# MAGIC #Python Miscellaneous Examples of Data Manipulation

# COMMAND ----------

files = dbutils.fs.ls(f"{goldPath}/flights/flightdata/")

for file in files:
    print(file.name)

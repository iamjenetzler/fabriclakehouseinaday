# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Introduction
# MAGIC 
# MAGIC In this Notebook we will see how to work with Delta Tables when using Databricks Notebooks. 
# MAGIC 
# MAGIC Some of the things we will look at are:
# MAGIC * Creating a new Delta Table
# MAGIC * Using Delta Log and Time Traveling 
# MAGIC * Tracking data changes using Change Data Feed
# MAGIC * Cloning tables
# MAGIC * Masking data by using Dynamic Views
# MAGIC 
# MAGIC In addition to Delta Tables we will also get to see some tips and tricks on working on Databricks environment.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Environment Setup
# MAGIC 
# MAGIC We will be using [Databricks Notebooks workflow](https://docs.databricks.com/notebooks/notebook-workflows.html) element to set up environment for this exercise. 
# MAGIC 
# MAGIC `dbutils.notebook.run()` command will run another notebook and return its output to be used here.
# MAGIC 
# MAGIC `dbutils` has some other interesting uses such as interacting with file system or reading [Databricks Secrets](https://docs.databricks.com/dev-tools/databricks-utils.html#dbutils-secrets)

# COMMAND ----------

setup_responses = dbutils.notebook.run("../utils/Get-Metadata", 0).split()

local_data_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]

print("Local data path is {}".format(local_data_path))
print("DBFS path is {}".format(dbfs_data_path))
print("Database name is {}".format(database_name))

#print("Local data path is {}".format(local_data_path))
#print("DBFS path is {}".format(dbfs_data_path))
#print("Database name is {}".format(database_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create a user specific Schema

# COMMAND ----------

#Delete the old schema if it exists and cascade (drop) all tables within that schema
DeltaDataSchema = f"{database_name}_delta"

query = f"DROP SCHEMA IF EXISTS {DeltaDataSchema} CASCADE"
print( query )

spark.sql( query)

# COMMAND ----------

DeltaDataSchema = f"{database_name}_delta"

query = f"CREATE SCHEMA IF NOT EXISTS {DeltaDataSchema}"
print( query )

spark.sql( query)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Use the Schema for subsequent cell commands

# COMMAND ----------

query = f"USE {DeltaDataSchema};"

spark.sql( query)
print(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta Tables
# MAGIC 
# MAGIC Let's load store locations data to Delta Table. In our case we don't want to track any history and opt to overwrite data every time process is running.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create Delta Table
# MAGIC 
# MAGIC ***Load employees data to a spark data frame then write that dataframe to a Delta Table***
# MAGIC 
# MAGIC Let's start with simply reading CSV file into a DataFrame

# COMMAND ----------

dataPath = "dbfs:/databricks-datasets/retail-org/company_employees/company_employees.csv"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("quote", "\"") \
  .option("inferSchema", "true")\
  .csv(dataPath)\
  .limit(9)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Data is in a DataFrame, but not yet in a Delta Table. Still, we can already use SQL to query data or copy it into the Delta table

# COMMAND ----------

# Creating a Temporary View will allow us to use SQL to interact with data

df.createOrReplaceTempView("employees_csv_file")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * from employees_csv_file

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC SQL DDL can be used to create table using view we have just created. 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS employees;
# MAGIC 
# MAGIC CREATE TABLE employees
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM employees_csv_file;
# MAGIC 
# MAGIC SELECT * from employees;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This SQL query has created a simple Delta Table (as specified by `USING DELTA`). DELTA a default format so it would create a delta table even if we skip the `USING DELTA` part.
# MAGIC 
# MAGIC For more complex tables you can also specify table PARTITION or add COMMENTS.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Describe Delta Table
# MAGIC 
# MAGIC Now that we have created our first Delta Table - let's see what it looks like on our database and where are the data files stored.  
# MAGIC 
# MAGIC Quick way to get information on your table is to run `DESCRIBE EXTENDED` command on SQL cell

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED employees;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC There is yet another way to see these files - it is by using `%fs ls file_path` magic command.
# MAGIC You can try it out by filling in cell below with your table location path

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/

# COMMAND ----------

# DBTITLE 1,Notice the _delta_log/ folder unders employees.  Its contains the JSON log file info that tracks versions.
display(dbutils.fs.ls( f"dbfs:/user/hive/warehouse/{DeltaDataSchema}.db/employees/" ))


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Explore Delta Log
# MAGIC 
# MAGIC We can see that next to data stored in parquet file we have a *_delta_log/* folder - this is where the Log files can be found

# COMMAND ----------


log_files_location = f"dbfs:/user/hive/warehouse/{DeltaDataSchema}.db/employees/_delta_log/"
print(log_files_location)

display(dbutils.fs.ls(log_files_location))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC `00000000000000000000.json` has a very first commit logged for our table. Each change to the table will be creating a new _json_ file

# COMMAND ----------

first_log_file_location = f"{log_files_location}00000000000000000000.json"
dbutils.fs.head(first_log_file_location)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Another way to see what is stored on our log file is to use `DESCRIBE HISTORY` command. 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Update Delta Table
# MAGIC 
# MAGIC Provided dataset has address information, but no country name - let's add one!

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC alter table employees
# MAGIC add column employee_title string;

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC update employees
# MAGIC set employee_title = case when employee_id in (0,2) then 'MGR' else 'unknown' end

# COMMAND ----------

# MAGIC %sql 
# MAGIC select employee_id,employee_title from employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC update employees
# MAGIC set employee_title = 'CEO'
# MAGIC where employee_id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select employee_title, count(employee_id) as employee_count_by_title from employees group by employee_title

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Track Data History
# MAGIC 
# MAGIC 
# MAGIC Delta Tables keep all changes made in the delta log we've seen before. There are multiple ways to see that - e.g. by running `DESCRIBE HISTORY` for a table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC We can also check what files storage location has now

# COMMAND ----------

display(dbutils.fs.ls(log_files_location))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Time Travel
# MAGIC Having all this information and old data files mean that we can **Time Travel**!  You can query your table at any given `VERSION AS OF` or  `TIMESTAMP AS OF`.
# MAGIC 
# MAGIC Let's check again what table looked like before we ran last update

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from employees VERSION AS OF 2 where employee_title = 'MGR';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from employees VERSION AS OF 3
# MAGIC where employee_title = 'MGR';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Change Data Feed
# MAGIC 
# MAGIC 
# MAGIC The Delta change data feed represents row-level changes between versions of a Delta table. When enabled on a Delta table, the runtime records “change events” for all the data written into the table. This includes the row data along with metadata indicating whether the specified row was inserted, deleted, or updated.
# MAGIC 
# MAGIC It is not enabled by default, but we can enabled it using `TBLPROPERTIES`

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE employees SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Changes to table properties also generate a new version

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe history employees

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Change data feed can be seen by using `table_changes` function. You will need to specify a range of changes to be returned - it can be done by providing either version or timestamp for the start and end. The start and end versions and timestamps are inclusive in the queries. 
# MAGIC 
# MAGIC To read the changes from a particular start version to the latest version of the table, specify only the starting version or timestamp.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- simulate change of address for store AKL01 and removal of store BNE02
# MAGIC 
# MAGIC update employees
# MAGIC set employee_title = 'Supervisor'
# MAGIC where employee_title = 'MGR';
# MAGIC 
# MAGIC delete from employees
# MAGIC where employee_id = 0;
# MAGIC 
# MAGIC SELECT * FROM table_changes('employees', 5,6) -- Note that we increment versions due to UPDATE statements above

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Delta CDC gives back 4 cdc types in the "__change_type" column:
# MAGIC 
# MAGIC | CDC Type             | Description                                                               |
# MAGIC |----------------------|---------------------------------------------------------------------------|
# MAGIC | **update_preimage**  | Content of the row before an update                                       |
# MAGIC | **update_postimage** | Content of the row after the update (what you want to capture downstream) |
# MAGIC | **delete**           | Content of a row that has been deleted                                    |
# MAGIC | **insert**           | Content of a new row that has been inserted                               |
# MAGIC 
# MAGIC Therefore, 1 update results in 2 rows in the cdc stream (one row with the previous values, one with the new values)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### CLONE
# MAGIC 
# MAGIC 
# MAGIC What if our use case is more of a having monthly snapshots of the data instead of detailed changes log? Easy way to get it done is to create CLONE of table.
# MAGIC 
# MAGIC You can create a copy of an existing Delta table at a specific version using the clone command. Clones can be either deep or shallow.
# MAGIC 
# MAGIC  
# MAGIC 
# MAGIC * A **deep clone** is a clone that copies the source table data to the clone target in addition to the metadata of the existing table.
# MAGIC * A **shallow clone** is a clone that does not copy the data files to the clone target. The table metadata is equivalent to the source. These clones are cheaper to create, but they will break if original data files were not available

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists employees_clone;
# MAGIC 
# MAGIC create table employees_clone DEEP CLONE employees VERSION AS OF 3 -- you can specify timestamp here instead of a version

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe history employees_clone;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists employees_clone_shallow;
# MAGIC 
# MAGIC -- Note that no files are copied
# MAGIC 
# MAGIC create table employees_clone_shallow SHALLOW CLONE employees;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Dynamic Views
# MAGIC 
# MAGIC 
# MAGIC Our stores table has some PII data (email, phone number). We can use dynamic views to limit visibility to the columns and rows depending on groups user belongs to.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from employees

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP VIEW IF EXISTS v_employee_name_redacted;
# MAGIC 
# MAGIC CREATE VIEW v_employee_name_redacted AS
# MAGIC SELECT
# MAGIC   employee_id,
# MAGIC   CASE WHEN
# MAGIC     --NOT is_member('admins') THEN employee_name
# MAGIC     is_member('admins') THEN employee_name
# MAGIC     ELSE 'REDACTED'
# MAGIC   END AS employee_name,
# MAGIC   department,
# MAGIC   region,
# MAGIC   employee_title
# MAGIC FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from v_employee_name_redacted;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP VIEW IF EXISTS v_employees_limited;
# MAGIC 
# MAGIC CREATE VIEW v_employees_limited AS
# MAGIC SELECT *
# MAGIC FROM employees
# MAGIC WHERE 
# MAGIC   (employee_title = 'unknown');

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from v_employees_limited;

# COMMAND ----------



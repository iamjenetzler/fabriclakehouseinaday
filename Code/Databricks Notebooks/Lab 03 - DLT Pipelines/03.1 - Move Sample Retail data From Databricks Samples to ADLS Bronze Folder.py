# Databricks notebook source
# MAGIC %md
# MAGIC #Move Databricks Sample Retail data to Managed Bronze Folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configure environmental variables

# COMMAND ----------

# DBTITLE 1,Set User Specific Variables for Isolation
setup_responses = dbutils.notebook.run("../utils/Get-Metadata", 0).split()

local_data_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]
user_name = setup_responses[3]

print(f"Local data path is {local_data_path}")
print(f"DBFS path is {dbfs_data_path}")
print(f"Database name is {database_name}")
print(f"Username is {user_name}")
      
#print("Local data path is {}".format(local_data_path))
#print("DBFS path is {}".format(dbfs_data_path))
#print("Database name is {}".format(database_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create user specific medallion folders
# MAGIC We want to create user specific folders in DBFS for bronze, silver and Gold folders

# COMMAND ----------

#Remove old folders and data if they exist
dbutils.fs.rm(f"{local_data_path}/bronze",True)
dbutils.fs.rm(f"{local_data_path}/silver",True)
dbutils.fs.rm(f"{local_data_path}/gold",True)

# COMMAND ----------

dbutils.fs.mkdirs(f"{local_data_path}/bronze")
dbutils.fs.mkdirs(f"{local_data_path}/silver")
dbutils.fs.mkdirs(f"{local_data_path}/gold")


# COMMAND ----------

display(dbutils.fs.ls(local_data_path))

# COMMAND ----------


bronzePath = f"{local_data_path}bronze"
silverPath = f"{local_data_path}silver"
goldPath = f"{local_data_path}gold"

print(bronzePath)
print(silverPath)
print(goldPath)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Now Copy Databricks databricks-datasets sample retail data to Bronze folder in DBFS

# COMMAND ----------

# copy the Customers source data into the bronze folder
dbutils.fs.cp ("/databricks-datasets/retail-org/customers/", f"{bronzePath}/customers/", True) 



# COMMAND ----------

#confirm the Customers sample dataset is your your user specific folder
display(dbutils.fs.ls(f"{bronzePath}/customers"))

# COMMAND ----------

# copy the Customers source data into the bronze folder
dbutils.fs.cp ("/databricks-datasets/retail-org/sales_orders/", f"{bronzePath}/sales_orders/", True) 

# confirm the file is where it should be
display(dbutils.fs.ls(f"{bronzePath}/sales_orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC #Ready to Start Delta Live Tables Pipeline
# MAGIC At this point, we have data in our bronze folder and can now execute the 03.2 - sample-DLT-pipeline-notebook within a delta live table pipeline.

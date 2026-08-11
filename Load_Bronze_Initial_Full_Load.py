# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

# Access the secrests after creating the scope
access_key=dbutils.secrets.get(scope='project-scope',key='adlslogisticfilesa1AccessKey')

#if your file is ADLS GEN2 location and we need to read the file inside the databricks notebook then the below code will be used #

spark.conf.set("fs.azure.account.key.adlslogisticfilesa1.dfs.core.windows.net",access_key)  

# Test the path or connection
dbutils.fs.ls("abfss://logistics@adlslogisticfilesa1.dfs.core.windows.net/Raw/in")

# give variable name to ADLS GEN2 storage path
folderPath = "abfss://logistics@adlslogisticfilesa1.dfs.core.windows.net"

# Test the path or connection of AppointmentDataTest
dbutils.fs.ls('abfss://logistics@adlslogisticfilesa1.dfs.core.windows.net/')

# Test the path or connection of Sales_Data_Prior_Day
dbutils.fs.ls("abfss://logistics@adlslogisticfilesa1.dfs.core.windows.net/Raw/in/Sales_Data_Prior_Day")

# Retrieves the SQL Server name from the secret scope.
serverName = dbutils.secrets.get(scope='project-scope',key='servername')
# Retrieves the database name.
databaseName = dbutils.secrets.get(scope='project-scope',key='databasename')
# Retrieves the SQL login username.
userName = dbutils.secrets.get(scope='project-scope',key='username')
# Retrieves the database password securely.
password = dbutils.secrets.get(scope='project-scope',key='password')

# It reads data from SQL Server table into a Spark DataFrame using JDBC
configDF = spark.read \
  .format("jdbc") \
  .option("url", f"jdbc:sqlserver://{serverName}:1433;database={databaseName}") \
  .option("dbtable", "metadata.[OBJECTS_CONFIGURATION]") \
  .option("user",userName) \
  .option("password", password) \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .load()


# COMMAND ----------

# It helps you see the actual data stored in the Spark DataFrame
print(configDF.collect())

# COMMAND ----------

# make the file path dynamic based on the path it will show the full path for all file
for Row in configDF.collect():
    print(folderPath + '/'+ Row["SOURCE_FILE_PATH"]+ '/'+ Row["SOURCE_FILE_NAME"]+'_*.csv')

# COMMAND ----------

# DBTITLE 1,Cell 4
# Read the csv file of AppointmentDataTest
df1=spark.read \
.format('csv')\
     .option("header",True)\
     .option("inferSchema",True)\
     .load("abfss://logistics@adlslogisticfilesa1.dfs.core.windows.net/Raw/in/AppointmentDataTest/AppointmentDataTest_*.csv")
display(df1)

# Read the csv file of Sales_Data_Prior_Day
df2=spark.read \
     .format('csv')\
     .option("header",True)\
     .option("inferSchema",True)\
     .load("abfss://logistics@adlslogisticfilesa1.dfs.core.windows.net/Raw/in/Sales_Data_Prior_Day/Sales_Data_Prior_Day_*.csv")
display(df2)

# show all the columns of df2
print(df2.columns)

# COMMAND ----------

# iterate through all the column names in the PySpark DataFrame df2 and print them one by one.
for col in df2.columns:
    print(col)

# COMMAND ----------

# set masterpipeline id widget
dbutils.widgets.text("masterPipelineRunID", "")
masterPipelineRunID = dbutils.widgets.get("masterPipelineRunID")

# COMMAND ----------

# DBTITLE 1,Cell 3
    masterPipelineRunID = '123445'

# COMMAND ----------

# give variable name to ADLS GEN2 storage path
folderPath = "abfss://logistics@adlslogisticfilesa1.dfs.core.windows.net"

# COMMAND ----------

configDF.select("BRONZE_TABLE_NAME", "SOURCE_FILE_PATH").show(truncate=False)

# COMMAND ----------

# Function Definition take 2 parameters 
# ObjectName-- target Delta table name 
# filePath-- path of the CSV file

def load_data_into_delta(ObjectName, filePath):
    # Step 1: Read CSV File
    df = spark.read.format("csv").option("header", True).load(filePath)
    # Step 2: Loop Through Every Column .Iterates through every column in the DataFrame.
    for column_name in df.columns:
        # Step 3: Remove Special Characters
        # Replace every character that is not a letter (A–Z, a–z) or number (0–9) with an underscore (_).
        new_column_name = re.sub(r"[^a-zA-Z0-9]+", "_", column_name).strip("_")
        # Step 4: Convert Column Names to Uppercase
        new_column_name = new_column_name.upper()
        # Step 5: Rename the Columns
        df = df.withColumnRenamed(column_name, new_column_name)
    print(f"Table: {ObjectName}")
    print(df.columns)
        # Step 6: Add Audit Columns
        # These are audit columns that help track data lineage and debugging.
    df = df.withColumn("Added_By", lit(masterPipelineRunID)) \
           .withColumn("Added_On", current_timestamp())\
           .withColumn("Modified_By",lit(masterPipelineRunID))\
           .withColumn("Modified_On", current_timestamp())
    # Step 7: Read Target Delta Table 
    target_df = spark.table(ObjectName)
    # Step 8: Loop Through Target Schema
    for field in target_df.schema.fields:
        # Step 9: Cast Data Types
        # CSV values are often read as strings. Casting ensures they match the destination table's data types and prevents schema mismatch errors
        if field.name in df.columns:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    # Step 10: Reorder Columns
    # Spark writes data by column position when saving to a table. Matching the target order avoids incorrect data placement.
    df = df.select(*target_df.columns)
    # Step 11: Write to Delta Table
    df.write.format("delta").mode("append").saveAsTable(ObjectName)

# COMMAND ----------

 #This code is used in a metadata-driven data ingestion pipeline. Instead of hardcoding the table name and file path, it reads those values from a metadata table (configDF) and loads each file dynamically into its corresponding Bronze table.
 
from pyspark.sql.functions import col

try:
    # Reads each row from the metadata DataFrame.
    for row in configDF.collect():
        # Reads the target Delta table name.
        ObjectName = row['BRONZE_TABLE_NAME']
        # Reads the source folder path from the metadata.
        filePath = row['SOURCE_FILE_PATH']
        for path in dbutils.fs.ls(folderPath + '/' + filePath):
        # Builds the complete pa    th and calls your reusable function.
            load_data_into_delta(ObjectName, path.path)
except Exception as e:
    print(f"Error occurred: {e}")
    # Re-throws the exception.
    raise

# COMMAND ----------

print(f"ObjectName : {ObjectName}")
print(f"Source Path : {filePath}")
dbutils.fs.ls(folderPath + '/' + filePath)

# COMMAND ----------

for path in dbutils.fs.ls(folderPath + '/' + filePath):
    print(path.path)
    load_data_into_delta(ObjectName, path.path)

# COMMAND ----------

# DBTITLE 1,Cell 21
# # Move archive the source CSV files after they have been successfully loaded into the Bronze layer to archive folder
# # In a metadata-driven pipeline, once the ingestion is complete, we don't want to process the same files again in the next pipeline run. This code moves the processed files from the source folder to an archive folder.

# # Step 1: Start Exception Handling
# try:
#     # Step 2: Read Metadata Table
#     for row in configDF.collect():
#         # Step 3: Read Source Folder
#             filePath = row['SOURCE_FILE_PATH']
#             # Step 4: Read Archive Folder
#             archivePath = row['ARCHIVE_PATH']
#             # Step 5: Create Source Path
#             sourcePath = f"abfss://logistics@adlslogisticfilesa1.dfs.core.windows.net/{filePath}/"
#             # Step 6: Create Archive Path
#             archivePath = f"abfss://logistics@adlslogisticfilesa1.dfs.core.windows.net/{archivePath}/archive"
#             # for f in dbutils.fs.ls(sourcePath):
#             #     if f.name.endswith(".csv"):
#             #         dbutils.fs.mv(f.path, archivePath + '/' + f.name)
# except Exception as e:
#     print(f"Error occurred: {e}")
#     raise

# COMMAND ----------

# it iwill show the full path
dbutils.fs.ls(folderPath)


# COMMAND ----------

# it will sow the only main relative path

for i in dbutils.fs.ls(folderPath):
    print(i.path)
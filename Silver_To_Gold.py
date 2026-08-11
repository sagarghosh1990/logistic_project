# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("masterPipelineRunID", "")
masterPipelineRunID = dbutils.widgets.get("masterPipelineRunID")

# COMMAND ----------

masterPipelineRunID = '1234'

# COMMAND ----------

# Retrieves the SQL Server name from the secret scope.
serverName = dbutils.secrets.get(scope='project-scope',key='servername')
# Retrieves the database name.
databaseName = dbutils.secrets.get(scope='project-scope',key='databasename')
# Retrieves the SQL login username.
userName = dbutils.secrets.get(scope='project-scope',key='username')
# Retrieves the database password securely.
password = dbutils.secrets.get(scope='project-scope',key='password')


configGoldDF = spark.read \
  .format("jdbc") \
  .option("url", f"jdbc:sqlserver://{serverName}:1433;database={databaseName}") \
  .option("query", "select * from metadata.EDW_CONFIG where IS_RUN=1") \
  .option("user",userName) \
  .option("password", password) \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .load()
display(configGoldDF)

# COMMAND ----------

# This function prepares the column lists and MERGE update statement required for a Delta Lake MERGE operation. Instead of hardcoding column names, it reads them from a metadata table (configGoldDF), making the process metadata-driven.

# ObjectName → Table/object whose column mapping needs to be fetched.
# sourceAlias="s" → Alias used for the source table during MERGE.
# targetAlias="t" → Alias used for the target table.
# skipCols=None → List of columns that should not be updated during MERGE.

def getColumnsForMerge(ObjectName, sourceAlias="s", targetAlias="t", skipCols=None):
    # Step 1: Initialize skip columns
    if skipCols is None:
        skipCols = [ "MODIFIED_ON", "MODIFIED_BY"]

# Step 2: Read metadata 
    row = (
        configGoldDF
        .filter(col("OBJECT_NAME") == ObjectName).orderBy("GOLD_COLUMN_NAMES")
        .select("GOLD_COLUMN_NAMES")
        .first()
    )
    # Step 3: Validate metadata exists .If there is no metadata for the given object, execution stops with an error.
    if not row:
        raise ValueError(f"No mappings found for {ObjectName}")
    # Step 4: Convert comma-separated string into a list
    cols = [c.strip() for c in row["GOLD_COLUMN_NAMES"].split(",")]
    # Step 5: Create empty lists
    src_columns = []
    tgt_columns = []
    merge_pairs = []
    # Step 6: Loop through every column
    for c in cols:
        src_columns.append(f"{sourceAlias}.{c}")
        tgt_columns.append(c)
    # Build UPDATE expressions
        if c not in skipCols and c != "ACCOUNT_KEY":
            merge_pairs.append(f"{targetAlias}.{c} = {sourceAlias}.{c}")
    # Step 7: Remove audit columns from source list
    cols_to_remove = ["ADDED_BY","ADDED_ON","MODIFIED_BY", "MODIFIED_ON"]
    for col_name in cols_to_remove:
        aliased_col = f"{sourceAlias}.{col_name}"
        if aliased_col in src_columns:
            src_columns.remove(aliased_col)
    # Step 8: Convert lists into strings
    sourceColumn = ", ".join(src_columns)
    targetColumn = ", ".join(tgt_columns)
    mergeColumnStatement = ", ".join(merge_pairs)
    # Step 9: Build insert source columns
    insertSourceColumn = sourceColumn + ", s.ADDED_BY, s.ADDED_ON, s.MODIFIED_BY, s.MODIFIED_ON"
    # Step 10: Return values
    return sourceColumn, insertSourceColumn,targetColumn, mergeColumnStatement

# COMMAND ----------

# This function is responsible for loading data into the Gold layer of a Delta Lake using either a Full Load or an Incremental Upsert (MERGE). 
# It is a metadata-driven function because it uses the getColumnsForMerge() function to dynamically generate the column lists required for the SQL statements.

# This function parameter came from bronze to silver  notebook
def load_data_into_gold(ObjectName ,targetTableName, finalDF, operationType, keyColumnName,targetCatalogName,targetSchemaName,    isFullLoad,masterPipelineRunID):
    # step 1 A variable named sourceName is created.Normally this represents the source system.
    sourceName = 'logistic'
    # step 2 This converts the DataFrame into a temporary SQL view.
    finalDF.createOrReplaceTempView("tempView")
    # step 3 This function returns four strings
    srcColumns,insertSourceColumn,targetColumns,mergeColumnStatement = getColumnsForMerge(ObjectName, sourceAlias="s", targetAlias="t", skipCols=None)
    # step 4 Converts the operation type to lowercase.
    if operationType.lower() == "upsert":
        # Full Load --its check full load or not.if isFullLoad = 1 then Full Load is performed.
            if isFullLoad == 1:
                print(f"FULL LOAD STARTED FOR {ObjectName}")
                # Delete all existing records
                delete_query = spark.sql(f"DELETE FROM {targetCatalogName}.{targetSchemaName}.{targetTableName}")
                # Insert all records from tempView
                insert_query = spark.sql(f"""
                                    INSERT INTO {targetCatalogName}.{targetSchemaName}.{targetTableName} ({targetColumns})
                                    SELECT {insertSourceColumn}
                                    FROM tempView s
                                    """)
                # Read Delta History
                df_history = spark.sql(f"describe history {targetCatalogName}.{targetSchemaName}.{targetTableName} limit 1").first()
                # Get inserted rows
                rowsInserted = df_history["operationMetrics"]["numOutputRows"]
                print("Number of Rows Inserted :", rowsInserted)
                print(f"Data Loading for {ObjectName} completed")
                # Incremental Load isFullLoad == 0 then it excutes the else part
            else:
                print(f"Merge Operation Started on {targetTableName}")
                # Build Merge Query.This query performs an Upsert which is update and insert operation.
                mergeQuery = f"""
                MERGE INTO {targetCatalogName}.{targetSchemaName}.{targetTableName} t
                USING tempView s ON s.{keyColumnName}=t.{keyColumnName}
                WHEN MATCHED THEN UPDATE
                SET {mergeColumnStatement},Modified_On = CURRENT_TIMESTAMP, Modified_By = '{masterPipelineRunID}'
                WHEN NOT MATCHED THEN INSERT ({targetColumns}) VALUES ({srcColumns},CURRENT_TIMESTAMP,'{masterPipelineRunID}', CURRENT_TIMESTAMP, '{masterPipelineRunID}')
                """
                print("Executing merge query")
                spark.sql(mergeQuery)
                print(f"Data Loading for {ObjectName} completed")

# COMMAND ----------

# 1.This function is creating a Dimension table DataFrame for the Gold layer from your Silver APPOINTMENT_DATA table.

def dim_appointment_data():
    # 2. Read the Silver table(This reads the Silver table into a PySpark DataFrame.)
    # 3. Select required columns
    appointmentDataDF = spark.read.table('silver.logistic.APPOINTMENT_DATA')\
                        .select(col('YARD_NAME').alias('YARD_NAME'),col('SALES_ORDER_TICKET_ID')
                        .alias('SALES_ORDER_ID'),'CARRIER_NAME')
    # 4. Generate the surrogate key(row_number()-it generates sequential numbers
    # the records are ordered by YARD_NAME before assigning the numbers)
    finalDF = appointmentDataDF.withColumn('APPOINTMENT_DATA_KEY',row_number().over(Window.orderBy(col('YARD_NAME'))))
    # 5. Add audit column: ADDED_ON-it stores the current timestamp
    #                      ADDED_BY- it stores the pipeline run ID.
    # lit()- it converts a Python value into a Spark column value.
    #                      MODIFIED_ON-it stores the current timestamp
    #                      MODIFIED_BY - it tores the pipeline run ID
    finalDF = finalDF.withColumn('ADDED_ON',current_timestamp())\
                     .withColumn('ADDED_BY',lit(masterPipelineRunID))\
                     .withColumn('MODIFIED_ON',current_timestamp())\
                     .withColumn('MODIFIED_BY',lit(masterPipelineRunID))
    # 9. Read the Gold target table(Now the code reads the existing Gold dimension table.)
    targetTableDF = spark.read.table("gold.edw.DIM_APPOINTMENT_DATA")
    # 10. Get the Gold table schema
    targetSchema = targetTableDF.schema
    # 11. Cast DataFrame columns to Gold data types
    finalDF = finalDF.select(*[ col(field.name).cast(field.dataType).alias(field.name) for field in targetSchema])
    return finalDF

# COMMAND ----------

# DBTITLE 1,fact_sales function
def fact_sales():
    salesDF = spark.read.table('silver.logistic.SALES_DATA_PRIOR_DAY')\
                .select(col('YARD_ID').alias('YARD_ID')\
                ,'YARD_KEY'
                ,'COMMODITY_NAME'
                ,'PRICE'
                ,'SELL_PRICE'
                ,'INVOICE_TOTAL','YARD')
    dimAppointmentDataDF = spark.read.table("gold.EDW.DIM_APPOINTMENT_DATA").select("YARD_NAME", "APPOINTMENT_DATA_KEY")
    finalDF = salesDF.alias("s").join(dimAppointmentDataDF.alias("da"), col("s.YARD") == col("da.YARD_NAME"), "left").select("s.*", "da.APPOINTMENT_DATA_KEY").withColumn("SALES_KEY",row_number().over(Window.orderBy("YARD_ID", "YARD_KEY", "COMMODITY_NAME")))
    finalDF = finalDF.withColumn('ADDED_ON',current_timestamp())\
                     .withColumn('ADDED_BY',lit(masterPipelineRunID))\
                     .withColumn('MODIFIED_ON',current_timestamp())\
                     .withColumn('MODIFIED_BY',lit(masterPipelineRunID))
    targetTableDF = spark.read.table("GOLD.EDW.FACT_SALES")
    targetSchema = targetTableDF.schema
    finalDF = finalDF.select(*[ col(field.name).cast(field.dataType).alias(field.name) for field in targetSchema])
    return finalDF

# COMMAND ----------

try:
    for row in configGoldDF.collect():
            ObjectName = row['OBJECT_NAME']
            sourceName = 'logistic'
            targetTableName = row['OBJECT_NAME']
            operationType = row['OPERATION_TYPE']
            keyColumnName = row['GOLD_KEY_COLUMN_NAME']
            targetCatalogName = row['GOLD_CATALOG_NAME']
            targetSchemaName = row['GOLD_SCHEMA_NAME']
            isFullLoad = row['IS_FULL_LOAD']
            if ObjectName.lower() == 'dim_appointment_data':
                finalDF = dim_appointment_data()
                load_data_into_gold(ObjectName ,targetTableName, finalDF, operationType, keyColumnName,targetCatalogName,targetSchemaName,    isFullLoad,masterPipelineRunID)
            elif ObjectName.lower() == 'fact_sales':
                finalDF = fact_sales()
                load_data_into_gold(ObjectName ,targetTableName, finalDF, operationType, keyColumnName,targetCatalogName,targetSchemaName,    isFullLoad,masterPipelineRunID)
            else:
                print(f"Invalid Object Name {ObjectName}")
except Exception as e:
    print(f"Error occurred: {e}")
    raise

# COMMAND ----------

# DBTITLE 1,Archive source files to ADLS
# ADLS Gen2 storage path — constant for this workspace
access_key = dbutils.secrets.get(scope='project-scope', key='adlslogisticfilesa1AccessKey')
spark.conf.set("fs.azure.account.key.adlslogisticfilesa1.dfs.core.windows.net", access_key)
storagePath = "abfss://logistics@adlslogisticfilesa1.dfs.core.windows.net"

# Load file path metadata from OBJECTS_CONFIGURATION
# configGoldDF (metadata.EDW_CONFIG) has no SOURCE_FILE_PATH or ARCHIVE_PATH columns —
# those live in metadata.OBJECTS_CONFIGURATION, same source the bronze archive step uses.
serverName   = dbutils.secrets.get(scope='project-scope', key='servername')
databaseName = dbutils.secrets.get(scope='project-scope', key='databasename')
userName     = dbutils.secrets.get(scope='project-scope', key='username')
password     = dbutils.secrets.get(scope='project-scope', key='password')

configGoldDF = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver://{serverName}:1433;database={databaseName}") \
    .option("dbtable", "metadata.[OBJECTS_CONFIGURATION]") \
    .option("user", userName) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

try:
    for config_row in configGoldDF.collect():
        sourcePath  = f"{storagePath}/{config_row['SOURCE_FILE_PATH']}/"
        archiveDest = f"{storagePath}/{config_row['ARCHIVE_PATH']}/archive"

        try:
            source_files = dbutils.fs.ls(sourcePath)
        except Exception:
            print(f"Source folder not found or empty, skipping: {sourcePath}")
            continue

        archived = 0
        for f in source_files:
            if f.name.endswith(".csv"):
                dbutils.fs.mkdirs(archiveDest)
                dbutils.fs.mv(f.path, f"{archiveDest}/{f.name}")
                print(f"Archived: {f.name} → {archiveDest}")
                archived += 1

        if archived == 0:
            print(f"No CSV files to archive in: {sourcePath}")

except Exception as e:
    print(f"Archive failed: {str(e)}")
    raise

# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Cell 2
masterPipelineRunID = dbutils.widgets.get("masterPipelineRunID")

# COMMAND ----------

# Retrieves the SQL Server name from the secret scope.
serverName = dbutils.secrets.get(scope='project-scope',key='servername')
# Retrieves the database name.
databaseName = dbutils.secrets.get(scope='project-scope',key='databasename')
# Retrieves the SQL login username.
userName = dbutils.secrets.get(scope='project-scope',key='username')
# Retrieves the database password securely.
password = dbutils.secrets.get(scope='project-scope',key='password')

# It reads configurationdata from SQL Server table into a Spark DataFrame using JDBC
configSilverDF = spark.read \
  .format("jdbc") \
  .option("url", f"jdbc:sqlserver://{serverName}:1433;database={databaseName}") \
  .option("query", "select * from metadata.OBJECTS_CONFIGURATION where IS_RUN = 1") \
  .option("user",userName) \
  .option("password", password) \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .load()



# COMMAND ----------

# It load the column mapping metadata from SQL Server into a Spark DataFrame
columnMappingDF = spark.read \
  .format("jdbc") \
  .option("url", f"jdbc:sqlserver://{serverName}:1433;database={databaseName}") \
  .option("dbtable", "metadata.OBJECTS_COLUMN_MAPPING") \
  .option("user",userName) \
  .option("password", password) \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .load()

#it is used to create a temporary SQL view from the Spark DataFrame so that you can query it using Spark SQL instead of PySpark DataFrame methods.
columnMappingDF.createOrReplaceTempView("fieldMappingView") 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fieldMappingView where OBJECT_NAME ='APPOINTMENT_DATA'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 6
# column mapping
# it will show SOURCE_COLUMN_NAME,TARGET_COLUMN_NAME columns from METADATA.OBJECTS_COLUMN_MAPPING table
# Read the source column names from the metadata table.
# Read the corresponding target column names.
# Create a dictionary mapping Source → Target.
# Rename the columns in the DataFrame dynamically.

# ObjectName → The table currently being processed (e.g., "SALES_DATA_PRIOR_DAY").
# rawDF → The DataFrame read from the CSV file.
def create_column_mapping_dict(ObjectName:str,rawDF:DataFrame):
    # .rdd.flatMap is used for Convert it to Python list
    source_cols = (
        columnMappingDF
        .filter(col("OBJECT_NAME")==f'{ObjectName}')
        .select("SOURCE_COLUMN_NAME")
        .rdd.flatMap(lambda x: x)
        .collect()
        )
    target_cols = (
        columnMappingDF
        .filter(col("OBJECT_NAME")==f'{ObjectName}')
        .select("TARGET_COLUMN_NAME")
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    renameDict = dict(zip(source_cols, target_cols))
    print(renameDict)
    # Start with the original DataFrame
    bronzeDF = rawDF   
    # Loop through dictionary
    for old_column,new_column in renameDict.items():
        # Rename columns
        bronzeDF = bronzeDF.withColumnRenamed(old_column,new_column)
    return bronzeDF

# COMMAND ----------

# DBTITLE 1,Cell 7
# handling nulls
# This function is intended to replace NULL values in specific columns with default values defined in the metadata table
# ObjectName → Name of the table being processed (e.g., "SALES_DATA_PRIOR_DAY").
# rawDF → The DataFrame whose NULL values need to be handled

def handle_nulls(ObjectName , rawDF):
    # Step 1: Get target column names
    # Only rows where VALUES is not NULL & Only rows for the given ObjectName
    targetColumnList = (
                    columnMappingDF
                    .filter((col("VALUES")
                    .isNotNull()) & (col('OBJECT_NAME')==f'{ObjectName}'))
                    .select("TARGET_COLUMN_NAME")
                    .rdd.flatMap(lambda x: x)
                    .collect()
                    )
    # Step 2: Get replacement values
    nullValueList =( 
                    columnMappingDF
                    .filter((col("VALUES")
                    .isNotNull()) & (col('OBJECT_NAME')==f'{ObjectName}'))
                    .select("VALUES")
                    .rdd.flatMap(lambda x: x)
                    .collect()
                    )
    # Step 3: Create a dictionary for the column names and replacement values
    HandlingNullDict = dict(zip(targetColumnList, nullValueList))
    # Step 4: Replace NULL values
    bronzeDF = rawDF
    for column, value in HandlingNullDict.items():
        bronzeDF = bronzeDF.fillna({column:value})
    return bronzeDF

# COMMAND ----------

# DBTITLE 1,Cell 8
#type casting
# Changes column data types based on metadata in OBJECTS_COLUMN_MAPPING.
# Handles multiple date formats via try_to_date (returns NULL on mismatch, never throws).
def data_type_casting(ObjectName, rawDF):
    dataTypeColList = (
        columnMappingDF
        .filter(col('OBJECT_NAME') == f'{ObjectName}')
        .select('TARGET_COLUMN_NAME')
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    dataTypeList = (
        columnMappingDF
        .filter(col('OBJECT_NAME') == f'{ObjectName}')
        .select('COLUMN_DATATYPE')
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    dataTypeCastDict = dict(zip(dataTypeColList, dataTypeList))
    bronzeDF = rawDF
    for col_name, col_type in dataTypeCastDict.items():
        if col_name not in bronzeDF.columns:
            continue
        if col_type.lower() == 'date':
            # Try each known date format in order; first non-NULL result wins
            bronzeDF = bronzeDF.withColumn(
                col_name,
                coalesce(
                    try_to_date(col(col_name), 'M/d/yyyy'),
                    try_to_date(col(col_name), 'MM-dd-yyyy'),
                    try_to_date(col(col_name), 'yyyy-MM-dd'),
                    try_to_date(regexp_replace(col(col_name), 'T.*', ''), 'yyyy-MM-dd')
                )
            )
        elif col_type.lower() in ('timestamp', 'timestamp_ntz'):
            # Use character-class regex to strip [TimezoneName] suffix without backslashes
            bronzeDF = bronzeDF.withColumn(
                col_name,
                to_timestamp(regexp_replace(col(col_name), '[[].*?[]]', ''), "yyyy-MM-dd'T'HH:mm:ss.SSXXX")
            )
        else:
            bronzeDF = bronzeDF.withColumn(col_name, expr(f"try_cast({col_name} as {col_type})"))
    return bronzeDF

# COMMAND ----------

# TThis function is used to dynamically generate the column lists required for a SQL MERGE statement. Instead of hardcoding column names, it reads them from the metadata table (fieldMappingView). This makes the code metadata-driven, so if a new column is added to the metadata, the merge logic automatically includes it.


def getColumnsForMerge(sourceName, ObjectName, sourceAlias="s", targetAlias="t", skipCols=None):
    # Step 1: Default Skip Columns
    # If the user doesn't pass a list of columns to skip, these audit columns are skipped automatically.
    if skipCols is None:
        skipCols = ["Added_On", "Added_By", "Modified_On", "Modified_By"]

    # It reads metadata from fieldMappingView
    rows = spark.sql(f"""
                SELECT TARGET_COLUMN_NAME
                FROM fieldMappingView
                WHERE lower(SOURCE_NAME) = lower('{sourceName}')
                AND lower(OBJECT_NAME) = lower('{ObjectName}')
                            ORDER BY TARGET_COLUMN_NAME
                        """).collect()
    
    # Step 3: Check if Metadata Exists .This prevents the merge from running without valid metadata.
    if not rows:
        raise ValueError(f"No mappings found for {sourceName} -> {ObjectName}")

# Step 4: Create Empty Lists
    src_columns = []
    tgt_columns = []
    merge_pairs = []
# Step 5: Loop Through Each Metadata Row - Each iteration processes one column.
    for r in rows:
        # Step 6: Source Column hete source column name and target column name are identical.
        dest_col = r["TARGET_COLUMN_NAME"]
        src_col  = r["TARGET_COLUMN_NAME"]
    # Step 7: Skip Audit Columns. Since it is in skipCols, the loop skips it.No lists are updated.
        if dest_col in skipCols:
            continue
        # Step 8: Build Source Columns
        src_columns.append(f"{sourceAlias}.{src_col}")
        # Step 9: Build Target Columns
        tgt_columns.append(dest_col)
        # Step 10: Build MERGE Assignments
        merge_pairs.append(f"{targetAlias}.{dest_col} = {sourceAlias}.{src_col}")
    
    # audit_fields = ["Added_On", "Added_By", "Modified_On", "Modified_By"]

    # merge_pairs.append(f"{targetAlias}.ADDED_ON = {sourceAlias}.ADDED_ON")
    # merge_pairs.append(f"{targetAlias}.ADDED_BY = {sourceAlias}.ADDED_BY")

    # Step 11: Convert Lists to Strings
    sourceColumn = ", ".join(src_columns)
    targetColumn = ", ".join(tgt_columns )
    mergeColumnStatement = ", ".join(merge_pairs)
    # Step 12: Add Audit Columns.These columns will be included during the INSERT part of the MERGE.
    targetColumn = targetColumn + ", ADDED_BY, ADDED_ON, MODIFIED_BY, MODIFIED_ON"
    # Step 15: Return Values .The function returns three strings.src_columns,tgt_columns,merge_pairs
    return sourceColumn, targetColumn, mergeColumnStatement 

# COMMAND ----------

# DBTITLE 1,Cell 11
# This function is responsible for loading data from the Bronze layer to the Silver layer. It supports both:
# Full Load – Deletes all existing records and reloads the table.
# Incremental Load (Upsert)- Updates existing records and inserts new records using a MERGE statement.

# step 1-- Function definition
def load_data_into_silver(
    sourceName,objectName,targetTableName,bronzeDF,operationType,keyColumnName,isFullLoad,masterPipelineRunID
):

    # Create temporary view
    bronzeDF.createOrReplaceTempView("tempView")
    # Get metadata columns
    srcColumns, targetColumns, mergeColumnStatement = getColumnsForMerge(
        sourceName,
        objectName,
        sourceAlias="s",
        targetAlias="t"
    )

    # Get columns from Bronze DataFrame
    sourceCols = {c.upper() for c in bronzeDF.columns}
    # Define audit columns
    auditCols = {"ADDED_BY", "ADDED_ON", "MODIFIED_BY", "MODIFIED_ON"}
    # Convert column strings into lists (.split(",") is used to convert the column strings into lists)
    srcList = [c.strip() for c in srcColumns.split(",")]
    tgtList = [c.strip() for c in targetColumns.split(",")]
    # Separate data columns and audit columns
    dataTargetCols = [c for c in tgtList if c.upper() not in auditCols]
    auditTargetCols = [c for c in tgtList if c.upper() in auditCols]
    # Filter columns that actually exist in Bronze
    filtered = [
        (sc, tc)
        for sc, tc in zip(srcList, dataTargetCols)
        if sc.split(".")[-1].upper() in sourceCols
    ]
    # Rebuild source columns
    srcColumns = ", ".join(sc for sc, _ in filtered)
    # Rebuild target columns
    targetColumns = ", ".join([tc for _, tc in filtered] + auditTargetCols)
    # Build UPDATE statement (Upsert)- Updates existing records and inserts new records using a MERGE statement)
    mergeColumnStatement = ", ".join(
        f"t.{tc} = {sc}"
        for sc, tc in filtered
    )

    # Only UPSERT supported
    
    # Check operation type
    if operationType.lower() != "upsert":
        return
    
    # FULL LOAD
    # If isFullLoad is 1, perform a full load.
    if isFullLoad == 1:
        # Truncate Silver table(This removes all existing records from the Silver table.)
        spark.sql(f"TRUNCATE TABLE {targetTableName}")
        # Insert Bronze data into Silver table
        spark.sql(f"""
            INSERT INTO {targetTableName}
            ({targetColumns})
            SELECT {srcColumns}
            FROM tempView s
        """)
        # Get number of inserted rows (DESCRIBE HISTORY gets the latest operation performed on the table.)
        history = spark.sql(
            f"DESCRIBE HISTORY {targetTableName} LIMIT 1"    
        ).first()
        # gets the number of output rows.
        rowsInserted = history["operationMetrics"]["numOutputRows"]
        # prints statement
        print(f"Rows Inserted : {rowsInserted}")
        # returns due to stops the function because the full load is complete.
        return
    
    # INCREMENTAL LOAD (MERGE)
    # This is basically an UPSERT. (If record exists → UPDATE)&(If record doesn't exist → INSERT)
    # Build merge condition
    mergeCondition = " AND ".join(
        f"s.{key.strip()} = t.{key.strip()}"
        for key in keyColumnName.split(",")
    )
    # Get Silver table columns(This gets the columns available in the Silver table.)
    silverColumns = {
        col.name.upper()
        for col in spark.catalog.listColumns(targetTableName)
    }

    # If MODIFIED_ON exists in Silver table, update MODIFIED_ON and MODIFIED_BY columns. 
    if "MODIFIED_ON" in silverColumns:

        updateAudit = f"""
            ,
            t.MODIFIED_ON = CURRENT_TIMESTAMP,
            t.MODIFIED_BY = '{masterPipelineRunID}'
        """
        # Audit values for INSERT statement (This is used to insert the MODIFIED_BY and MODIFIED_ON columns)
        insertAuditValues = f"""
            '{masterPipelineRunID}',
            CURRENT_TIMESTAMP,
            '{masterPipelineRunID}',
            CURRENT_TIMESTAMP
        """
    # If MODIFIED_ON does NOT exist No update audit fields will be added to the INSERT statement
    else:

        updateAudit = ""

        insertAuditValues = f"""
            '{masterPipelineRunID}',
            CURRENT_TIMESTAMP
        """
        # This removes: MODIFIED_BY,MODIFIED_ON from the target column list.
        targetColumns = ", ".join(
            c
            for c in targetColumns.split(",")
            if c.strip().upper() not in ("MODIFIED_BY", "MODIFIED_ON")
        )
# Build the MERGE query
    mergeQuery = f"""
        MERGE INTO {targetTableName} t
        USING tempView s
        ON {mergeCondition}

        WHEN MATCHED THEN
        UPDATE SET
            {mergeColumnStatement}
            {updateAudit}

        WHEN NOT MATCHED THEN
        INSERT ({targetColumns})
        VALUES ({srcColumns}, {insertAuditValues})
    """
    # Then print the query and execute it 
    print(f"Executing merge query for {objectName}")
    # It executes the generated SQL.
    spark.sql(mergeQuery)

# COMMAND ----------

# for example- 
# MERGE INTO silver.customer t
# USING tempView s
# ON s.CUSTOMER_ID = t.CUSTOMER_ID
# WHEN MATCHED THEN
# UPDATE SET
#         t.CUSTOMER_NAME = s.CUSTOMER_NAME,
#         t.CITY = s.CITY,
#         t.MODIFIED_ON = CURRENT_TIMESTAMP,
#         t.MODIFIED_BY = 'pipeline-1234'
# WHEN NOT MATCHED INSERT VALUE
# Bronze CUSTOMER_ID = 200
# Silver CUSTOMER_ID = 200 → doesn't exist
# then it is a NOT MATCHED record. & The record will be inserted into Silver.

# COMMAND ----------

# DBTITLE 1,Cell 12
# This code is the main driver/orchestration code that reads your Silver configuration metadata and then processes each Bronze table one by one before calling the load_data_into_silver() function.

try:
    # 1-configSilverDF is your Silver metadata/configuration DataFrame
    # 2-.collect() brings all rows of configSilverDF to the driver.
    for row in configSilverDF.collect():
            # 3. Get Object Name
            ObjectName = row['OBJECT_NAME']
            # 4. Get Source Name
            sourceName = row['SOURCE_NAME']
            # 5. Get Silver target table
            targetTableName = row['SILVER_TABLE_NAME']
            # 6. Get operation type UPSERT or FULLLOAD
            operationType = row['OPERATION_TYPE']
            # 7. Get key column (This is the business/primary key used for the MERGE.)
            keyColumnName = row['KEY_COLUMN_NAME']
            # 8. Get full-load flag(This tells the function whether the current table should be processed as a full load or incremental load)
            isFullLoad = row['IS_FULL_LOAD']
            # 9. Get Bronze table name(This tells Spark which Bronze table should be read)
            bronzeTableName = row['BRONZE_TABLE_NAME']
            # 10. Read Bronze table(This reads the Bronze Delta table into a PySpark DataFrame.)
            bronzeDF = spark.table(f"{bronzeTableName}")
            # 11. Create column mapping(This calls your custom function)
            bronzeDF = create_column_mapping_dict(ObjectName, bronzeDF)
            # 12. Handle NULL values(This calls your custom null-handling function)
            bronzeDF = handle_nulls(ObjectName, bronzeDF)
            # 13. Perform data type casting(It converts the Bronze columns into the data types expected by Silver)
            bronzeDF = data_type_casting(ObjectName, bronzeDF)
            # 14. Remove duplicate records(It keeps only one record for each key.If multiple source records match the same target record, Delta MERGE can fail or produce ambiguous results.)
            bronzeDF = bronzeDF.dropDuplicates([k.strip() for k in keyColumnName.split(',')])
            # 15. Get Silver table columns(This reads the column names from the Silver target table)
            _tgt_cols = {c.name.upper() for c in spark.catalog.listColumns(targetTableName)}
            # 16. Select only columns available in Silver(it gives only matching records)
            bronzeDF = bronzeDF.select([c for c in bronzeDF.columns if c.upper() in _tgt_cols])
            # 17. Load the data into Silver(processed Bronze DataFrame is passed to the function)
            load_data_into_silver(sourceName,ObjectName ,targetTableName, bronzeDF, operationType, keyColumnName,isFullLoad,masterPipelineRunID)
except Exception as e:
    print(f"Error occurred: {e}")
    raise
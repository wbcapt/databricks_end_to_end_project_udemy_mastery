# Databricks notebook source
# MAGIC %md
# MAGIC #Creating Delta Tables
# MAGIC
# MAGIC This is the recommended way to create data in Databricks because it will persist within the hive_metastore as a default option, and if you have unity catalog it will persist there
# MAGIC
# MAGIC Delta tables are the default table type created. It means they are backed by the Delta Format
# MAGIC
# MAGIC To create any table, you need to create a schema first and you can also specify the security on a table

# COMMAND ----------

# MAGIC %md
# MAGIC DDL & DML Statements
# MAGIC https://docs.databricks.com/en/sql/language-manual/index.html 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create a Schema (HIVE_METASTORE (Depricated Pattern))
# MAGIC
# MAGIC This will be registered in the hive_metastore (Depricated pattern). If you do enable Unity catalog later, it will appear in the hive_metastore catalog unity catalog 3 level namespace.
# MAGIC
# MAGIC Remember that hive metastores do not actually have the construct of a catalog
# MAGIC
# MAGIC Hive metastore allows you to declare a LOCATION for a schema during creation. This functions similarly to Unity Catalog managed storage locations, with the following behavioral differences:
# MAGIC     
# MAGIC     If you don’t provide a location, the default location /user/hive/warehouse/<schema-name> is used. 
# MAGIC     This location is on the DBFS root, which is not recommended for storing any production data.
# MAGIC
# MAGIC     The provided path can be any cloud storage location available to the user that creates the schema, including cloud URIs, DBFS root, and DBFS mounts.
# MAGIC     
# MAGIC     Access to the location is not managed by Hive metastore.
# MAGIC     
# MAGIC     Deleting a schema in the Hive metastore causes all files in that schema location to be deleted recursively, regardless of the table type (managed or external).
# MAGIC
# MAGIC To avoid accidental data loss, Databricks recommends the following when you work with Hive metastore schema locations:
# MAGIC     Don’t assign a schema location that already contains data.
# MAGIC     
# MAGIC     Don’t create an external table in a schema location.
# MAGIC     
# MAGIC     Don’t share a location among multiple schemas.
# MAGIC     
# MAGIC     Don’t assign a schema location that overlaps another schema location. In other words, don’t use a path that is a child of another schema location.
# MAGIC     
# MAGIC     Don’t assign a schema location that overlaps the location of an external table

# COMMAND ----------

# MAGIC %md
# MAGIC ###defualt location, however I called it defult by accident. Will update below
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Save in the default Hive Metastore Location
# MAGIC */
# MAGIC CREATE SCHEMA IF NOT EXISTS buster_schema_defult_location

# COMMAND ----------

# MAGIC %md
# MAGIC ###specified location

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create A Table (HIVE METASTORE)
# MAGIC
# MAGIC https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-hiveformat.html 
# MAGIC Default Syntax
# MAGIC
# MAGIC CREATE [ EXTERNAL ] TABLE [ IF NOT EXISTS ] table_identifier
# MAGIC     [ ( col_name1[:] col_type1 [ COMMENT col_comment1 ], ... ) ]
# MAGIC     [ COMMENT table_comment ]
# MAGIC     [ PARTITIONED BY ( col_name2[:] col_type2 [ COMMENT col_comment2 ], ... )
# MAGIC         | ( col_name1, col_name2, ... ) ]
# MAGIC     [ ROW FORMAT row_format ]
# MAGIC     [ STORED AS file_format ]
# MAGIC     [ LOCATION path ]
# MAGIC     [ TBLPROPERTIES ( key1=val1, key2=val2, ... ) ]
# MAGIC     [ AS select_statement ]
# MAGIC
# MAGIC row_format:
# MAGIC     : SERDE serde_class [ WITH SERDEPROPERTIES (k1=v1, k2=v2, ... ) ]
# MAGIC     | DELIMITED [ FIELDS TERMINATED BY fields_terminated_char [ ESCAPED BY escaped_char ] ]
# MAGIC         [ COLLECTION ITEMS TERMINATED BY collection_items_terminated_char ]
# MAGIC         [ MAP KEYS TERMINATED BY map_key_terminated_char ]
# MAGIC         [ LINES TERMINATED BY row_terminated_char ]
# MAGIC         [ NULL DEFINED AS null_char ]

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Create a table in the schema specified*/
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS buster_schema_defult_location.my_delta_table
# MAGIC (
# MAGIC   Education_Level STRING
# MAGIC  ,Line_Number INT
# MAGIC  ,Employed INT
# MAGIC  ,Unemployed INT
# MAGIC  ,Industry STRING
# MAGIC  ,Gender STRING
# MAGIC  ,Date_Inserted STRING
# MAGIC  ,dense_rank INT
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Differences

# COMMAND ----------

# MAGIC %md
# MAGIC ##What makes a managed table diffrent in unity catalog than hive_metastore
# MAGIC
# MAGIC First off, both use the Delta format by default. However, only Managed Tables in the Unity Catalog have the performance benefits
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Examine the Delta Log
# MAGIC
# MAGIC The delta log is in JSON format

# COMMAND ----------

#examine the table created

dbutils.fs.ls('dbfs:/user/hive/warehouse/buster_schema_defult_location.db/my_delta_table')

# COMMAND ----------

#examine the delta log created

dbutils.fs.ls('dbfs:/user/hive/warehouse/buster_schema_defult_location.db/my_delta_table/_delta_log')

delta_log_path = 'dbfs:/user/hive/warehouse/buster_schema_defult_location.db/my_delta_table/_delta_log'

# COMMAND ----------

# MAGIC %sql
# MAGIC /*creating a view to view the data*/
# MAGIC
# MAGIC CREATE TEMPORARY VIEW my_delta_table_view
# MAGIC USING json
# MAGIC OPTIONS (path 'dbfs:/user/hive/warehouse/buster_schema_defult_location.db/my_delta_table/_delta_log/00000000000000000000.json');
# MAGIC
# MAGIC SELECT * FROM my_delta_table_view;

# COMMAND ----------

# MAGIC %md
# MAGIC #Insert Records Into A Table

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO buster_schema_defult_location.my_delta_table
# MAGIC VALUES
# MAGIC     ('Bachelor', 1, 4500, 500, 'IT', 'Male', '2023-07-12',  1),
# MAGIC     ('Master', 2, 6500, 500, 'Finance', 'Female', '2023-07-12', 2),
# MAGIC     ('High School', 3, 3500, 500, 'Retail', 'Male', '2023-07-12', 3),
# MAGIC     ('PhD', 4, 5500, 500, 'Healthcare', 'Female', '2023-07-12', 4);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Examine the delta log

# COMMAND ----------

display(dbutils.fs.ls(delta_log_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Pro Tip: to examine a JSON Schema using SQL use the back parenthesis or "`" top left of your keyboard

# COMMAND ----------

# MAGIC %sql
# MAGIC /**/
# MAGIC SELECT
# MAGIC *
# MAGIC FROM
# MAGIC TEXT.`dbfs:/user/hive/warehouse/buster_schema_defult_location.db/my_delta_table/_delta_log/00000000000000000001.json`

# COMMAND ----------

# MAGIC %md
# MAGIC ##Update data in a table
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE buster_schema_defult_location.my_delta_table
# MAGIC SET Industry = 'Finance'
# MAGIC WHERE Education_level = 'PhD'

# COMMAND ----------

# MAGIC %md
# MAGIC ###This created a new record in the delta_log directory 002.json

# COMMAND ----------

#examine the delta_log directory

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/buster_schema_defult_location.db/my_delta_table/_delta_log'))

# COMMAND ----------

# MAGIC %sql
# MAGIC /*examine the data*/
# MAGIC
# MAGIC SELECT
# MAGIC *
# MAGIC FROM
# MAGIC TEXT.`dbfs:/user/hive/warehouse/buster_schema_defult_location.db/my_delta_table/_delta_log/00000000000000000002.json`

# COMMAND ----------

# MAGIC %md
# MAGIC ##Query table after update

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM buster_schema_defult_location.my_delta_table

# COMMAND ----------

# MAGIC %md
# MAGIC ##Delete a record

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM buster_schema_defult_location.my_delta_table
# MAGIC WHERE dense_rank =2

# COMMAND ----------

# MAGIC %md
# MAGIC ###This should have also ceated a new transaction log/JSON file in the delta log directory, 003JSON

# COMMAND ----------

#confirmed now a 003 JSON File
display(dbutils.fs.ls(delta_log_path))

# COMMAND ----------

#display the parquet file
dbutils.fs.ls('dbfs:/user/hive/warehouse/buster_schema_defult_location.db/my_delta_table')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Here we can see that a new parquet file is being used

# COMMAND ----------

#this shows modification time as most recent to least recent top to 
display(dbutils.fs.ls('dbfs:/user/hive/warehouse/buster_schema_defult_location.db/my_delta_table'))

# COMMAND ----------

from pyspark.sql.functions import col, from_unixtime

# Convert modificationTime from milliseconds to seconds, then to timestamp
df = spark.createDataFrame(dbutils.fs.ls('dbfs:/user/hive/warehouse/buster_schema_defult_location.db/my_delta_table')) \
    .select("path", from_unixtime(col("modificationTime") / 1000).alias("modificationDateTime"))

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###showing how parquet file relates to Delta_Log JSON file created
# MAGIC
# MAGIC If you query the JSON 3 file created in the delta log folder, we can see there was a remove operation of the record: part-00000-637ed9f3-38b5-445c-8e81-6e287872fc30-c000.snappy.parquet file which was the original record
# MAGIC
# MAGIC And then, part-00000-13f98ca7-e8fa-40ab-9aee-f0773341ff20-c000.snappy.parquet was added

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM
# MAGIC TEXT.`dbfs:/user/hive/warehouse/buster_schema_defult_location.db/my_delta_table/_delta_log/00000000000000000003.json`

# COMMAND ----------

# MAGIC %md
# MAGIC #Create a Delta Table using PySpark by reading a Parquet file then converting to Delta

# COMMAND ----------

# MAGIC %md
# MAGIC ##First need to create the connection

# COMMAND ----------



service_credential = dbutils.secrets.get(scope="azure-key-vault-secret-scope-new",key="databricks-free-trial-secret")

spark.conf.set("fs.azure.account.auth.type.datbrudmymstryprjstg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.datbrudmymstryprjstg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.datbrudmymstryprjstg.dfs.core.windows.net", "638478f8-a4fc-41be-9e9d-b3abe0e988ff")
spark.conf.set("fs.azure.account.oauth2.client.secret.datbrudmymstryprjstg.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.datbrudmymstryprjstg.dfs.core.windows.net", "https://login.microsoftonline.com/98898401-cb66-46bd-9820-017f5f98d1c6/oauth2/token")

# COMMAND ----------

az_stg_source = "abfss://test-connection-container@datbrudmymstryprjstg.dfs.core.windows.net/"
az_dir_source = f'{az_stg_source}/delta_experimentation/my_delta_lake_folder/'

# COMMAND ----------

# MAGIC %md
# MAGIC ##Load the file into a dataframe

# COMMAND ----------

#load the parquet folder for experimentation
df = (spark.read.format('parquet').load(f'{az_stg_source}/ParquetFolder/'))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Now write to delta table
# MAGIC
# MAGIC Syntax is df.write.format('delta').mode('overwrite').saveAsTable('[database_name].[TableName])

# COMMAND ----------

#Write parquet file to delta table. Syntax here is saveAsTable (database_name.Table_Name)
df.write.format('delta').mode('overwrite').saveAsTable('buster_schema_defult_location.my_delta_spark_tbl')

# COMMAND ----------

# MAGIC %md
# MAGIC ###lets view the operation in the parquet file

# COMMAND ----------

location_my_delta_spark_tbl = 'dbfs:/user/hive/warehouse/buster_schema_defult_location.db/my_delta_spark_tbl'

# COMMAND ----------

display(dbutils.fs.ls(location_my_delta_spark_tbl))

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/buster_schema_defult_location.db/my_delta_spark_tbl/_delta_log/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read contents of JSON file in delta log using spark

# COMMAND ----------

#display contents of the JSON file for the log
display(spark.read.format('text').load(f'{location_my_delta_spark_tbl}/_delta_log/*.json'))
                                          

# COMMAND ----------

# MAGIC %md
# MAGIC ##Display the delta file contents

# COMMAND ----------

display(spark.read.format('delta').load('dbfs:/user/hive/warehouse/buster_schema_defult_location.db/my_delta_spark_tbl'))

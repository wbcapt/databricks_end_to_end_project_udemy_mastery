# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.deltadbstg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.deltadbstg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.deltadbstg.dfs.core.windows.net", "428e6361-2896-4a81-ba91-244293ad41d2")
spark.conf.set("fs.azure.account.oauth2.client.secret.deltadbstg.dfs.core.windows.net", "4EF8Q~iHz-YEI2MNXB89HvmPYAmBpoL2MwLLGc_U")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.deltadbstg.dfs.core.windows.net", "https://login.microsoftonline.com/2d5dcdaa-3dc8-4853-a43b-ac7af2182644/oauth2/token")

# COMMAND ----------

source = 'abfss://test@deltadbstg.dfs.core.windows.net/'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Reading data from CSV file

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DateType,FloatType,DoubleType

schema1 = StructType([
    StructField('Education_Level',StringType()),
    StructField('Line_Number',IntegerType()),
    StructField('Employed',IntegerType()),
    StructField('Unemployed',IntegerType()),
    StructField('Industry',StringType()),
    StructField('Gender',StringType()),
    StructField('Date_Inserted',StringType()),
    StructField('dense_rank',IntegerType())
])

# COMMAND ----------

df = (spark.read.format('csv')
            .option('header','true')
            .schema(schema1)
            .load(f'{source}/files/*.csv'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Writing data to a Delta Table

# COMMAND ----------

df.write.format('delta').saveAsTable('delta.VersionTable')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.VersionTable

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Inserting records in the table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO `delta`.VersionTable
# MAGIC VALUES
# MAGIC     ('Bachelor', 1, 4500, 500, 'Networking', 'Male', '2023-07-12',  1),
# MAGIC     ('Master', 2, 6500, 500, 'Networking', 'Female', '2023-07-12', 2),
# MAGIC     ('High School', 3, 3500, 500, 'Networking', 'Male', '2023-07-12', 3),
# MAGIC     ('PhD', 4, 5500, 500, 'Networking', 'Female', '2023-07-12', 4);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY `delta`.VersionTable

# COMMAND ----------

dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/versiontable')

# COMMAND ----------

dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/versiontable/_delta_log')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading 000.json 

# COMMAND ----------

display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/versiontable/_delta_log/00000000000000000000.json'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading 0001.json

# COMMAND ----------

display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/versiontable/_delta_log/00000000000000000001.json'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Updating records

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE `delta`.versiontable
# MAGIC SET Education_Level = 'PhD'
# MAGIC WHERE Industry = 'Networking'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT Education_Level , Industry
# MAGIC FROM `delta`.versiontable
# MAGIC WHERE Industry = 'Networking'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY `delta`.VersionTable

# COMMAND ----------

dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/versiontable/_delta_log')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Using versionAsOf using PySpark Code

# COMMAND ----------

df_1 = (spark.read.format('delta')
            .option('versionAsOf',"1")
            .load('dbfs:/user/hive/warehouse/delta.db/versiontable'))

# COMMAND ----------

df_1.filter("Industry == 'Networking'").select(["Education_Level","Industry"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Using versionAsOf using SQL Syntax

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT Education_Level , Industry
# MAGIC FROM `delta`.versiontable VERSION AS OF 1
# MAGIC WHERE Industry = 'Networking'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Using @v (VersionNumber) after Table Name

# COMMAND ----------

df_v_1 = (spark.read.format('delta')
            .load('dbfs:/user/hive/warehouse/delta.db/versiontable@v1'))

# COMMAND ----------

df_v_1.filter("Industry == 'Networking'").select(["Education_Level","Industry"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Using @v with SQL Syntax

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT Education_Level , Industry
# MAGIC FROM DELTA.`dbfs:/user/hive/warehouse/delta.db/versiontable@v1`
# MAGIC WHERE Industry = 'Networking'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Using timestampAsOf

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY `delta`.VersionTable

# COMMAND ----------

df_t_1 = (spark.read.format('delta')
            .option('timestampAsOf',"2023-12-08T05:06:43Z")
            .load('dbfs:/user/hive/warehouse/delta.db/versiontable'))

# COMMAND ----------

df_t_1.filter("Industry == 'Networking'").select(["Education_Level","Industry"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Using timestamp AS OF in SQL 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT Education_Level , Industry
# MAGIC FROM `delta`.versiontable TIMESTAMP AS OF "2023-12-08T05:06:44Z"
# MAGIC WHERE Industry = 'Networking'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT Education_Level , Industry
# MAGIC FROM `delta`.versiontable TIMESTAMP AS OF "2023-12-07"
# MAGIC WHERE Industry = 'Networking'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT Education_Level , Industry
# MAGIC FROM `delta`.versiontable
# MAGIC WHERE Industry = 'Networking'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Using RESTORE command to get previous version to Table

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE `delta`.versiontable TO VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT Education_Level , Industry
# MAGIC FROM `delta`.versiontable
# MAGIC WHERE Industry = 'Networking'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY `delta`.versiontable

# COMMAND ----------


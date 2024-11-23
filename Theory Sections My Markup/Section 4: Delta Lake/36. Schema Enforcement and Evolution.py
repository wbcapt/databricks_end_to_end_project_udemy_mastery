# Databricks notebook source
# MAGIC %md
# MAGIC # This Corresponds to lecture 36, 37 and the notebooks 05+Schema+Enforce and 05.+Schema+Evoluation

# COMMAND ----------

# MAGIC %md
# MAGIC #Schema Enforcement
# MAGIC
# MAGIC The idea is to make sure that the schema you configure is enforced over your delta lake tables
# MAGIC
# MAGIC To accomplish this, Delta Lake uses Schema validations on write operations. If the schema is not valid, the operation is cancelled and the user is told so
# MAGIC
# MAGIC This correlates to the 05 workbook

# COMMAND ----------

# MAGIC %md
# MAGIC ##General Schema Enforcement Rules:
# MAGIC
# MAGIC - 1. Incomming data cannot contain any additional columns
# MAGIC - 2. Incomming data cannot have data types that differe from the column data types in the target table

# COMMAND ----------

# MAGIC %md
# MAGIC #Create A connection

# COMMAND ----------

# MAGIC %md
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/connect/storage/tutorial-azure-storage
# MAGIC
# MAGIC Documentation on how to connect:
# MAGIC
# MAGIC service_credential = dbutils.secrets.get(scope="<scope>",key="<service-credential-key>")
# MAGIC
# MAGIC spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# COMMAND ----------

#configuring connection
service_credential = dbutils.secrets.get(scope="azure-key-vault-secret-scope-new",key="databricks-free-trial-secret")

storage_account = 'datbrudmymstryprjstg'


spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", "638478f8-a4fc-41be-9e9d-b3abe0e988ff")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", "https://login.microsoftonline.com/98898401-cb66-46bd-9820-017f5f98d1c6/oauth2/token")

# COMMAND ----------

#specify location of files
storage_container = 'databricks-schema-stuff'
dbutils.fs.ls(f'abfss://{storage_container}@{storage_account}.dfs.core.windows.net/')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Test your connection using DBUtils

# COMMAND ----------

#target directory
target_dir = 'SchemaEvol'
top_path = f'abfss://{storage_container}@{storage_account}.dfs.core.windows.net/'


dbutils.fs.ls(f'{top_path}/{target_dir}')

# COMMAND ----------

# MAGIC %md
# MAGIC We will use the target Delta Table called my_delta_spark_tbl created under the buster_schema_defult_location schema

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create an explicit schema
# MAGIC
# MAGIC The target table has more columns than intended, specifically the StructField

# COMMAND ----------

#create the schema
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DateType,FloatType,DoubleType

my_schema = StructType([
    StructField('Education_Level',StringType()),
    StructField('Line_Number',IntegerType()),
    StructField('Employed',IntegerType()),
    StructField('Unemployed',IntegerType()),
    StructField('Industry',StringType()),
    StructField('Gender',StringType()),
    StructField('Date_Inserted',StringType()),
    StructField('dense_rank',IntegerType()),
    StructField('Max_Salary_USD',IntegerType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC create your file path and test that it works

# COMMAND ----------

#specify the file source
path_dir = f'{top_path}/{target_dir}'

file_source = f'{path_dir}/SchemaMoreCols.csv'

#test the path
dbutils.fs.ls(file_source)

# COMMAND ----------

# MAGIC %md
# MAGIC load the csv file as a dataframe, remember to specify the option header as true because it is a csv file

# COMMAND ----------

#create the data frame
df_moreCols = (spark.read.format('csv')
               .schema(my_schema)
               .option('header','true')
               .load(f'{file_source}')
)

# COMMAND ----------

#print the schema
df_moreCols.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Example in action, try to append a new column to the delta table called my_delta_spark_tbl, this will throw an error

# COMMAND ----------

df_moreCols.write.format('delta').mode('append').saveAsTable('buster_schema_defult_location.my_delta_spark_tbl')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Try to get table with less columns. This will not throw an error

# COMMAND ----------

#create the schema
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DateType,FloatType,DoubleType

my_schema = StructType([
    StructField('Education_Level',StringType()),
    StructField('Line_Number',IntegerType()),
    StructField('Employed',IntegerType()),
    StructField('Unemployed',IntegerType()),
    StructField('Industry',StringType()),
    StructField('Gender',StringType())
])

# COMMAND ----------

#create the dataframe
df_lessCols = (spark.read.format('csv')
               .schema(my_schema)
               .option('header','true')
               .load(f'{path_dir}/SchemaLessCols.csv')
)

# COMMAND ----------

df_lessCols.write.format('delta').mode('append').saveAsTable('buster_schema_defult_location.my_delta_spark_tbl')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Source with different data type

# COMMAND ----------

#create the dataframe
df_diff_schema = (spark.read.format('csv')
               .schema(my_schema)
               .option('header','true')
               .load(f'{path_dir}/*.csv')
)

# COMMAND ----------

df_diff_schema.write.format('delta').mode('append').saveAsTable('buster_schema_defult_location.my_delta_spark_tbl')

# COMMAND ----------

# MAGIC %md
# MAGIC #Schema Evolution
# MAGIC
# MAGIC Schema evolution refers to adding new columns to a delta table where Delta accepts the writes

# COMMAND ----------

# MAGIC %md
# MAGIC ##Allow changes for extra column
# MAGIC
# MAGIC The key thing to add is the .option('mergeSchema','True')

# COMMAND ----------

df_moreCols.write.format('delta').mode('append').option('mergeSchema','True').saveAsTable('buster_schema_defult_location.my_delta_spark_tbl')

# COMMAND ----------

# MAGIC %md
# MAGIC We added the column Max_Salary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM buster_schema_defult_location.my_delta_spark_tbl

# COMMAND ----------

# MAGIC %md
# MAGIC ###Schema evolution: different format types

# COMMAND ----------

df_diff_schema.write.format('delta').mode('overwrite').option('overwriteSchema','True').saveAsTable('buster_schema_defult_location.my_delta_spark_tbl')

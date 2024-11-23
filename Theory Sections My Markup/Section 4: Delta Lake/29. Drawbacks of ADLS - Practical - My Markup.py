# Databricks notebook source
# MAGIC %md
# MAGIC # Corresponds to Lectures: 29. Drawbacks of ADLS - Practical lecture and 30. Creating Delta Lake
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## This is a marked up version of the 29. Drawbacks of ADLS Practical Lecture and 01.+Drawbacks+of+ADLS notebook
# MAGIC

# COMMAND ----------

#configuring connection
service_credential = dbutils.secrets.get(scope="azure-key-vault-secret-scope-new",key="databricks-free-trial-secret")

spark.conf.set("fs.azure.account.auth.type.datbrudmymstryprjstg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.datbrudmymstryprjstg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.datbrudmymstryprjstg.dfs.core.windows.net", "638478f8-a4fc-41be-9e9d-b3abe0e988ff")
spark.conf.set("fs.azure.account.oauth2.client.secret.datbrudmymstryprjstg.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.datbrudmymstryprjstg.dfs.core.windows.net", "https://login.microsoftonline.com/98898401-cb66-46bd-9820-017f5f98d1c6/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC Syntax is: abfs://[container_name]@[stroage_account].dfs.core.windows.net/[directory_path]

# COMMAND ----------

#target container for connection
source = 'abfss://test-connection-container@datbrudmymstryprjstg.dfs.core.windows.net/'

# COMMAND ----------

#create a schema, you need this for CSV files in particular

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
      .option('header', 'true')
      .schema(schema1)
      .load(f'{source}/test_folder/*.csv')
)


# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Writing to parquet format

# COMMAND ----------

(df.write.format('parquet')
    .mode('overwrite')
    .save(f'{source}/ParquetFolder/'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading parquet File
# MAGIC Remember that parquet is a file type that has the schema as part of the file

# COMMAND ----------

df_parquet = (spark.read.format('parquet')
            .load(f'{source}/ParquetFolder/'))

# COMMAND ----------

df_parquet.printSchema()

# COMMAND ----------

#create a temporary view
df_parquet.createOrReplaceTempView('ParquetView')

# COMMAND ----------

# MAGIC %md
# MAGIC # Drawback of Data Lake: Trying to update a value
# MAGIC Update the value of "High School" to School in the education_level field
# MAGIC
# MAGIC Note that this won't work though because we are writing against the Data Lake (technically just blob storage in this case) and NOT the Delta Lake. We have not yet stored anything in Databricks or Delta at this point.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE ParquetView 
# MAGIC SET Education_Level = 'School'
# MAGIC WHERE Education_Level = 'High School'

# COMMAND ----------

spark.sql("""   UPDATE ParquetView SET Education_Level = 'School' WHERE Education_Level = 'High School'  """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Filter and overwrite in Storage Container
# MAGIC

# COMMAND ----------

#filter the dataframe
df_parquet = df_parquet.filter("Education_level == 'High School'")

# COMMAND ----------

#write a temp view in Azure storage container
(df_parquet.write.format('parquet')
    .mode('overwrite')
    .save(f'{source}/Temp/'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading parquet file from Temp folder

# COMMAND ----------

df_temp = (spark.read.format('parquet')
                    .load(f'{source}/Temp/'))

# COMMAND ----------

display(df_temp)

# COMMAND ----------

(df_temp.write.format('parquet')
    .mode('overwrite')
    .save(f'{source}/ParquetFolder/'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading the overwritten file

# COMMAND ----------

df_parquet_ov = (spark.read.format('parquet')
            .load(f'{source}/ParquetFolder/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Problem with data lake: we cannot see previous versions of our overwritten data frame

# COMMAND ----------

display(df_parquet_ov)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating delta lake
# MAGIC

# COMMAND ----------

#write to a folder called delta
(df.write.format('delta')
    .mode('overwrite')
    .save(f'{source}/delta/'))

# COMMAND ----------

# MAGIC %md
# MAGIC The wrong way to read data
# MAGIC
# MAGIC this is not the way to read a delta like folder. This will throw an error of incompatible format because there is a delta log folder identifying it as a delta lake directory which means you cannot read as parquet
# MAGIC
# MAGIC df_read = (spark.read.format('parquet')
# MAGIC            .load(f'{source}/delta/*.parquet')
# MAGIC            )

# COMMAND ----------



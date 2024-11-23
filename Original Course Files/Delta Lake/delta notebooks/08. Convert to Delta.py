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

df.write.format('parquet').save(f'{source}/OnlyParquet')

# COMMAND ----------

dfnew = df.withColumnRenamed(existing='Line_Number',new='LNo')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Overwriting Parquet

# COMMAND ----------

dfnew.write.format('parquet').mode('overwrite').save(f'{source}/OnlyParquet')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Reading overwritten parquet

# COMMAND ----------

df_ov = (spark.read.format('parquet')
            .load(f'{source}/OnlyParquet'))

# COMMAND ----------

display(df_ov)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CONVERT TO DELTA parquet.`abfss://test@deltadbstg.dfs.core.windows.net/OnlyParquet`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`abfss://test@deltadbstg.dfs.core.windows.net/OnlyParquet`

# COMMAND ----------


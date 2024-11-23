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
# MAGIC ## Reading the delta file into dataframe
# MAGIC

# COMMAND ----------

df = (spark.read.format('parquet')
                .load(f'{source}/ParquetFolder/'))

# COMMAND ----------

display(df)

# COMMAND ----------

(df.write.format('delta')
        .mode('overwrite')
        .saveAsTable('`delta`.DeltaSpark'))

# COMMAND ----------

dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/deltaspark')

# COMMAND ----------

dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/deltaspark/_delta_log')

# COMMAND ----------

display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/deltaspark/_delta_log/00000000000000000000.json'))

# COMMAND ----------

display(spark.read.format('delta').load('dbfs:/user/hive/warehouse/delta.db/deltaspark'))

# COMMAND ----------


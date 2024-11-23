# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.deltadbstg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.deltadbstg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.deltadbstg.dfs.core.windows.net", "428e6361-2896-4a81-ba91-244293ad41d2")
spark.conf.set("fs.azure.account.oauth2.client.secret.deltadbstg.dfs.core.windows.net", "4EF8Q~iHz-YEI2MNXB89HvmPYAmBpoL2MwLLGc_U")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.deltadbstg.dfs.core.windows.net", "https://login.microsoftonline.com/2d5dcdaa-3dc8-4853-a43b-ac7af2182644/oauth2/token")

# COMMAND ----------

source = 'abfss://test@deltadbstg.dfs.core.windows.net/'

# COMMAND ----------

dbutils.fs.ls(f'{source}/delta/')

# COMMAND ----------

dbutils.fs.ls(f'{source}/delta/_delta_log')

# COMMAND ----------

display(spark.read.format('text').load('abfss://test@deltadbstg.dfs.core.windows.net/delta/_delta_log/00000000000000000000.json'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading the delta lake file

# COMMAND ----------

df = (spark.read.format('delta')
                .load(f'{source}/delta/'))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

df_delta = df.filter("Education_Level =='High School'")

# COMMAND ----------

df_delta.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Overwriting the same file in delta folder

# COMMAND ----------

(df_delta.write.format('delta')
        .mode('overwrite')
        .save(f'{source}/delta/'))

# COMMAND ----------

display(spark.read.format('text').load('abfss://test@deltadbstg.dfs.core.windows.net/delta/_delta_log/00000000000000000001.json'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading the overwritten file

# COMMAND ----------

df_overwrite = (spark.read.format('delta')
                .load(f'{source}/delta/'))

# COMMAND ----------

display(df_overwrite)

# COMMAND ----------


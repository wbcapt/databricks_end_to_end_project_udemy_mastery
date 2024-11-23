# Databricks notebook source
# MAGIC %md
# MAGIC # This is a marked up version of the file 02. Understanding the transaction log and corresponds to the lectures 32

# COMMAND ----------

# MAGIC %md
# MAGIC The storage I created was dtabrudmymstrgprjstg for databricks udemy storage project

# COMMAND ----------

#configuring connection
service_credential = dbutils.secrets.get(scope="azure-key-vault-secret-scope-new",key="databricks-free-trial-secret")

spark.conf.set("fs.azure.account.auth.type.datbrudmymstryprjstg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.datbrudmymstryprjstg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.datbrudmymstryprjstg.dfs.core.windows.net", "638478f8-a4fc-41be-9e9d-b3abe0e988ff")
spark.conf.set("fs.azure.account.oauth2.client.secret.datbrudmymstryprjstg.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.datbrudmymstryprjstg.dfs.core.windows.net", "https://login.microsoftonline.com/98898401-cb66-46bd-9820-017f5f98d1c6/oauth2/token")

# COMMAND ----------

#target container for connection
source = 'abfss://test-connection-container@datbrudmymstryprjstg.dfs.core.windows.net/'

# COMMAND ----------

#this prints your files
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



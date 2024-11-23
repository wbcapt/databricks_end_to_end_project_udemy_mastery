# Databricks notebook source
# MAGIC %md
# MAGIC # This is just an experimentation of databricks tables
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Create a connection First

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

# MAGIC %md
# MAGIC #Create a new directory
# MAGIC Note: this just created a block blob in the storage container and NOT a directory. You would have had to specify delta_experimentation/some other file

# COMMAND ----------

dbutils.fs.mkdirs(source + "delta_experimentation/")

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ##remove an empty directory created in error

# COMMAND ----------

#remove the erronenous directory
dbutils.fs.rm(source+"delta_experimentation")

# COMMAND ----------

# MAGIC %md
# MAGIC ##remove a non empyty directory created

# COMMAND ----------

dbutils.fs.rm(source+"new_folder/",True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##create a directory properly

# COMMAND ----------

dbutils.fs.mkdirs(source+"delta_experimentation/read_me.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ##move a file into the directory

# COMMAND ----------

#move file into the new directory
new_source = source+"delta_experimentation/"

dbutils.fs.cp(source+"/Online_Retail.csv",new_source)

# COMMAND ----------

# MAGIC %md
# MAGIC #Create a Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC ##first specify your dataframe source

# COMMAND ----------

retail_df = (spark.read.format('csv')
      .option('header', 'true')
      #.schema(schema1)
      .load(f'{new_source}/*.csv')
)

# COMMAND ----------

print(retail_df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##write a delta lake using your created data frame
# MAGIC
# MAGIC This will create several things:
# MAGIC 1. The delta lake files that will be saved under the my_delta_lake_folder created
# MAGIC 2. A folder called delta log that contains the delta log. (Created under the my_delta_lake_folder). THE DELTA LOG Is what really creates the delta lake
# MAGIC   1. CRC = prevents file corruption 
# MAGIC   2. JSON file = transaction information applied on actual data
# MAGIC 3. A collection of parquet files under the my_delta_lake_folder_created (unless you create/apply a partition directory) that are called 'part0000' part0001 etc
# MAGIC
# MAGIC The parquet files containe the actual data, whereas the JSON files record Insert, update, delete information.

# COMMAND ----------

#create the data lake
(retail_df.write.format('delta').mode('overwrite').save(f'{new_source}/my_delta_lake_folder/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###read the delta transaction log files
# MAGIC
# MAGIC

# COMMAND ----------

target_file_path = f'{new_source}/my_delta_lake_folder/_delta_log/00000000000000000000.json'

display(spark.read.format('text').load(target_file_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reading the delta lake file and create a dataframe

# COMMAND ----------

#create dataframe
delta_retail_df = (spark.read.format('delta').load(f'{source}/delta/'))

# COMMAND ----------

#print the schema
delta_retail_df.printSchema()

# COMMAND ----------

display(delta_retail_df)

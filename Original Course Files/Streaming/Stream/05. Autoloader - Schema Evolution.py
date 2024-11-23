# Databricks notebook source
dbutils.fs.rm('dbfs:/user/hive/warehouse/stream.db',True)
dbutils.fs.rm('dbfs:/FileStore/streaming',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS stream CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS stream

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## AutoLoader

# COMMAND ----------

source_dir = 'dbfs:/FileStore/streaming/'

# COMMAND ----------

df = spark.readStream\
        .format('cloudFiles')\
        .option("cloudFiles.format","csv")\
        .option("cloudFiles.schemaLocation",f'{source_dir}/schemaInfer')\
        .option("cloudFiles.inferColumnTypes","true")\
        .option('header','true')\
        .load(source_dir)

# COMMAND ----------

dbutils.fs.ls(f'{source_dir}/schemaInfer')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/streaming/schemaInfer/_schemas/')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM JSON.`dbfs:/FileStore/streaming/schemaInfer/_schemas/0`

# COMMAND ----------

# MAGIC %md
# MAGIC ### SchemaHints

# COMMAND ----------

df = spark.readStream\
        .format('cloudFiles')\
        .option("cloudFiles.format","csv")\
        .option("cloudFiles.schemaLocation",f'{source_dir}/schemaInfer')\
        .option("cloudFiles.inferColumnTypes","true")\
        .option('cloudFiles.schemaHints',"Citizens LONG")\
        .option('header','true')\
        .load(source_dir)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM JSON.`dbfs:/FileStore/streaming/schemaInfer/_schemas/0`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Schema Evolution

# COMMAND ----------

df = spark.readStream\
        .format('cloudFiles')\
        .option("cloudFiles.format","csv")\
        .option("cloudFiles.schemaLocation",f'{source_dir}/schemaInfer')\
        .option('cloudFiles.schemaEvolutionMode','rescue')\
        .option('rescuedDataColumn','_rescued_data')\
        .option("cloudFiles.inferColumnTypes","true")\
        .option('cloudFiles.schemaHints',"Citizens LONG")\
        .option('header','true')\
        .load(source_dir)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM JSON.`dbfs:/FileStore/streaming/schemaInfer/_schemas/0`

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/streaming/schemaInfer/_schemas/')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 02 - addNewColumns - Default

# COMMAND ----------

df = spark.readStream\
        .format('cloudFiles')\
        .option("cloudFiles.format","csv")\
        .option("cloudFiles.schemaLocation",f'{source_dir}/schemaInfer')\
        .option("cloudFiles.inferColumnTypes","true")\
        .option('cloudFiles.schemaHints',"Citizens LONG")\
        .option('header','true')\
        .load(source_dir)

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/streaming/schemaInfer/_schemas/')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM JSON.`dbfs:/FileStore/streaming/schemaInfer/_schemas/1`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 03- failOnNewColumns

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/streaming/schemaInfer/_schemas/')

# COMMAND ----------

df = spark.readStream\
        .format('cloudFiles')\
        .option("cloudFiles.format","csv")\
        .option("cloudFiles.schemaLocation",f'{source_dir}/schemaInfer')\
        .option('cloudFiles.schemaEvolutionMode','failOnNewColumns')\
        .option("cloudFiles.inferColumnTypes","true")\
        .option('cloudFiles.schemaHints',"Citizens LONG")\
        .option('header','true')\
        .load(source_dir)

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/streaming/schemaInfer/_schemas/')

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### None

# COMMAND ----------

df = spark.readStream\
        .format('cloudFiles')\
        .option("cloudFiles.format","csv")\
        .option("cloudFiles.schemaLocation",f'{source_dir}/schemaInfer')\
        .option('cloudFiles.schemaEvolutionMode','none')\
        .option("cloudFiles.inferColumnTypes","true")\
        .option('cloudFiles.schemaHints',"Citizens LONG")\
        .option('header','true')\
        .load(source_dir)

# COMMAND ----------

display(df)

# COMMAND ----------


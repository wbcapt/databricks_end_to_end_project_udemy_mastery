# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Running common notebook to get access to variables

# COMMAND ----------

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(notebook_path)


# COMMAND ----------

# MAGIC %md
# MAGIC ##This is the syntax for how you run a notebook and it will make the 

# COMMAND ----------

# MAGIC %run "/Repos/baldcodes2@gmail.com/databricks_end_to_end_project_udemy_mastery/working_directory/(Clone) 04. Common"
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Test that you can call a function frome notebook 4 - Common

# COMMAND ----------

#check function from notebook
print(checkpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Make sure you use the % run command as opposed to the dbutils command. Here's why
# MAGIC
# MAGIC The %run command literally includes the specified notebook into the current notebook as if its code were written in the current notebook. This means that all variables, functions, and imports defined in the included notebook are available in the current notebook's scope and can be called
# MAGIC
# MAGIC dbutils.notebook.run executes the target notebook in a separate context. This means that variables, functions, and imports defined in the target notebook are not accessible in the calling notebook after the execution finishes.
# MAGIC
# MAGIC dbutils.notebook.run allows you to pass parameters to the target notebook, which can be useful for certain use cases where you need to execute a notebook with specific input parameters.
# MAGIC However, the separate context means that you need to explicitly handle parameter passing and returning results, which can add complexity.
# MAGIC
# MAGIC
# MAGIC Use %run when:
# MAGIC
# MAGIC You need to include common functions, classes, or configurations from another notebook.
# MAGIC You want to share state (variables, imports, etc.) directly between notebooks.
# MAGIC You are developing code that should be modular and well-organized without needing to explicitly pass parameters or return values.
# MAGIC
# MAGIC Use dbutils.notebook.run when:
# MAGIC
# MAGIC You need to run a notebook as a separate job or task with its own execution context.
# MAGIC You need to pass parameters to the notebook and handle its output as a distinct step in a workflow.
# MAGIC You are working with more complex workflows that involve conditional execution, retries, or parallelism.

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver transformation for the roads data set

# COMMAND ----------

# MAGIC %md
# MAGIC ##Specify the catalog to use and pass it as a variable

# COMMAND ----------

#specify a variable to use
dbutils.widgets.text(name="env",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading from bronze raw_Roads

# COMMAND ----------


def read_BronzeRoadsTable(environment):
    print('Reading the Bronze Table raw_roads Data : ',end='')
    df_bronzeRoads = (spark.readStream
                    .table(f"`{environment}_catalog`.`bronze`.raw_roads")
                    )
    print(f'Reading {environment}_catalog.bronze.raw_roads Success!')
    print("**********************************")
    return df_bronzeRoads

# COMMAND ----------

df_roads = read_BronzeRoadsTable(env)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating road_category_name column

# COMMAND ----------

def road_Category(df):
    print('Creating Road Category Name Column: ', end='')
    #note the import here of when and col
    from pyspark.sql.functions import when,col

    #This is like a case when statement where you create a new column to transform values
    df_road_Cat = df.withColumn("Road_Category_Name",
                  when(col('Road_Category') == 'TA', 'Class A Trunk Road')
                  .when(col('Road_Category') == 'TM', 'Class A Trunk Motor')
                   .when(col('Road_Category') == 'PA','Class A Principal road')
                    .when(col('Road_Category') == 'PM','Class A Principal Motorway')
                    .when(col('Road_Category') == 'M','Class B road')
                    .otherwise('NA')
                  
                  )
    print('Success!! ')
    print('***********************')
    return df_road_Cat

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating road_type column
# MAGIC
# MAGIC

# COMMAND ----------

def road_Type(df):
    print('Creating Road Type Name Column: ', end='')
    from pyspark.sql.functions import when,col

    df_road_Type = df.withColumn("Road_Type",
                  when(col('Road_Category_Name').like('%Class A%'),'Major')
                  .when(col('Road_Category_Name').like('%Class B%'),'Minor')
                    .otherwise('NA')
                  
                  )
    print('Success!! ')
    print('***********************')
    return df_road_Type

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Writing data to silver_roads in Silver schema

# COMMAND ----------

def write_Roads_SilverTable(StreamingDF,environment):
    print('Writing the silver_roads Data : ',end='') 

    write_StreamSilver_R = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint+ "/SilverRoadsLoad/Checkpt/")
                .outputMode('append')
                .queryName("SilverRoadsWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{environment}_catalog`.`silver`.`silver_roads`"))
    
    write_StreamSilver_R.awaitTermination()
    print(f'Writing `{environment}_catalog`.`silver`.`silver_roads` Success!')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Calling functions

# COMMAND ----------

df_noDups = remove_Dups(df_roads)

AllColumns = df_noDups.schema.names
df_clean = handle_NULLs(df_noDups,AllColumns)

## Creating Road_Category_name 
df_roadCat = road_Category(df_clean)

## Creating Road_Type column
df_type = road_Type(df_roadCat)

## Writing data to silver_roads table

write_Roads_SilverTable(df_type,env)

# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Extracting Checkpoint, Bronze, Silver containers URLs

# COMMAND ----------

# MAGIC %md
# MAGIC ## We first need to get the external locations and save as a variable

# COMMAND ----------

checkpoint_location = spark.sql("DESCRIBE EXTERNAL LOCATION checkpoints").select("url").collect()[0][0]
bronze_external = spark.sql("DESCRIBE EXTERNAL LOCATION bronze").select("url").collect()[0][0]
silver_external = spark.sql("DESCRIBE EXTERNAL LOCATION silver").select("url").collect()[0][0]

# COMMAND ----------

print(checkpoint_location)
print(bronze_external)
print(silver_external)


# COMMAND ----------

collection_df_base = spark.sql("DESCRIBE EXTERNAL LOCATION checkpoints").select("url").collect()
print(collection_df_base)

# COMMAND ----------

collection_df = spark.sql("DESCRIBE EXTERNAL LOCATION checkpoints").select("url").collect()[0]
print(collection_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Parameterize environment

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %md
# MAGIC #Perform data checks before Silver Transformations
# MAGIC
# MAGIC Note, we can no longer utilize auto loader syntax with cloud files. Why? Because we are reading from a spark table
# MAGIC
# MAGIC Instead, we are now utilizing spark structured streaming to read data from tables. We were using spark structured streaming before in the bronze transformations, but this is just a call out that we are still using it.
# MAGIC
# MAGIC structured streaming is easily identified by spark.readStream
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Reading the Traffic data from bronze Table
# MAGIC
# MAGIC This function will be called and then the returned result will be passed as an argument to handle duplicate rows

# COMMAND ----------


def read_BronzeTrafficTable(environment):
    print('Reading the Bronze Table Data : ',end='')
    df_bronzeTraffic = (spark.readStream
                    .table(f"`{environment}_catalog`.`bronze`.raw_traffic")
                    )
    print(f'Reading {environment}_catalog.bronze.raw_traffic Success!')
    return df_bronzeTraffic

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Handing duplicate rows
# MAGIC
# MAGIC This function accepts the returned df_bronzeTraffic dataframe created from the above function read_BronzeTrafficTable as an argument and it will create a de-duplicated dataframe
# MAGIC
# MAGIC Syntax is <your_dataframe>.dropDuplicates()

# COMMAND ----------

def remove_Dups(df):
    print('Removing Duplicate values: ', end='')
    df_dup = df.dropDuplicates() #syntax to drop duplicates
    print('Success!! ')
    return df_dup

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Handling NULL values by replacing them
# MAGIC
# MAGIC This is going to fill in null values. The general syntax is:
# MAGIC
# MAGIC df.fillna([fill with value], subset = which columns to replace )

# COMMAND ----------

def handle_NULLs(df,columns):
    print('Replacing NULL values on String Columns with "Unknown" ' , end='')
    df_string = df.fillna('Unknown',subset= columns) #syntax to remove null values
    print('Successs!! ')

    print('Replacing NULL values on Numeric Columns with "0" ' , end='')
    df_clean = df_string.fillna(0,subset = columns)
    print('Success!! ')

    return df_clean

# COMMAND ----------

# MAGIC %md
# MAGIC #Now begin the process of Silver Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Getting count of Electric vehicles by creating new column
# MAGIC
# MAGIC Note EV_Car and EV_Bike are integer columns that count the amount of these vehichles at a certain timestamp on the road checkpoint
# MAGIC
# MAGIC Also note that in the bronze notebook we specified the schema of the structured table to use snake case for the columns

# COMMAND ----------

def ev_Count(df):
    print('Creating Electric Vehicles Count Column : ', end='')
    from pyspark.sql.functions import col
    df_ev = df.withColumn('Electric_Vehicles_Count',
                            col('EV_Car') + col('EV_Bike')
                            )
    
    print('Success!! ')
    return df_ev

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating columns to get Count of all motor vehicles
# MAGIC

# COMMAND ----------

def Motor_Count(df):
    print('Creating All Motor Vehicles Count Column : ', end='')
    from pyspark.sql.functions import col
    df_motor = df.withColumn('Motor_Vehicles_Count',
                            col('Electric_Vehicles_Count') + col('Two_wheeled_motor_vehicles') + col('Cars_and_taxis') + col('Buses_and_coaches') + col('LGV_Type') + col('HGV_Type')
                            )
    
    print('Success!! ')
    return df_motor

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating Transformed Time column

# COMMAND ----------

def create_TransformedTime(df):
    from pyspark.sql.functions import current_timestamp
    print('Creating Transformed Time column : ',end='')
    df_timestamp = df.withColumn('Transformed_Time',
                      current_timestamp()
                      )
    print('Success!!')
    return df_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Calling the functions
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Writing the Transformed data to Silver_Traffic Table

# COMMAND ----------

def write_Traffic_SilverTable(StreamingDF,environment):
    print('Writing the silver_traffic Data : ',end='') 

    write_StreamSilver = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint_location+ "/SilverTrafficLoad/Checkpt/")
                .outputMode('append')
                .queryName("SilverTrafficWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{environment}_catalog`.`silver`.`silver_traffic`"))
    
    write_StreamSilver.awaitTermination()
    print(f'Writing `{environment}_catalog`.`silver`.`silver_traffic` Success!')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Call the functions

# COMMAND ----------

## Reading the bronze traffic data

df_trafficdata = read_BronzeTrafficTable(env)

# To remove duplicate rows

df_dups = remove_Dups(df_trafficdata)

# To raplce any NULL values
Allcolumns =df_dups.schema.names
df_nulls = handle_NULLs(df_dups,Allcolumns)

## To get the total EV_Count

df_ev = ev_Count(df_nulls)


## To get the Total Motor vehicle count

df_motor = Motor_Count(df_ev)

## Calling Transformed time function

df_final = create_TransformedTime(df_motor)

## Writing to silver_traffic

write_Traffic_SilverTable(df_final, env)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Check outputs

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) FROM `dev_catalog`.`silver`.`silver_traffic`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM `dev_catalog`.`silver`.`silver_traffic`
# MAGIC WHERE Record_ID BETWEEN '37086' and '37096'
# MAGIC ORDER BY Record_ID

# COMMAND ----------



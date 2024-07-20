# Databricks notebook source
# MAGIC %md
# MAGIC #Refersher on What AUto Loader is and how it works:
# MAGIC
# MAGIC Autoloader provides a structured streaming source called cloud files. Given an input directory path on the cloud file storage, the cloudFiles soruce automatically processes new files as they arrive with the option of also processing existing files in that directory.
# MAGIC
# MAGIC As files are discovered their metadata is persisisted in the checkpoing location of your autoLoader pipeline
# MAGIC
# MAGIC Databricks recommends auto loader in Delta Live Tables for incremental data ingestion to gain all benefits:
# MAGIC
# MAGIC 1. Autoscaling compute for cost savings
# MAGIC 2. Data quality checks with expectations
# MAGIC 3. Automatic schema inference & evolution handling
# MAGIC 4. Monitoring via metrics in the event log
# MAGIC
# MAGIC
# MAGIC Schema inferernce = enables Auto Loader to automatically detect the schema of loaded data, allowing you to initialize tables without explicitly declaring the data schema and evolve the table schema as new columns are introduced. It eliminates the need to manually track and apply schmea changes over time
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Basic demo of how to cerate a (spark structured) read stream to ingest data
# MAGIC
# MAGIC Don't run these blocks because you create everything in a python file later

# COMMAND ----------

# MAGIC %md
# MAGIC ##First let's identify where our checkpoint is going to be
# MAGIC
# MAGIC This is a critical piece of information as it is where the metadata is persisted

# COMMAND ----------

checkpoint_location = spark.sql("DESCRIBE EXTERNAL LOCATION checkpoints").select("url").collect()[0][0]
print(checkpoint_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ##This is the basic read stream syntax

# COMMAND ----------

spark.readStream
    .format("cloudFiles") #this is required per databricks. cloudFiles are the storage format
    .option("cloudFiles.format","csv") #specify the type of file format
    .option("cloudFiles.schemaLocation", f'{checkpoint_location}/rawTrafficload/schemaInfer') #syntax to enable schema inference and evolution. Also creates a directory of where to persist this information
    .option('header','true') #required for CSV files
    .schema(schema) #use schema specified. This is typically handled as a defined cell all on its own
    .load(landing+'/raw_traffic/') #where to get files from

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Setup the ingestion for autoloader using a function called read_Traffic_Data()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Identify the checkpoint location where metadata will be persisted

# COMMAND ----------

checkpoint_location = spark.sql("DESCRIBE EXTERNAL LOCATION checkpoints").select("url").collect()[0][0]
print(checkpoint_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Identify the landing location where the files will be ingested from and declare it as a variable

# COMMAND ----------

landing_location = spark.sql("DESCRIBE EXTERNAL LOCATION landing").select("url").collect()[0][0]
print(landing_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Now define a function that creates the schema and specifies setup of Auto Loader to load to a Unity Catalog Managed Table
# MAGIC
# MAGIC Step 1 is to specify a read stream

# COMMAND ----------

# MAGIC %md
# MAGIC ### read_traffic_Data

# COMMAND ----------

def read_Traffic_Data():
    #import necessary libraries for defining your schema
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from pyspark.sql.functions import current_timestamp
    print("Reading the Raw Traffic Data :  ", end='')

    #define your schema for the CSV file (headers)
    schema = StructType([
    StructField("Record_ID",IntegerType()),
    StructField("Count_point_id",IntegerType()),
    StructField("Direction_of_travel",StringType()),
    StructField("Year",IntegerType()),
    StructField("Count_date",StringType()),
    StructField("hour",IntegerType()),
    StructField("Region_id",IntegerType()),
    StructField("Region_name",StringType()),
    StructField("Local_authority_name",StringType()),
    StructField("Road_name",StringType()),
    StructField("Road_Category_ID",IntegerType()),
    StructField("Start_junction_road_name",StringType()),
    StructField("End_junction_road_name",StringType()),
    StructField("Latitude",DoubleType()),
    StructField("Longitude",DoubleType()),
    StructField("Link_length_km",DoubleType()),
    StructField("Pedal_cycles",IntegerType()),
    StructField("Two_wheeled_motor_vehicles",IntegerType()),
    StructField("Cars_and_taxis",IntegerType()),
    StructField("Buses_and_coaches",IntegerType()),
    StructField("LGV_Type",IntegerType()),
    StructField("HGV_Type",IntegerType()),
    StructField("EV_Car",IntegerType()),
    StructField("EV_Bike",IntegerType())
    ])

    # now define your autoloader managed table
    #https://docs.databricks.com/en/ingestion/auto-loader/unity-catalog.html 
    rawTraffic_stream = (spark.readStream
        .format("cloudFiles") #required syntax. CloudFiles is an argument databricks understands 
        .option("cloudFiles.format","csv") #speficy the format of the files
        .option('cloudFiles.schemaLocation',f'{checkpoint_location}/rawTrafficLoad/schemaInfer') #create a table under your checkpoint location to store the schema
        .option('header','true') #necessary for CSV files
        .schema(schema) #use schema as defined up above
        .load(landing_location+'/raw_traffic/') #where to get files from. Landing container + raw_traffic folder
        .withColumn("Extract_Time", current_timestamp()))
    
    print('Reading Succcess !!')
    print('*******************')

    return rawTraffic_stream

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating read_Road_Data() Function

# COMMAND ----------

def read_Road_Data():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from pyspark.sql.functions import current_timestamp
    print("Reading the Raw Roads Data :  ", end='')
    schema = StructType([
        StructField('Road_ID',IntegerType()),
        StructField('Road_Category_Id',IntegerType()),
        StructField('Road_Category',StringType()),
        StructField('Region_ID',IntegerType()),
        StructField('Region_Name',StringType()),
        StructField('Total_Link_Length_Km',DoubleType()),
        StructField('Total_Link_Length_Miles',DoubleType()),
        StructField('All_Motor_Vehicles',DoubleType())
        
        ])

    rawRoads_stream = (spark.readStream
        .format("cloudFiles") 
        .option("cloudFiles.format","csv")
        .option('cloudFiles.schemaLocation',f'{checkpoint_location}/rawRoadsLoad/schemaInfer')
        .option('header','true')
        .schema(schema)
        .load(landing_location+'/raw_roads/')
        )
    
    print('Reading Succcess !!')
    print('*******************')

    return rawRoads_stream

# COMMAND ----------

# MAGIC %md
# MAGIC ##Now create write functions to create delta tables

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating write_Traffic_Data(StreamingDF,environment) Function

# COMMAND ----------

def write_Traffic_Data(StreamingDF,environment):
    print(f'Writing data to {environment}_catalog raw_traffic table', end='' )
    write_Stream = (StreamingDF.writeStream
                    .format('delta') #write as a delta table
                    .option("checkpointLocation",checkpoint_location + '/rawTrafficLoad/Checkpt') #this is a folder created to check the progress of your stream
                    .outputMode('append')
                    .queryName('rawTrafficWriteStream') #this specifies a query name that is easily recognizeable in the Spark UI
                    .trigger(availableNow=True) #preferred trigger. Reads data and then automatically stops it once batch is read
                    .toTable(f"`{environment}_catalog`.`bronze`.`raw_traffic`")) #create a table to be persisted under the bronze schema in the dev catalog
    
    write_Stream.awaitTermination()
    print('Write Success')
    print("****************************")    

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating write_Road_Data(StreamingDF,environment) Function

# COMMAND ----------

def write_Road_Data(StreamingDF,environment):
    print(f'Writing data to {environment}_catalog raw_roads table', end='' )
    write_Data = (StreamingDF.writeStream
                    .format('delta')
                    .option("checkpointLocation",checkpoint_location + '/rawRoadsLoad/Checkpt')
                    .outputMode('append')
                    .queryName('rawRoadsWriteStream')
                    .trigger(availableNow=True)
                    .toTable(f"`{environment}_catalog`.`bronze`.`raw_roads`"))
    
    write_Data.awaitTermination()
    print('Write Success')
    print("****************************")    

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Calling read and Write Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ###use DB Utils to pass the environment name as an argument

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %md
# MAGIC ###call the various functions up above
# MAGIC
# MAGIC Note: you have to click on the function cells above to run them. Running the cell below will not call the functions automatically

# COMMAND ----------

## Reading the raw_traffic's data from landing to Bronze
read_Df = read_Traffic_Data()

## Reading the raw_roads's data from landing to Bronze
read_roads = read_Road_Data()

## Writing the raw_traffic's data from landing to Bronze
write_Traffic_Data(read_Df,env)

## Writing the raw_roads's data from landing to Bronze
write_Road_Data(read_roads,env)

# COMMAND ----------

# MAGIC %md
# MAGIC ##check results

# COMMAND ----------

# MAGIC %md
# MAGIC ###raw traffic table

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {env}_catalog.bronze.raw_traffic"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### raw roads table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM
# MAGIC dev_catalog.bronze.raw_roads

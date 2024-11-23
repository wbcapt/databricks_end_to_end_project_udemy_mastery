# Databricks notebook source
# MAGIC %md
# MAGIC #Project Setup
# MAGIC
# MAGIC This is the setup for the end to end project of the udemy mastery course
# MAGIC
# MAGIC This notebook is used to configure schemas under the dev_catalog you should have set up

# COMMAND ----------

# MAGIC %md
# MAGIC ##Specify the catalog to be used

# COMMAND ----------

# MAGIC %sql
# MAGIC --Specify the catalog to be used
# MAGIC
# MAGIC USE CATALOG dev_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC --identify what catalog you currently are in
# MAGIC
# MAGIC SELECT current_catalog()

# COMMAND ----------

# MAGIC %md
# MAGIC #Identify what schemas exist in the current catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ##Use the SQL Method

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN dev_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC Note that there will just be the default and information_schema
# MAGIC
# MAGIC The default schema's purpose is to be used if no specific schema is mentioned in sql queries. It will contain user-crreated tables, view or other objects. Dropping it would result in losing all user-created objects and data within it. It can be dropped using the CASCADE method
# MAGIC
# MAGIC The information schema provides the metadata and information about the database objects such as tables, columsn, and other schema objects. It is mission critical. Dropping it is not permissed

# COMMAND ----------

# MAGIC %md
# MAGIC ##Spark SQL Method (Ie python)
# MAGIC
# MAGIC Remember that all spark sql is, is just passing SQL statements as a python function

# COMMAND ----------

#show the schemas. Remember that the ; is not necesarily important
schema_df = spark.sql(f'SHOW SCHEMAS IN dev_catalog')
display(schema_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Drop All Schemas for a clean slate

# COMMAND ----------

# MAGIC %md
# MAGIC ###SQL Method

# COMMAND ----------

# MAGIC %sql
# MAGIC --Just to show dropping two
# MAGIC DROP SCHEMA IF EXISTS dev_catalog.bronze;
# MAGIC DROP SCHEMA IF EXISTS dev_catalog.silver;

# COMMAND ----------

# MAGIC %md
# MAGIC The SQL Method is a pain in the ass because you would need to drop all schemans and know what they are. You cannot just perform a CASCADE operation to drop all schemas because the CASCADE operation does not function at the catalog level
# MAGIC
# MAGIC Instead it is better to use python to loop through all your schemas in your catalog

# COMMAND ----------

# Define the catalog name
catalog_name = 'dev_catalog'

# List all schemas in the catalog
schemas_df = spark.sql(f"SHOW SCHEMAS IN {catalog_name}")
schemas = schemas_df.select("databaseName").rdd.flatMap(lambda x: x).collect()

# Review the list of schemas to be dropped
print("Schemas to be dropped:", schemas)

# Confirm before proceeding
confirm = input("Are you sure you want to drop these schemas? Note that default and information schemas will NOT be dropped. Type 'yes' to confirm: ")

if confirm.lower() == 'yes':
    for schema in schemas:
        if schema not in ['default', 'information_schema']:  # Avoid dropping critical schemas
            try:
                spark.sql(f"DROP SCHEMA {catalog_name}.{schema} CASCADE")
                print(f"Schema {catalog_name}.{schema} dropped successfully.")
            except Exception as e:
                print(f"Failed to drop schema {catalog_name}.{schema}: {e}")
        else:
            print(f"Skipping critical schema: {schema}")
else:
    print("Operation cancelled.")


# COMMAND ----------

# MAGIC %md
# MAGIC #Describe the external locations
# MAGIC
# MAGIC This feeds into below creation of schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ##Describe the location of a storage location

# COMMAND ----------

# MAGIC %sql
# MAGIC --This is where bronze is kept
# MAGIC DESCRIBE EXTERNAL LOCATION bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ##Spark SQL to get the URL of a locaiton

# COMMAND ----------

#get the url of the target location
display(spark.sql("""DESCRIBE EXTERNAL LOCATION bronze""").select("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Spark SQL to review an external location an assign it to a variable

# COMMAND ----------

# MAGIC %md
# MAGIC including the collect() argument removes the url header so that it can be passed as a variable

# COMMAND ----------

bronze_path = spark.sql("""DESCRIBE EXTERNAL LOCATION bronze""").select("url").collect()[0][0]
silver_path = spark.sql("""DESCRIBE EXTERNAL LOCATION silver""").select("url").collect()[0][0]
gold_path = spark.sql("""DESCRIBE EXTERNAL LOCATION gold""").select("url").collect()[0][0]

print(bronze_path)
print(silver_path)
print(gold_path)


# COMMAND ----------

# MAGIC %md
# MAGIC #SQL Command to create a schema:
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS 'silver'
# MAGIC MANGED LOCATION 'abfss://medallion@databricksdevstg.dfs.core.windwos.net/silver'

# COMMAND ----------

# MAGIC %md
# MAGIC First Confirm that the catalog being used is dev_catalog. This should be run if you run the top most cell in this workbook. OTHERWISE everything after this will be using the hive metastore

# COMMAND ----------

#Confirm the current catalog used to ensure managed table created in correct place
df = spark.sql("""SELECT current_catalog()""")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##create a schema in the dev_catalog using managed location

# COMMAND ----------

#confirm silver path variable
print(silver_path)

# COMMAND ----------

spark.sql(f"""CREATE SCHEMA IF NOT EXISTS silver MANAGED LOCATION '{silver_path}/silver'"""
)

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN dev_catalog"))

# COMMAND ----------

# MAGIC %md
# MAGIC Now drop the schema for good measure

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS silver

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN dev_catalog"))

# COMMAND ----------

# MAGIC %run "/Users/shanmukh@shanmukhsattiraju.com/04. Common"

# COMMAND ----------

# MAGIC %md
# MAGIC #Use the below widget to prompt the user for a value that would be used to create a database/metadata under a specified location

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue="",label=" Enter the environment in lower case")
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %md
# MAGIC #Systematically create schemas using functions
# MAGIC
# MAGIC remember to drop these

# COMMAND ----------

# MAGIC %md
# MAGIC ##Set up the functions for bronze, gold, silver

# COMMAND ----------

#DO NOT RUN AGAIN
#pass an argument of environment and path to the function
#this will specify to use the dev_catalog if you pass 'dev' as an argument and then use the particular path passed in as an argument
#so last line will be bronze_path/bronze

def create_Bronze_Schema(environment,path):
    print(f'Using {environment}_Catalog ')
    spark.sql(f""" USE CATALOG '{environment}_catalog'""")
    print(f'Creating Bronze Schema in {environment}_Catalog')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS bronze MANAGED LOCATION '{path}/bronze'""")
    print("************************************")

# COMMAND ----------

def create_Silver_Schema(environment,path):
    print(f'Using {environment}_Catalog ')
    spark.sql(f""" USE CATALOG '{environment}_catalog'""")
    print(f'Creating Silver Schema in {environment}_Catalog')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `silver` MANAGED LOCATION '{path}/silver'""")
    print("************************************")

# COMMAND ----------

def create_Gold_Schema(environment,path):
    print(f'Using {environment}_Catalog ')
    spark.sql(f""" USE CATALOG '{environment}_catalog'""")
    print(f'Creating Gold Schema in {environment}_Catalog')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `gold` MANAGED LOCATION '{path}/gold'""")
    print("************************************")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Call the functions

# COMMAND ----------

create_Bronze_Schema(env,bronze_path)

# COMMAND ----------

create_Silver_Schema('dev',silver_path)

# COMMAND ----------

create_Gold_Schema('dev',gold_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Show all schemas created

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN dev_catalog"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Creating Tables demo

# COMMAND ----------

# MAGIC %md
# MAGIC ##SQL Method
# MAGIC
# MAGIC First create a table
# MAGIC
# MAGIC Then drop it
# MAGIC
# MAGIC Then confirm drop

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev_catalog.bronze.drop_me
# MAGIC (
# MAGIC   Record_ID INT PRIMARY KEY,
# MAGIC   Count_point_id INT
# MAGIC
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN dev_catalog.bronze

# COMMAND ----------

schema_name = "dev_catalog.bronze"

tables_df = spark.sql(f'SHOW TABLES IN {schema_name}')
display(tables_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_catalog.bronze.drop_me

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN dev_catalog.bronze

# COMMAND ----------

# MAGIC %md
# MAGIC #Create tables in Bronze using python functions

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating raw_traffic table

# COMMAND ----------

def createTable_rawTraffic(environment):
    print(f'Creating raw_Traffic table in {environment}_catalog')
    spark.sql(f"""CREATE TABLE IF NOT EXISTS `{environment}_catalog`.`bronze`.`raw_traffic`
                        (
                            Record_ID INT,
                            Count_point_id INT,
                            Direction_of_travel VARCHAR(255),
                            Year INT,
                            Count_date VARCHAR(255),
                            hour INT,
                            Region_id INT,
                            Region_name VARCHAR(255),
                            Local_authority_name VARCHAR(255),
                            Road_name VARCHAR(255),
                            Road_Category_ID INT,
                            Start_junction_road_name VARCHAR(255),
                            End_junction_road_name VARCHAR(255),
                            Latitude DOUBLE,
                            Longitude DOUBLE,
                            Link_length_km DOUBLE,
                            Pedal_cycles INT,
                            Two_wheeled_motor_vehicles INT,
                            Cars_and_taxis INT,
                            Buses_and_coaches INT,
                            LGV_Type INT,
                            HGV_Type INT,
                            EV_Car INT,
                            EV_Bike INT,
                            Extract_Time TIMESTAMP
                    );""")
    
    print("************************************")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating raw_roads Table

# COMMAND ----------

def createTable_rawRoad(environment):
    print(f'Creating raw_roads table in {environment}_catalog')
    spark.sql(f"""CREATE TABLE IF NOT EXISTS `{environment}_catalog`.`bronze`.`raw_roads`
                        (
                            Road_ID INT,
                            Road_Category_Id INT,
                            Road_Category VARCHAR(255),
                            Region_ID INT,
                            Region_Name VARCHAR(255),
                            Total_Link_Length_Km DOUBLE,
                            Total_Link_Length_Miles DOUBLE,
                            All_Motor_Vehicles DOUBLE
                    );""")
    
    print("************************************")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Calling all functions for creation using python functions
# MAGIC
# MAGIC Remember that the functions create a schema only if it does not exist

# COMMAND ----------

create_Bronze_Schema(env,bronze_path)
createTable_rawTraffic(env)
createTable_rawRoad(env)


create_Silver_Schema(env,silver_path)
create_Gold_Schema(env,gold_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Confirm that schemas were created and that tables were created under the bronze schema

# COMMAND ----------

schemas_df = spark.sql(f'SHOW SCHEMAS IN dev_catalog')
display(schemas_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN dev_catalog.bronze

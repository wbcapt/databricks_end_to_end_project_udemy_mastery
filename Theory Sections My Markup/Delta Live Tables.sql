-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Delta Live Table Creation: this notebook won't work. See below. Just use to understand syntax
-- MAGIC
-- MAGIC This cannot be run interactively (meaning cell by cell). Also, these magic command Markdown cells cannot be used either.
-- MAGIC
-- MAGIC The syntax is "CREATE OR REFERESH STERAMING LIVE TABLE" these are keywords
-- MAGIC
-- MAGIC Also make note of the auto loader syntax that utilizes cloud files
-- MAGIC
-- MAGIC This notebook will get called by the Delta Live Tables pipeline as a source for what to do
-- MAGIC
-- MAGIC Remember that DLT is a declarative method for creating a pipeline and tables meant to reduce the headache of maintaining a medallion architecture. 
-- MAGIC
-- MAGIC Declarative means that you say WAHT to do, not how to do it
-- MAGIC
-- MAGIC This pipeline can be used as a job in a workflow

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Delta Live Table setup syntax

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### raw traffic

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE raw_traffic_dl
AS SELECT 
"Record ID"   AS  Record_ID ,
"Count point id"    AS  Count_point_id ,
"Direction of travel"     AS Direction_of_travel  ,
"Year"     AS  Year ,
"Count date"     AS  Count_date ,
"hour"     AS  hour ,
"Region id"     AS   Region_id,
"Region name"     AS   Region_name,
"Local authority name"     AS  Local_authority_name ,
"Road name"     AS  Road_name ,
"Road Category ID"     AS  Road_Category_ID ,
"Start junction road name"     AS  Start_junction_road_name ,
"End junction road name"     AS   End_junction_road_name,
"Latitude"     AS   Latitude,
"Longitude"     AS   Longitude,
"Link length km"     AS  Link_length_km ,
"Pedal cycles"     AS   Pedal_cycles ,
"Two wheeled motor vehicles"     AS  Two_wheeled_motor_vehicles ,
"Cars and taxis"     AS  Cars_and_taxis ,
"Buses and coaches"     AS   Buses_and_coaches,
"LGV Type"     AS   LGV_Type,
"HGV Type"     AS  HGV_Type ,
"EV Car"     AS   EV_Car,
"EV Bike" AS EV_Bike

/*Note the autoloader syntax here*/
FROM cloud_files(
  'abfss://landing@endtoenddatabricksdev.dfs.core.windows.net/raw_traffic',
  'csv'
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### raw roads

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE raw_roads_dl
AS SELECT 
"Road ID"    AS   Road_ID  ,
"Road category id"    AS    Road_category_id ,
"Road category"    AS   Road_category  ,
"Region id"    AS     Region_id,
"Region name"    AS     Region_name,
"Total link length km"    AS    Total_link_length_km ,
"Total link length miles"    AS    Total_link_length_miles ,
"All motor vehicles"   AS   All_motor_vehicles

FROM cloud_files(
  'abfss://landing@databricksdevstg.dfs.core.windows.net/raw_roads',
  'csv'
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Data quality checks in the DLT pipeline
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### This is called creating experctations. It says what to do when you encounter a NULL

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE traffic_cleaned_dl (
CONSTRAINT valid_Record1 EXPECT ("Year" IS NOT NULL ) ON VIOLATION DROP ROW )
AS SELECT *
FROM STREAM(LIVE.`raw_traffic_dl`)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### This is what to do if you encounter some other unknown value

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE roads_cleaned_dl (
CONSTRAINT valid_Record1 EXPECT ("Region_name" IS NOT NULL ) ON VIOLATION DROP ROW )
AS SELECT *
FROM STREAM(LIVE.`raw_roads_dl`)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Joining both the tables

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE Final_Gold 
AS
SELECT 
  TR.Record_ID,
  TR.Count_point_id,
  TR.Direction_of_travel,
  TR.Year,
  TR.Count_date,
  TR.hour,
  TR.Region_id,
  TR.Region_name,
  TR.Local_authority_name,
  TR.Road_name,
  TR.Road_Category_ID,
  TR.Start_junction_road_name,
  TR.End_junction_road_name,
  TR.Latitude,
  TR.Longitude,
  TR.Link_length_km,
  TR.Pedal_cycles,
  TR.Two_wheeled_motor_vehicles,
  TR.Cars_and_taxis,
  TR.Buses_and_coaches,
  TR.LGV_Type,
  RR.Road_ID

FROM STREAM(LIVE.`traffic_cleaned_dl`) AS TR
JOIN
  STREAM(LIVE.`roads_cleaned_dl`) AS RR
ON 
TR.Road_Category_ID = RR.Road_category_id;

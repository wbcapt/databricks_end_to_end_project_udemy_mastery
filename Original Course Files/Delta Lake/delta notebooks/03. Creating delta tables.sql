-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS delta;

-- COMMAND ----------

CREATE TABLE `delta`.deltaFile
(
    Education_Level VARCHAR(50),
    Line_Number INT,
    Employed INT,
    Unemployed INT,
    Industry VARCHAR(50),
    Gender VARCHAR(10),
    Date_Inserted DATE,
    dense_rank INT
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/deltafile')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log')

-- COMMAND ----------

select *
from TEXT.`dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log/00000000000000000000.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Adding records to delta table

-- COMMAND ----------

INSERT INTO `delta`.deltafile
VALUES
    ('Bachelor', 1, 4500, 500, 'IT', 'Male', '2023-07-12',  1),
    ('Master', 2, 6500, 500, 'Finance', 'Female', '2023-07-12', 2),
    ('High School', 3, 3500, 500, 'Retail', 'Male', '2023-07-12', 3),
    ('PhD', 4, 5500, 500, 'Healthcare', 'Female', '2023-07-12', 4);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Querying the table

-- COMMAND ----------

SELECT *
FROM `delta`.deltafile

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/deltafile')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log')

-- COMMAND ----------

select *
from TEXT.`dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log/00000000000000000001.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Updating data in table

-- COMMAND ----------

UPDATE `delta`.deltafile
SET Industry = 'Finance'
WHERE Education_Level = 'PhD'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log')

-- COMMAND ----------

select *
from TEXT.`dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log/00000000000000000002.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Querying table after UPDATE

-- COMMAND ----------

SELECT *
FROM `delta`.deltafile

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/deltafile')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Delet operation

-- COMMAND ----------

DELETE FROM `delta`.deltafile
WHERE dense_rank =2

-- COMMAND ----------

SELECT *
FROM `delta`.deltafile

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/deltafile')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log')

-- COMMAND ----------

select *
from TEXT.`dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log/00000000000000000003.json`

-- COMMAND ----------


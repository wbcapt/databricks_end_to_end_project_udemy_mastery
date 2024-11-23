-- Databricks notebook source
CREATE TABLE `delta`.OptimizeTable
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

-- MAGIC %md
-- MAGIC
-- MAGIC ## List the location of Delta table 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000000.json'))

-- COMMAND ----------

-- Line number 100
INSERT INTO delta.OptimizeTable
VALUES
    ('Bachelor', 100,  4500, 500, 'Networking', 'Male', '2023-07-12', 1)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Reading JSON to understand which parquet file got inserted

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000001.json'))

-- COMMAND ----------

-- Line Number row is having value 101

INSERT INTO delta.OptimizeTable
VALUES
    ('Bachelor', 101, 5200, 700, 'Networking', 'Male', '2023-07-12', 2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Reading JSON to understand which parquet file got inserted

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000002.json'))

-- COMMAND ----------

-- Line number row is having value 102

INSERT INTO delta.OptimizeTable
VALUES
('Master', 102, 6500, 500, 'Networking', 'Female', '2023-07-12', 3)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Reading JSON to understand which parquet file got inserted

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000003.json'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Listing all parquet files

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Deleting a record

-- COMMAND ----------

DELETE FROM delta.OptimizeTable
WHERE Line_Number = 101

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Seeing delta log files

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000004.json'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Update a record

-- COMMAND ----------

UPDATE `delta`.OptimizeTable
SET Line_Number = 99
WHERE Line_Number = 102

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000005.json'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Optimize Command

-- COMMAND ----------

OPTIMIZE `delta`.OptimizeTable;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000006.json'))

-- COMMAND ----------

SELECT *
FROM `delta`.OptimizeTable;

-- COMMAND ----------

describe history `delta`.OptimizeTable;

-- COMMAND ----------

SELECT *
FROM `delta`.OptimizeTable VERSION AS OF 4

-- COMMAND ----------


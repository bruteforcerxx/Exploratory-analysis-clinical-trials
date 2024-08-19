-- Databricks notebook source
-- MAGIC %python
-- MAGIC clinical_trial_file = "clinicaltrial_2023" # Change to clinicaltrial_2020 or clinicaltrial_2021 to run for those files
-- MAGIC pharma_file = "pharma"
-- MAGIC dbutils.fs.head(f"/FileStore/tables/{clinical_trial_file}.csv")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # CREATING SQL TABLES FROM SPARK DATAFRAMES

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #CREATING DATAFRAME
-- MAGIC from pyspark.sql.types import *
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC delimiter = {
-- MAGIC     "clinicaltrial_2023": "\t",
-- MAGIC     "clinicaltrial_2021": "|",
-- MAGIC     "clinicaltrial_2020": "|",
-- MAGIC     "pharma": ","
-- MAGIC }
-- MAGIC
-- MAGIC def create_dataframe(_file):
-- MAGIC     if _file == "clinicaltrial_2023":
-- MAGIC         rdd = sc.textFile(f"/FileStore/tables/{_file}.csv").map(lambda x: x.rstrip(",").strip('"')).map(lambda row: row.split(delimiter[_file]))
-- MAGIC         head = rdd.first()
-- MAGIC         rdd = rdd.map(lambda row: row + [" " for i in range(len(head) - len(row))] if len(row) < len(head) else row )
-- MAGIC         df = rdd.toDF()
-- MAGIC         first = df.first()
-- MAGIC         for col in range(0, len(list(first))):
-- MAGIC            df = df.withColumnRenamed(f"_{col + 1}", list(first)[col])
-- MAGIC         df = df.withColumn('index', monotonically_increasing_id())
-- MAGIC         return df.filter(~df.index.isin([0])).drop('index')
-- MAGIC     else:
-- MAGIC         return spark.read.csv(f"/FileStore/tables/{_file}.csv", sep=delimiter[_file], header = True)
-- MAGIC
-- MAGIC
-- MAGIC ct_dataframe = create_dataframe(clinical_trial_file)
-- MAGIC pharma_dataframe = create_dataframe(pharma_file)
-- MAGIC ct_dataframe.show(20)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Creating an information schema for the tables
-- MAGIC
-- MAGIC i_schema = StructType([StructField("info_type", StringType()), StructField("value", StringType())])
-- MAGIC conditions_delimiter = { "clinicaltrial_2023": "\|", "clinicaltrial_2021": ",", "clinicaltrial_2020": ","}
-- MAGIC
-- MAGIC date_delimiter = {"clinicaltrial_2023": "-", "clinicaltrial_2021": " ", "clinicaltrial_2020": " "}
-- MAGIC
-- MAGIC rdd = sc.parallelize([("filename", clinical_trial_file), ("conditions_delimiter", conditions_delimiter[clinical_trial_file]), ("date_delimiter", date_delimiter[clinical_trial_file]), ("year", clinical_trial_file[-4:])])
-- MAGIC infos = spark.createDataFrame(rdd, schema=i_schema)
-- MAGIC
-- MAGIC
-- MAGIC infos.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ct_dataframe.createOrReplaceTempView('clinical_trial_table')
-- MAGIC pharma_dataframe.createOrReplaceTempView('pharma')
-- MAGIC infos.createOrReplaceTempView('infos')

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS bdtt_db;
CREATE OR REPLACE TABLE bdtt_db.infos AS SELECT * FROM infos;

-- COMMAND ----------

SELECT * FROM clinical_trial_table

-- COMMAND ----------

SELECT * FROM pharma

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # QUESTION 1

-- COMMAND ----------

-- The number of studies in the dataset

SELECT DISTINCT count(*) FROM clinical_trial_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # QUESTION 2

-- COMMAND ----------

-- list all the types (as contained in the Type column) of studies in the dataset along with the frequencies of each type.
SELECT clinical_trial_table.Type, count(*) as count FROM clinical_trial_table
WHERE clinical_trial_table.Type NOT IN (" ", "")
GROUP BY clinical_trial_table.Type
ORDER BY count DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # QUESTION 3

-- COMMAND ----------

-- The top 5 conditions (from Conditions) with their frequencies.
CREATE OR REPLACE FUNCTION conditions_delimiter()
  RETURNS STRING
  RETURN SELECT FIRST(value) FROM bdtt_db.infos WHERE info_type == 'conditions_delimiter';

CREATE OR REPLACE TEMP VIEW all_conditions AS SELECT explode(split(clinical_trial_table.conditions, (SELECT conditions_delimiter()))) as conditions FROM clinical_trial_table;
SELECT conditions, count(*) as count FROM all_conditions
GROUP BY conditions
ORDER BY count DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # QUESTION 4

-- COMMAND ----------

-- Find the 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored.

CREATE OR REPLACE TEMP VIEW non_pharma_sponsor AS SELECT Sponsor FROM clinical_trial_table WHERE Sponsor NOT IN (SELECT Parent_Company FROM pharma);
SELECT Sponsor, count(*) as count FROM non_pharma_sponsor
GROUP BY Sponsor
ORDER BY count DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # QUESTION 5

-- COMMAND ----------

-- Creating user defined functions for extraction of dates components dynamically for all years
CREATE OR REPLACE FUNCTION date_delimiter()
  RETURNS STRING
  RETURN SELECT FIRST(value) FROM bdtt_db.infos WHERE info_type == 'date_delimiter';

CREATE OR REPLACE FUNCTION date_index_month()
  RETURNS INT
  RETURN SELECT CASE FIRST(value) WHEN ' ' THEN 0 ELSE 1 END AS index FROM bdtt_db.infos WHERE info_type == 'date_delimiter';

CREATE OR REPLACE FUNCTION date_index_year()
  RETURNS INT
  RETURN SELECT CASE FIRST(value) WHEN ' ' THEN 1 ELSE 0 END AS index FROM bdtt_db.infos WHERE info_type == 'date_delimiter';

CREATE OR REPLACE FUNCTION file_year()
  RETURNS INT
  RETURN SELECT FIRST(value) FROM bdtt_db.infos WHERE info_type == 'year';

-- COMMAND ----------

-- Plot number of completed studies for each month in 2023

create or replace temp view extractedDates as SELECT split(Completion, (select date_delimiter()))[(select date_index_month())] as Month, count(*) as Count
From clinical_trial_table
where clinical_trial_table.Status in ('COMPLETED', 'Completed') and split(clinical_trial_table.Completion, (select date_delimiter()))[(select date_index_year())] == (select file_year())
group by Month
order by Month;

Select 
CASE Month
        WHEN '01' THEN 'January'
        WHEN '02' THEN 'February'
        WHEN '03' THEN 'March'
        WHEN '04' THEN 'April'
        WHEN '05' THEN 'May'
        WHEN '06' THEN 'June'
        WHEN '07' THEN 'July'
        WHEN '08' THEN 'August'
        WHEN '09' THEN 'September'
        WHEN '10' THEN 'October'
        WHEN '11' THEN 'November'
        WHEN '12' THEN 'December'
        ELSE Month
    END AS Month, Count from extractedDates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # FURTHER ANALYSIS SQL

-- COMMAND ----------

-- FURTHER ANALYSIS: Top 5 conditions sponsored by non-pharmaceutical companies

CREATE OR REPLACE TEMP VIEW non_pharma_sponsor_trials AS SELECT * FROM clinical_trial_table WHERE Sponsor NOT IN (SELECT Parent_Company FROM pharma);
CREATE OR REPLACE TEMP VIEW non_pharma_trial_conditions AS SELECT explode(split(non_pharma_sponsor_trials.conditions, (SELECT conditions_delimiter()))) as conditions FROM non_pharma_sponsor_trials;
SELECT conditions, count(*) as count FROM non_pharma_trial_conditions
GROUP BY conditions
ORDER BY count DESC
LIMIT 5

-- COMMAND ----------

-- DROPPING FUNCTIONS TO PREPARE ENVIRONMENT FOR NEXT RE-RUN
DROP FUNCTION conditions_delimiter;
DROP FUNCTION date_delimiter;
DROP FUNCTION  date_index_month;
DROP FUNCTION  date_index_year;
DROP FUNCTION  file_year;

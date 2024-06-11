# Databricks notebook source
# MAGIC %run /Workspace/Users/naval@cloudthat.net/Day3/include

# COMMAND ----------

input_file_path

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/adlscloudthat/raw/input_files/circuits.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`dbfs:/mnt/adlscloudthat/raw/input_files/circuits.csv` header=True

# COMMAND ----------

df1=spark.read.json("dbfs:/mnt/adlscloudthat/raw/input_files/constructors.json")

# COMMAND ----------

Method 2: Querying the raw file: simple json, parquet, delta 
    spark SQL (NO OPTIONS)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from format.`path`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table constructor as 
# MAGIC select *,current_timestamp() as ingestion_date from json.`dbfs:/mnt/adlscloudthat/raw/input_files/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`dbfs:/mnt/adlscloudthat/raw/input_files/json/`

# COMMAND ----------



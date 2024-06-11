# Databricks notebook source
(
spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","json")
 .option("cloudFiles.schemaLocation","dbfs:/mnt/adlscloudthat/raw/metadata/naval/autoloader2/schema")
 .option("cloudFiles.schemaEvolutionMode","rescue")
 .option("cloudFiles.inferColumnTypes",True)
 .load("dbfs:/mnt/adlscloudthat/raw/inputstream/json/")
 .writeStream
 .option("checkpointLocation","dbfs:/mnt/adlscloudthat/raw/metadata/naval/autoloader2/checkpoint")
 .option("mergeSchema",True)
 .table("bronze.autoloader2")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.autoloader2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.autoloader2

# COMMAND ----------



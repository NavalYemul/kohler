# Databricks notebook source
Databricks: Autoloader: Advancement of Structured Streaming
1. user schema is not mantadory.
2. Schmea evolution can be handled easily. 

# COMMAND ----------

(
spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .load("dbfs:/mnt/adlscloudthat/raw/inputstream/csv/")
 .writeStream
 .option("checkpointLocation","dbfs:/mnt/adlscloudthat/raw/metadata/naval/autoloader/csv")
 .trigger(once=True)
 .table("bronze.autoloader")
)

# COMMAND ----------

(
spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.schemaLocation","dbfs:/mnt/adlscloudthat/raw/metadata/naval/autoloader/schema")
 .load("dbfs:/mnt/adlscloudthat/raw/inputstream/csv/")
 .writeStream
 .option("checkpointLocation","dbfs:/mnt/adlscloudthat/raw/metadata/naval/autoloader/checkpoint")
 .trigger(once=True)
 .table("bronze.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.autoloader

# COMMAND ----------

(
spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.schemaLocation","dbfs:/mnt/adlscloudthat/raw/metadata/naval/autoloader/schema")
 .option("cloudFiles.inferColumnTypes",True)
 .load("dbfs:/mnt/adlscloudthat/raw/inputstream/csv/")
 .writeStream
 .option("checkpointLocation","dbfs:/mnt/adlscloudthat/raw/metadata/naval/autoloader/checkpoint")
 .table("bronze.autoloader")
)

# COMMAND ----------

(
spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.schemaLocation","dbfs:/mnt/adlscloudthat/raw/metadata/naval/autoloader/schema")
 .option("cloudFiles.schemaEvolutionMode","rescue")
 .option("cloudFiles.inferColumnTypes",True)
 .load("dbfs:/mnt/adlscloudthat/raw/inputstream/csv/")
 .writeStream
 .option("checkpointLocation","dbfs:/mnt/adlscloudthat/raw/metadata/naval/autoloader/checkpoint")
 .option("mergeSchema",True)
 .table("bronze.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.autoloader

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history bronze.autoloader

# COMMAND ----------

Whenever there is change in schema. Nofity me then I make necessay. 

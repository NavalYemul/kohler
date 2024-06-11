# Databricks notebook source
Structured Streaming: Data which grows over time. 
     Graunteens that it read data/file ONLY ONCE.
batch
df=spark.read.csv("path")
df.write.saveAstable("schema.tablname")

Stream/ real time
df=spark.readStream.csv("path")
df.writeStream.option("checkpointLocaiton","path").saveAsTable("schema.tablenaem")

# COMMAND ----------

Streaming:
    1. user defined schema

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adlscloudthat/raw/inputstream/csv/

# COMMAND ----------

df=spark.readStream.csv("dbfs:/mnt/adlscloudthat/raw/inputstream/csv/")

# COMMAND ----------

user_schema="Id int, Name string, Gender string, Salary int, Country string, Date string"

# COMMAND ----------

df=spark.readStream.schema(user_schema).csv("dbfs:/mnt/adlscloudthat/raw/inputstream/csv/",header=True)

# COMMAND ----------

df.writeStream.table("bronze.stream")

# COMMAND ----------

df.writeStream.option("checkpointLocation","dbfs:/mnt/adlscloudthat/raw/metadata/naval/stream/csv").table("bronze.stream")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.bronze.stream

# COMMAND ----------

(
spark
.readStream
.schema(user_schema)
.csv("dbfs:/mnt/adlscloudthat/raw/inputstream/csv/",header=True)
.writeStream
.option("checkpointLocation","dbfs:/mnt/adlscloudthat/raw/metadata/YOURNAME/stream/csv")
.trigger(once=True)
.table("bronze.stream")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.bronze.stream

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.bronze.stream

# COMMAND ----------



# Databricks notebook source
df=spark.read.table("hive_metastore.bronze.superstore")

# COMMAND ----------

Every action---- Spark JOB is created
How many?
Transformation? 
1. Narrow ( )
2. Wide

# COMMAND ----------

df.display()

# COMMAND ----------

df.filter("row_id_no=5")

# COMMAND ----------

df.filter("row_id_no=5").display()

# COMMAND ----------

df.filter("row_id_no=5").explain()

# COMMAND ----------

df.groupBy("Ship_Mode").count()

# COMMAND ----------

df.groupBy("Ship_Mode").count().display()

# COMMAND ----------

df.groupBy("Ship_Mode").count().explain()

# COMMAND ----------



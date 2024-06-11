# Databricks notebook source
# MAGIC %run /Workspace/Users/naval@cloudthat.net/Day3/include

# COMMAND ----------

df=spark.read.table("bronze.bronze_adobe_json")

# COMMAND ----------

df1=df.withColumn("batters",explode("batters.batter"))\
.withColumn("batters_id",col("batters.id"))\
.withColumn("batters_type",col("batters.type"))\
.drop("batters")\
.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
.drop("topping")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists silver

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("silver.silver_adobe")

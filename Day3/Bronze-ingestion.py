# Databricks notebook source
# MAGIC %run /Workspace/Users/naval@cloudthat.net/Day3/include

# COMMAND ----------

dbutils.widgets.text("environment"," ")
v=dbutils.widgets.get("environment")

# COMMAND ----------

df=spark.read.option("multiline",True).json(f"{input_file_path}adobe_sample_json.json")

# COMMAND ----------

df1=add_ingestion(df)

# COMMAND ----------

df2=df1.withColumn("environment",lit(v))

# COMMAND ----------

df2.write.mode("overwrite").saveAsTable("bronze.bronze_adobe_json")

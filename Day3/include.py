# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

input_file_path="dbfs:/mnt/adlscloudthat/raw/input_files/json/"

# COMMAND ----------

def add_ingestion(input):
    output=df.withColumn("ingestion_date",current_timestamp())
    return output

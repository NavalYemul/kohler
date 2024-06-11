# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists gold.gold_adobe as 
# MAGIC select batters_type, count(*) as count from hive_metastore.silver.silver_adobe group by batters_type

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.gold.gold_adobe

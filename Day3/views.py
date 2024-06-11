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

# COMMAND ----------

# MAGIC %md
# MAGIC ### View:  A Virtual Table
# MAGIC 1. Std view : Saves in schema
# MAGIC 2. Temp view: valid only to that spark session and cluster
# MAGIC 3. Global view

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view gold.topping_view as 
# MAGIC select distinct(topping_type) from hive_metastore.silver.silver_adobe

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.gold.topping_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.silver.silver_adobe

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view count_topping as 
# MAGIC select topping_id, count(*) as count_topping_id from  silver.silver_adobe group by all sort by topping_id

# COMMAND ----------

# MAGIC %sql 
# MAGIC show views

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from count_topping

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace global temp view count_topping_global as 
# MAGIC select topping_id, sum(topping_id) as count_topping_id from  silver.silver_adobe group by all sort by topping_id

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %sql
# MAGIC show views in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from count_topping_global

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.count_topping_global

# COMMAND ----------



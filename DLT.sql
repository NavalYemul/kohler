-- Databricks notebook source
Create STREAMING LIVE TABLE sales_bronze
as
select *,current_timestamp() as ingestion_date from cloud_files("dbfs:/mnt/adlscloudthat/raw/dlt_input/sales/","csv", map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

CREATE STREAMING TABLE sales_silver 
(
  CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT DISTINCT * EXCEPT (_rescued_data,ingestion_date) FROM STREAM(LIVE.sales_bronze)

-- COMMAND ----------

Create STREAMING LIVE TABLE product_bronze
as
select *,current_timestamp() as ingestion_date from cloud_files("dbfs:/mnt/adlscloudthat/raw/dlt_input/product/","csv", map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

-- Create and populate the target table.
CREATE OR REFRESH STREAMING LIVE TABLE product_silver;

APPLY CHANGES INTO
  live.product_silver
FROM
  stream(live.product_bronze)
KEYS
  (product_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  seqNum
COLUMNS * EXCEPT
  (operation,seqNum,_rescued_data, ingestion_date)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

Create STREAMING LIVE TABLE customer_bronze
as
select *,current_timestamp() as ingestion_date from cloud_files("dbfs:/mnt/adlscloudthat/raw/dlt_input/customers/","csv", map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

-- Create and populate the target table.
CREATE OR REFRESH STREAMING LIVE TABLE customer_silver;

APPLY CHANGES INTO
  live.customer_silver
FROM
  stream(live.customer_bronze)
KEYS
  (customer_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  sequenceNum
COLUMNS * EXCEPT
  (operation,sequenceNum,_rescued_data, ingestion_date)
STORED AS
  SCD TYPE 2;

-- COMMAND ----------

CREATE LIVE TABLE sales_customers
AS
SELECT s.order_id,s.customer_id,s.total_amount,c.customer_name,c.customer_city
FROM LIVE.sales_silver as s
INNER JOIN LIVE.customer_silver as c
ON s.customer_id=c.customer_id

-- COMMAND ----------

select customer_id,sum(total_amount) as sum_amount from hive_metastore.dlt_ny.sales_customers group by customer_id

-- COMMAND ----------



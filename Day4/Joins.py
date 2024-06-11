# Databricks notebook source
customer_data = [(1001, 'Alice', 'Johnson', '2024-01-15'),
(1002, 'Bob', 'Smith', '2024-01-18'),
(1003, 'Carol', 'Davis', '2024-01-22'),
(1004, 'David', 'Miller', '2024-01-25'),
(1005, 'Emily', 'Martinez', '2024-01-28'),
(1006, 'Frank', 'Taylor', '2024-01-30'),
(1007, 'Grace', 'Anderson', '2024-02-02'),
(1008, 'Harry', 'White', '2024-02-05'),
(1009, 'Iris', 'Brown', '2024-02-08'),
(1010, 'Jack', 'Wilson', '2024-02-12')]
customer_schema = ['Customer_id','First_name','Last_name','Order_date']
customer_df = spark.createDataFrame(data=customer_data,schema=customer_schema)

# COMMAND ----------

sales_data = [
    (1, 1001, 'ProductA', 2, 50.0, '2024-01-15'),
    (2, 1002, 'ProductB', 1, 75.0, '2024-01-18'),
    (3, 1003, 'ProductC', 3, 30.0, '2024-01-22'),
    (4, 1004, 'ProductA', 1, 50.0, '2024-01-25'),
    (5, 1005, 'ProductB', 2, 75.0, '2024-01-28'),
    (6, 1006, 'ProductC', 1, 30.0, '2024-01-30'),
    (7, 1007, 'ProductA', 2, 50.0, '2024-02-02'),
    (8, 1008, 'ProductB', 1, 75.0, '2024-02-05'),
    (9, 1009, 'ProductC', 3, 30.0, '2024-02-08'),
    (10, 1010, 'ProductA', 1, 50.0, '2024-02-12'),
    # Adding some repeated entries for demonstration
    (11, 1002, 'ProductB', 1, 75.0, '2024-01-18'),  # Customer 1002 with ProductB repeated
    (12, 1006, 'ProductC', 1, 30.0, '2024-01-30')]
sales_schema = ['OrderID','CustomerID','Product','Quantity','Price','OrderDate']
sales_df = spark.createDataFrame(data=sales_data,schema=sales_schema)

# COMMAND ----------

df_new=customer_df.join(sales_df,customer_df["Customer_id"]==sales_df["CustomerID"])

# COMMAND ----------

df_new.display()

# COMMAND ----------

df_new.explain()

# COMMAND ----------

Broadcast:
    1. large DF
    2. Small DF

# COMMAND ----------

from pyspark.sql.functions import broadcast

# COMMAND ----------

df_new_broad=customer_df.join(broadcast(sales_df),customer_df["Customer_id"]==sales_df["CustomerID"])

# COMMAND ----------

df_new_broad.display()

# COMMAND ----------

df_new_broad.explain()

# COMMAND ----------



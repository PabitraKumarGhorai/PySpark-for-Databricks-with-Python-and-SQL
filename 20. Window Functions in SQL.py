# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('DataFrame').getOrCreate()
sample_data = [(100,'Agni'),
               (200,'Agni'),
               (200,'Vayu'),
               (300,'Vayu'),
               (500,'Vayu'),
               (500,'Dharti'),
               (700,'Dharti')]

columns = ["new_id","new_cat"]

df = spark.createDataFrame(data = sample_data, schema = columns)
df.display()
df.createOrReplaceTempView('category')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from category

# COMMAND ----------

# Types of window functions:
# 1. Aggregate --> sum, avg, max, min, count
# 2. Ranking --> Row_Number, Rank, Dense_Rank, Percent_Rank
# 3. Value/Analytic --> Lead, Lag, First_Value, Last_Value





# COMMAND ----------

# DBTITLE 1,Aggregate Window Function with Partitioned columns
# MAGIC
# MAGIC %sql
# MAGIC select new_id, new_cat,
# MAGIC sum(new_id) over(PARTITION BY new_cat order by new_id) as Total,
# MAGIC avg(new_id) over(PARTITION BY new_cat order by new_id) as Average,
# MAGIC count(new_id) over(PARTITION BY new_cat order by new_id) as Count,
# MAGIC min(new_id) over(PARTITION BY new_cat order by new_id) as Min,
# MAGIC max(new_id) over (PARTITION BY new_cat order by new_id) as Max
# MAGIC from category

# COMMAND ----------

# DBTITLE 1,Aggregate Window Function without Partitioned columns
# MAGIC
# MAGIC %sql
# MAGIC select new_id, new_cat,
# MAGIC sum(new_id) over(order by new_id rows between unbounded preceding and unbounded following) as Total,
# MAGIC avg(new_id) over(order by new_id rows between unbounded preceding and unbounded following) as Average,
# MAGIC count(new_id) over(order by new_id rows between unbounded preceding and unbounded following) as Count,
# MAGIC min(new_id) over(order by new_id rows between unbounded preceding and unbounded following) as Min,
# MAGIC max(new_id) over (order by new_id rows between unbounded preceding and unbounded following) as Max
# MAGIC from category

# COMMAND ----------

# DBTITLE 1,Rank Window Function 
# MAGIC %sql
# MAGIC select new_id,
# MAGIC row_number() over (order by new_id) as Row_Number,
# MAGIC rank() over (order by new_id) as Rank,
# MAGIC dense_rank() over (order by new_id) as Dense_Rank,
# MAGIC percent_rank() over (order by new_id) as Percent_Rank
# MAGIC from category

# COMMAND ----------

# DBTITLE 1,Analytics Window Function 
# MAGIC %sql
# MAGIC select new_id,
# MAGIC first_value(new_id) over (order by new_id) as First_Value,
# MAGIC last_value(new_id) over (order by new_id) as Last_value,
# MAGIC lead(new_id) over (order by new_id) as Lead,
# MAGIC lag(new_id) over (order by new_id) as lag
# MAGIC from category

# COMMAND ----------

# MAGIC %sql
# MAGIC select new_id,
# MAGIC first_value(new_id) over (order by new_id) as First_Value,
# MAGIC last_value(new_id) over (order by new_id rows between unbounded preceding and unbounded following) as Last_value,
# MAGIC lead(new_id) over (order by new_id) as Lead,
# MAGIC lag(new_id) over (order by new_id) as lag
# MAGIC from category

# COMMAND ----------

# DBTITLE 1,Offset the lead and lag value by 2 in the output columns
# MAGIC %sql
# MAGIC select new_id,
# MAGIC lead(new_id,2) over (order by new_id) as Lead,
# MAGIC lag(new_id,2) over (order by new_id) as lag
# MAGIC from category

# COMMAND ----------


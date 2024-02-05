# Databricks notebook source
# Broadcast join is an optimization technique in the Spark SQL engine that is used to join two spark dataframes.

# Note: Generally spark uses Sort Merge Join to join two dataframes.

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('DataFrame').getOrCreate()
df_1 = spark.read.format('csv').option('inferSchema','true').option('header','true').load('dbfs:/FileStore/transaction.csv')
display(df_1) #Large Table

# COMMAND ----------

df_2 = spark.read.format('csv').option('inferSchema','true').option('header','true').load('dbfs:/FileStore/customers.csv')
display(df_2) # Small Table

# COMMAND ----------

df_1.join(df_2, df_1.customer_code == df_2.customer_code, 'left').explain()


# COMMAND ----------

from pyspark.sql.functions import broadcast
df_1.join(broadcast(df_2), df_1.customer_code == df_2.customer_code, 'left').explain()

# COMMAND ----------

df_join = df_1.join(broadcast(df_2), df_1.customer_code == df_2.customer_code, 'left')
df_join.createOrReplaceTempView("Broadcast_Join")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Broadcast_Join

# COMMAND ----------


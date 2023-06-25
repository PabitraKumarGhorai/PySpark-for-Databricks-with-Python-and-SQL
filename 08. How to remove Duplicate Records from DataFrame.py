# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("DataFrame").getOrCreate()
df_1 = spark.read.format('csv').option('inferschema','true').option('header','true').load('dbfs:/FileStore/tables/Data_1.csv')
df_1.show()
df_1.count()

# COMMAND ----------

# Now we are going to remove duplicates from the DataFrame

df_2 = df_1.distinct()  # It will only show the distinct records not removing duplicates, you can't able to use 'col' object inside distinct.
df_2.show()

# COMMAND ----------

# Now we are going to use drop function for delete duplicates.

df_3 = df_1.dropDuplicates(['Ename'])
df_3.show()

# dropDuplicates() function also removing duplicate records and you can pass it column name inside this function

# COMMAND ----------


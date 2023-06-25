# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFrame").getOrCreate()
df_1 = spark.read.format('csv') \
                    .option('inferSchema','true') \
                    .option('header','true') \
                    .load('dbfs:/FileStore/tables/Data_1.csv')
df_1.printSchema()
display(df_1)

# COMMAND ----------

df_1.createOrReplaceTempView('Temp_Table_1')

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from Temp_Table_1
# MAGIC

# COMMAND ----------


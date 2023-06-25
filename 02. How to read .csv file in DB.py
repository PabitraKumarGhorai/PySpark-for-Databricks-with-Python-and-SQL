# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFrame").getOrCreate()
df = spark.read.format('csv').option('inferSchema','true').option('header','true').load('dbfs:/FileStore/tables/Data_1.csv')

# COMMAND ----------

df.show()

# COMMAND ----------


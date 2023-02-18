# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("DataFrame").getOrCreate()
df1 = spark.read.format('csv').option('inferSchema','true').option('header','true').load('dbfs:/FileStore/tables/emp_data.csv')
df1.show()

df2 = df1.groupBy('Dept_ID').max('Salary')
df2.show()

df3 = df1.groupBy('Dept_ID').agg(sum('Salary').alias("Total_Salary"),max('Salary').alias("Max_Salary"), min('Salary').alias("Min_Salary"))
df3.show()

# COMMAND ----------


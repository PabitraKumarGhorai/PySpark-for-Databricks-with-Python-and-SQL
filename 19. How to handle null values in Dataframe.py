# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('dataframe').getOrCreate()
sampledata = [(1,"Pabitra","52000"),
              (2,"null","72000"),
              (3,"Venkat","89000"),
              (4,"Saikat","")]

schema = ["ID","Name","Salary"]

df = spark.createDataFrame(data = sampledata, schema  = schema)
display(df)

# COMMAND ----------

#Change data type of the Salary column
df = df.withColumn("Salary",df.Salary.cast('int'))
df1 = df.na.fill(0,"Salary")

display(df1)

# COMMAND ----------


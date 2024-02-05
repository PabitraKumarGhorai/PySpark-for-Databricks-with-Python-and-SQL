# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Dataframe").getOrCreate()

define_schema= StructType([
                    StructField("Emp_Id", IntegerType(), False),
                    StructField("Emp_Name", StringType(), False),
                    StructField("Role", StringType(), False),
                    StructField("Designation", StringType(), False),
                    StructField("Salary", IntegerType(), False),
                ])





# COMMAND ----------

df_load1 = spark.read.format('csv').option('header','true') \
                                   .schema(define_schema) \
                                   .load('dbfs:/FileStore/Emp_Record_1.csv')

display(df_load1)



# COMMAND ----------

df_load2 = spark.read.format('csv').option("header","true") \
                                   .schema(define_schema) \
                                   .load('dbfs:/FileStore/Emp_Record_2.csv')

display(df_load2)

# COMMAND ----------

print(define_schema)

# COMMAND ----------

if define_schema == df_load1.printSchema():
    print("Schema is matching")

# COMMAND ----------


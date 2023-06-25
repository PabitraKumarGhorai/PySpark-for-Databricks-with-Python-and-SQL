# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFrame").getOrCreate()
df_1 = spark.read.format('csv') \
                    .option('inferSchema','true') \
                    .option('header','true') \
                    .load('dbfs:/FileStore/tables/Data_1.csv')

df_1.show()



# COMMAND ----------

# For DataFrame cloumn rename we have three methods:
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
df_1.show(2)

#Method 1
cr_1 = df_1.selectExpr("Emp_id as Employee_ID", "Ename as Employee_Name", "Location as LOC", "Dept_id as Dept_NO")
cr_1.show(2)

#Method 2
cr_2 = df_1.withColumnRenamed("Emp_id","E_ID") \
            .withColumnRenamed("Ename","Employee") \
            .withColumnRenamed("Location","Loc") \
            .withColumnRenamed("Dept_id", "Department")

cr_2.show(2)

#Method 3

cr_3 = df_1.select(col("Emp_id").alias("E_id"),col("Ename").alias("Emp_Nmae"),col("Location").alias("Loca"),col("Dept_id").alias("Dept"))
cr_3.show(2)

# COMMAND ----------


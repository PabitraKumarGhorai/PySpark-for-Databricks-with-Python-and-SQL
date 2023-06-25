# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import current_timestamp


spark = SparkSession.builder.appName("DataFrame").getOrCreate()
df_1 = spark.read.format('csv') \
                    .option('inferSchema','true') \
                    .option('header','true') \
                    .load('dbfs:/FileStore/tables/Data_1.csv')

df_1.show(4)



# COMMAND ----------

df_2 = df_1.withColumn("Country",lit("INDIA")) \
            .withColumn("Org_Id", concat(col('Emp_id'),col('Dept_id'))) \
            .withColumn("LOAD_DATE_TIMESTAMP",current_timestamp()) 
            
    
df_2.show(4)

# COMMAND ----------


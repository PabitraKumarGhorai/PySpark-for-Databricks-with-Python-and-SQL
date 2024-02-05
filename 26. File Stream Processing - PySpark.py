# Databricks notebook source
#dbutils.fs.rm('dbfs:/FileStore/tables',True)

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Stream').getOrCreate()

schema = "Emp_Id int, Emp_Name string, Role string, Designation string, Salary int"


df_read = spark.readStream.format('csv').option('header','true') \
                                        .schema(schema) \
                                        .option('maxfilesperTrigger', 2) \
                                        .load('dbfs:/FileStore/tables/')
display(df_read)

# COMMAND ----------

# There are 3 types of output mode : "append" , "complete", "update"


df_read.writeStream.format('memory').queryName("fileStream") \
                                    .trigger(processingTime="10 seconds") \
                                    .outputMode("append").start()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fileStream

# COMMAND ----------


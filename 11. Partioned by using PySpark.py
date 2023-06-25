# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("dataframe").getOrCreate()
dataset = [('India','TCS','60b$'),
        ('India','Wipro','12b$'),
        ('USA','CTS','25b$'),
        ('UK','GSK','65b$'),
        ('UAE','Aramco','23b$')]

columns = ["Country","Company","Value"]
df = spark.createDataFrame(data=dataset, schema=columns)
df.show()

# COMMAND ----------

df.write.mode('overwrite').format('delta').option("overwriteSchema","true").partitionBy('Country').save('dbfs:/FileStore/Temp_Partioned_Table')

# COMMAND ----------


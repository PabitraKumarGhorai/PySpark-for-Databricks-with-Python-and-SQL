# Databricks notebook source
# DBTITLE 1,Data Frame creation using RDD
data = [{"Name": 'Arun', "ID": 10, "State": "WB"},
        {"Name": 'Barun', "ID": 12, "State": "MP"},
        {"Name": 'Pabitra', "ID": 3, "State": "AP"},
        {"Name": 'Esha', "ID": 14, "State": "UP"}]

from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Dataframe').setMaster('local[*]')
sc = SparkContext.getOrCreate(conf)
rdd = sc.parallelize(data)
df1 = rdd.toDF()
df1.show()

# COMMAND ----------

# DBTITLE 1,When statement (Method - 1)
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
df2 = df1.withColumn("FUll_State_Name",when(col("State")=="WB" ,"West Bengal").when(col("State")=="UP","Uttar Pradesh").when(col("State")=="AP","Andhra Pradesh").when(col("State")=="MP","Madhya Pradesh").otherwise("Unknown"))
df2.show()

# COMMAND ----------

# DBTITLE 1,Method - 2
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
df3 = df1.select(when(col("State")=="WB" ,"West Bengal").when(col("State")=="UP","Uttar Pradesh").when(col("State")=="AP","Andhra Pradesh").when(col("State")=="MP","Madhya Pradesh").otherwise("Unknown").alias("Full_State_Name"))
df3.show()

# COMMAND ----------

# DBTITLE 1,Method - 3
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
df4 = df1.select(col("*"),when(col("State")=="WB" ,"West Bengal").when(col("State")=="UP","Uttar Pradesh").when(col("State")=="AP","Andhra Pradesh").when(col("State")=="MP","Madhya Pradesh").otherwise("Unknown").alias("Full_State_Name"))
df4.show()

# COMMAND ----------


# Databricks notebook source
data = [{"Name": 'Arun', "ID": 10, "City": "KOL"},
        {"Name": 'Barun', "ID": 12, "City": "PUNE"},
        {"Name": 'Pabitra', "ID": 3, "City": "CHEN"},
        {"Name": 'Esha', "ID": 14, "City": "MUMBAI"}
        ]

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Dataframe").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf)
rdd = sc.parallelize(data)
type(rdd)

# COMMAND ----------

df = rdd.toDF()
df.show()

# COMMAND ----------


# Databricks notebook source
# RDD --> Resilient Distributed Datasets.
# 1. RDD is a collection of elements that can be partioned across multiple nodes in a clusters and operated on in parallel.
# 2. RDD are immutable means that they cannot be modified once created.
# 3. RDD can be created various sources such as files, collection and other RDD's.

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("dataframe").getOrCreate()

numbers = [15,23,45,87,90]
rdd = spark.sparkContext.parallelize(numbers)
print(type(rdd))
print(rdd.collect())

# COMMAND ----------

#Create RDD from external Data source
rdd2 = spark.sparkContext.textFile("/path/textFile.txt")

# COMMAND ----------

# Creates empty RDD with no partition    
rdd = spark.sparkContext.emptyRDD 
# rddString = spark.sparkContext.emptyRDD[String]

# COMMAND ----------

#Create empty RDD with partition
rdd2 = spark.sparkContext.parallelize([],10) #This creates 10 partitions

# COMMAND ----------

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('Dataframe').setMaster('local[*]')
sc = SparkContext.getOrCreate(conf)

data = [{"Name" : "Pabitra", "Id" : 1, "Company" : "CTS"},
        {"Name" : "Susmita", "Id" : 2, "Company" : "TCS"},
        {"Name" : "Bony", "Id" : 3, "Company" : "Capilary"},
        {"Name" : "Abhijit", "Id" : 4, "Company" : "PWC"}]

rdd = sc.parallelize(data)
df = rdd.toDF()

display(df)





# COMMAND ----------


# Databricks notebook source
# About RDD
# Resilient Distributed Datasets (RDD) is a fundamental data structure of Spark. It is an immutable distributed collection of objects. Each dataset in RDD is divided into logical partitions, which may be computed on different nodes of the cluster. 

# COMMAND ----------

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

# DBTITLE 1,Dataframe from List
sampledata = [("Pabitra Kumar","CTS","KOL"),
              ("Malathi G","CTS","CKC"),
              ("Venkatesh V","CTS","CKC"),
              ("Yatharth Puri","IBM","LON"),
              ("Vignesh S","CTS","UK"),
              ("Rambabu V","CTS","BLR"),
              ("Bikram Jena","TCS","UK")]

columns = ["Employee_Name","Company","Location"]

df = spark.createDataFrame(data=sampledata, schema=columns)

df.show()


# COMMAND ----------

# DBTITLE 1,Dataframe from Dictnionary
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('DataFrame').getOrCreate()

data = [{'student_id': 12, 'name': 'sravan', 'address': 'kakumanu'},
        {'student_id': 14, 'name': 'jyothika', 'address': 'tenali'},
        {'student_id': 11, 'name': 'deepika', 'address': 'repalle'}]

dataframe = spark.createDataFrame(data)
dataframe.show()

# COMMAND ----------

# DBTITLE 1,Create an empty Dataframe
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName('emptyDataFrame').getOrCreate()
RDD = spark.sparkContext.emptyRDD()
column = StructType([])
df = spark.createDataFrame(data=RDD, schema=column)

# Print the dataframe
print('Dataframe :')
df.show()

# Print Schema
print('Schema :')
df.printSchema()


# COMMAND ----------


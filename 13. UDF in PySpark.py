# Databricks notebook source
# UDF are used to extend the function of the framework and reuse these functions on multiple dataframes.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

sampledata1 = [('1002','Pabitra Kumar','35000','CTS'), \
            ('1004','Suman','31000','TCS'), \
            ('1032','Bony Mondal','54000','Capilary'), \
            ('1290','aBHIJIT dAS','32000','wipro')]
columns  = ['ID','Name','Salary','Company']
df1 = spark.createDataFrame(data = sampledata1,schema = columns)

df1.show()


# COMMAND ----------

sampledata2 = [('1012','Aman Kumar','35000','cTS'), \
            ('1026','Suman Paul','31000','TCS'), \
            ('1076','Debjani Mondal','54000','Capgimini'), \
            ('1299','Soumick Karmakar','32000','tcs')]
columns  = ['ID','Name','Salary','Company']
df2 = spark.createDataFrame(data = sampledata2,schema = columns)

df2.show()

df = df1.union(df2)
df.show()

# COMMAND ----------

# Now making a user define function 

#input = 'cTS' , output = 'Cts'

def convertcase(st):
    a=""
    for i in st:
        if ('a'<=i<='z'):
            a=a+i.upper()
        else:
            a=a+i.lower()
    return a

# Function checking:

b="apple"
convertcase(b)
print(convertcase(b))


# COMMAND ----------

from pyspark.sql.functions import col,udf
convertcase_udf = udf(convertcase)
df.select(col("ID"),col("Name"),convertcase_udf(col("Company")).alias("Convert_Comp_Name")).show()

# COMMAND ----------

# To use this udf for sql quires we need to write like this 
# spark.udf.register(str, functionName, returnType)

spark.udf.register("convertcase_sql", convertcase)
# Databricks notebook source
# Window Function in SQL -> Row_Number(), Rank(), Dense_Rank(), Lead(), Lag(), Fisrt(), Last()

#Example : Select *, ROW_NUMBER() OVER(PARTION BY ID order By marks desc) as R1 from table 1


# COMMAND ----------

simpledata1 = [(1,"Pabitra","AEIE", 90)
              ,(2,"Bony","AEIE", 95)
              ,(3,"Akash","IT", 89)]
columns_1 = ["ID","NAME","DEPT","MARKS"]
df1 = spark.createDataFrame(data = simpledata1, schema = columns_1)
df1.show()

# COMMAND ----------

simpledata2 = [(4,"Riya","BA", 73)
              ,(5,"Puja","BCOM", 56)
              ,(6,"Rinku","CSE", 81)]
columns_2 = ["ID","NAME","DEPT","MARKS"]
df2 = spark.createDataFrame(data = simpledata2, schema = columns_2)
df2.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("DataFrame").getOrCreate()

df = df1.union(df2)
window = Window.partitionBy().orderBy(col("MARKS").desc())
df.withColumn("r1", row_number().over(window)).show()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("DataFrame").getOrCreate()

df = df1.union(df2)
window = Window.partitionBy().orderBy(col("MARKS").desc())
df.withColumn("r1", row_number().over(window)).select(col("ID"),col("NAME"),col("MARKS"),col("r1")).show()

# COMMAND ----------


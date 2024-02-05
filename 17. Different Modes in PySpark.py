# Databricks notebook source
# In PySpark 3 types of modes are available.

# 1. PREMISSIVE --> (Default Mode) Set others fields to null when it meets a corrupt record, and puts that malformed string to the next field by columnNameOfCorruptRecord.

# 2. DROPMALFORMED  --> Ignores the whole corrupted records.

# 3. FAILFAST  --> Throughs an exceptions when it meets corrupted records.

# COMMAND ----------

clear cache

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Dataframe").getOrCreate()
df = spark.read.format('csv').option('header','true').load('dbfs:/FileStore/Null_Records-1.csv')
display(df)
df.printSchema()

# COMMAND ----------

# Now I am going to apply schema 

df1 = spark.read.format('csv').option('header','true').schema("ID int,NAME string,SALARY int").load('dbfs:/FileStore/Null_Records-1.csv')
display(df1)
df1.printSchema()

# COMMAND ----------

# Applying PERMISSIVE mode.

df2 = spark.read.format('csv').option('header','true').option('mode','permissive').schema("ID int,NAME string,SALARY int").load('dbfs:/FileStore/Null_Records-1.csv')
display(df2)
df2.printSchema()


# COMMAND ----------

# Applying DROPMALFORMED mode.

df3 = spark.read.format('csv').option('header','true').option('mode','dropmalformed').schema("ID int,NAME string,SALARY int").load('dbfs:/FileStore/Null_Records-1.csv')
display(df3)
df3.printSchema()

# COMMAND ----------

# Applying FAILFAST mode.

df4 = spark.read.format('csv').option('header','true').option('mode','failfast').schema("ID int,NAME string,SALARY int").load('dbfs:/FileStore/Null_Records-1.csv')
display(df4)
df4.printSchema()

# COMMAND ----------

# Above command fail because there is string value presents in the SALARY column.

# To get those bad records we can use "badRecordsPath"



df_badRecords = spark.read.format('csv').option('badRecordspath','/temp/badRecords').option('header','true').schema("ID int,NAME string,SALARY int").load('dbfs:/FileStore/Null_Records-1.csv')
display(df_badRecords)
df_badRecords.printSchema()

# COMMAND ----------

# It will show, reason for the errors!!

%fs head dbfs:/temp/badRecords/20230723T164134/bad_records/part-00000-da1daede-bcba-4924-a08c-0eb385f4263e

# COMMAND ----------


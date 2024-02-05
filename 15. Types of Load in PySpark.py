# Databricks notebook source
# There are two types of Data Loads are available :

#   1. Incremental Load --> This is where all your data is selected, moved in bulk, and replaced by new data. It’s not as complex as incremental loading, as no specific loading order is required.


#   2. Full Load  --> This is where you are moving new data in intervals. Due to its intricate nature, delivery time during incredmental loading is much faster than during its counterpart. However, incremental loads are more likely to encounter problems. This is because you must manage them as individual batches rather than one big group.

# COMMAND ----------

# DBTITLE 1,Incremental / Delta Load
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName('DataFrame').getOrCreate()

sample_data = [("Pabitra Kumar","CTS","KOL"),
              ("Malathi G","CTS","CKC"),
              ("Venkatesh V","CTS","CKC"),
              ("Yatharth Puri","IBM","LON"),
              ("Vignesh S","CTS","UK"),
              ("Rambabu V","CTS","BLR"),
              ("Bikram Jena","TCS","UK")]

columns = ["Employee_Name","Company","Location"]

df = spark.createDataFrame(data = sample_data, schema = columns)
display(df)


# COMMAND ----------

# Write the data with today's date

df = df.withColumn('Date', current_date())
display(df)
df.write.partitionBy('Date').mode('append').saveAsTable('ICREMENTAL_PARTITIONED_TABLE')

# COMMAND ----------

# Write the data with today's date + 1

df_1 = df.withColumn('Date', date_add(current_date(),1))
display(df_1)
df_1.write.partitionBy('Date').mode('append').saveAsTable('ICREMENTAL_PARTITIONED_TABLE')

# COMMAND ----------

# DBTITLE 1,Full Load
# In Full Load data is completly load from Source to Target, with overwrite all things with the newly updated data.


from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName('DataFrame').getOrCreate()

sample_data = [("Pabitra Kumar","CTS","KOL"),
              ("Malathi G","CTS","CKC"),
              ("Venkatesh V","CTS","CKC"),
              ("Yatharth Puri","IBM","LON"),
              ("Vignesh S","CTS","UK"),
              ("Rambabu V","CTS","BLR"),
              ("Bikram Jena","TCS","UK")]

columns = ["Employee_Name","Company","Location"]
df = spark.createDataFrame(data = sample_data, schema = columns)

# !! Warning -- Don't run this will get error.
df.write.format('delta').mode('overwrite').load('<destination_path>')
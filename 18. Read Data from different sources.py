# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('DataFrame').getOrCreate()

# COMMAND ----------

df_csv = spark.read.format('csv').option('header','True').option('inferSchema','True').load('dbfs:/FileStore/countries.csv')
df_csv.show()

# COMMAND ----------

display(df_csv)

# COMMAND ----------

##dbutils.fs.rm('/FileStore/udemy/', True) 

# COMMAND ----------

# DBTITLE 1,Define Schema manually for a Dataframe
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

countries_schema = StructType([
                    StructField("COUNTRY_ID", IntegerType(), False),
                    StructField("NAME", StringType(), False),
                    StructField("NATIONALITY", StringType(), False),
                    StructField("COUNTRY_CODE", StringType(), False),
                    StructField("ISO_ALPHA2", StringType(), False),
                    StructField("CAPITAL", StringType(), False),
                    StructField("POPULATION", DoubleType(), False),
                    StructField("AREA_KM2", IntegerType(), False),
                    StructField("REGION_ID", IntegerType(), True),
                    StructField("SUB_REGION_ID", IntegerType(), True),
                    StructField("INTERMEDIATE_REGION_ID", IntegerType(), True),
                    StructField("ORGANIZATION_REGION_ID", IntegerType(), True)

                    ])

df_sch = spark.read.format('csv').option('header','true').schema(countries_schema).load('dbfs:/FileStore/countries.csv')

# COMMAND ----------

display(df_sch)

# COMMAND ----------

# DBTITLE 1,Single Line JSON
df_sl_json = spark.read.json('dbfs:/FileStore/countries_single_line.json')
df_sl_json.show()

# COMMAND ----------

display(df_sl_json)

# COMMAND ----------

# DBTITLE 1,Multi-line JSON
df_ml_json = spark.read.format('json').option('multiLine','True').load('dbfs:/FileStore/countries_multi_line.json')
df_ml_json.show()

# COMMAND ----------

display(df_ml_json)

# COMMAND ----------


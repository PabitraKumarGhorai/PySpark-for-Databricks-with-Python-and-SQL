#Parameters passing in Databrcks for creating widgets in Databricks

dbutils.widgets.removeAll()
dbutils.widgets.text("Curatedpath",'')      # /mnt/curated/mydrive/salesdata.csv
dbutils.widgets.text("TargetTable",'')      # KPI_SALES_DATA

#Initializing variables

Cuaretedpath = dbutils.widgets.get("Curatedpath")
TargetTable = dbutils.widgets.get("TargetTable")

from pyspark.sql.functions import *
from pyspark.sql.functions import expr
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType

from pyspark.sql.types import *
from pyspark.sql import *

import pandas as pd
import os.path import isfile,join

from datetime import *
from time import *
from pyspark.sql.functions import current_timestamp

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Dataframe").getOrCreate()
df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(Cuaretedpath)
display(df)

df.createOrReplaceTempView("KPI_SALES_TMP_DATA")        #In the Databricks now you can run sql queries on this table by writing "select * from KPI_SALES_TMP_DATA"

#Now creating delta tables in Azure ADLS stoarge account

df.write.mode("overwrite").option("overwriteSchema","true").format("delta").save("/mnt/curated/shared/KPI_SALES_DATA")

#Now you can read this delta table & create dataframe

df1.read.format("delta").load("/mnt/curated/shared/KPI_SALES_DATA")
df1.createOrReplaceTempView("KPI_SALES_REPORT"

)





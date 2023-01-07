from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFrame").getOrCreate()
df1.read.format("delta").load("/mnt/curated/shared/KPI_SALES_DATA")


sqlquery = """
select 
Trim(Oult_Code) as Outlet_Code,
Trim(`Canal Rx) as Channel_Rx,
Trim(CNPJ) as CNPJ,
Trim(Sale_Dtls) as Sales_Details,
Trim(`prod_code) as Product_Code
case
    when Trim(`Canal Rx) = "Nao Elegiveis" Then "Not Elligible"
    when Trim(`Canal Rx) = "Independentes" Then "Independence"
    when Trim(`Canal Rx) = "Grandes Redes" Then "Big Chains"
End as Channel_Conversion,
Trim(Cep) as Zip_Code,
Trim(UF) as State_code
from 
KPI_SALES_REPORT

 """
df = spark.sql(sqlquery)
df = df.withColumn("LOAD_DATE_TIMESTAMP",current_timestamp())
df.createOrReplaceTempView("KPI_SALES_Table")

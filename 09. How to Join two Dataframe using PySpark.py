# Databricks notebook source
sample_data1 = [("E100","Pabitra",45000,"D10") \
                ,("E200","Bony",55000,"D20") \
                ,("E300","Sheryan",25000,"D10") \
                ,("E400","Abhijit",35000,"D20") \
                ,("E500","Susmita",42000,"D30") \
                ,("E600","Ratna",42000,"D50")
               ]
columns1 = ["ID","Name","Salary","Dept_No"]
df_1 = spark.createDataFrame(data = sample_data1, schema = columns1)
df_1.show()

# COMMAND ----------

sample_data2 = [("D10","Data Engineer","Hp Pavillion") \
                ,("D20","Web Developer","MacBook") \
                ,("D30","Dot Net Developer","Lenevo ThinkPad") \
                ,("D40","Tester","Acer TravelBook")
              ]
columns2 = ["Dept_No","Profile","System"]
df_2 = spark.createDataFrame(data = sample_data2, schema = columns2)
df_2.show()

# COMMAND ----------

# DBTITLE 1,Now join above two DataFrame
#Inner Join
df_1.join(df_2, df_1.Dept_No == df_2.Dept_No,"inner").show()

# COMMAND ----------

#Left Join
df_1.join(df_2, df_1.Dept_No == df_2.Dept_No,"left").show()

# COMMAND ----------

#Right Join
df_1.join(df_2, df_1.Dept_No == df_2.Dept_No,"right").show()

# COMMAND ----------

df_1.alias("A").join(df_2.alias("B"),col("A.Dept_No")==col("B.Dept_No"),"right").select(col("A.ID"),col("A.Name"),col("A.Salary"),col("B.Dept_No")).show()

# COMMAND ----------


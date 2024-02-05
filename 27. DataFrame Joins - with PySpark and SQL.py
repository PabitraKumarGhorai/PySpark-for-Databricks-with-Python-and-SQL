# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Dataframe').getOrCreate()


data1 = [(1001, "Pabitra Kumar", 10, "Data Engineer", 56000),
         (1002, "Susmita Das", 12, "dot net", 36000),
         (1003, "Venkatesh Chittur", 10, "ETL Tester", 86000),
         (1004, "Ratna Bag", 14, "Manual Tester", 46000),
         (1007, "Bony Mondal", 12, "SDET 1", 56000),
         (1008, "Abhijit Das", 12, "Maximo Developer", 56000),
         (1017, "Megha Khare", 10, "Data Engineer", 68000),
         (1009, "Anirban Das", 14, "SAP QEA", 33000),
         (1010, "Yash Kurim", 11, "CIS", 26000),
         (10013, "Saswata Sarkar", 11, "CIS", 34500),
         (1023, "Malathy G", 10, "ETL Tester", 56000),
         (1045, "Suresh J", 10, "ETL Tester", 36000),
         (1235, "Vignesh Srini", 10, "PBI Developer", 79000),
         (1407, "Sangram Halder", 0,"Trainee", 12460),
         (1877, "Kunal Halder", 0, "Trainee", 12460)
        ]

schema = "Emp_Id string, Emp_Name string, Dept_Id string, Designation string, Salary string"


df_emp = spark.createDataFrame(data = data1, schema = schema)

display(df_emp)
df_emp.createOrReplaceTempView("employee")

# COMMAND ----------

data2 = [(10, "AIA", "KOLKATA", "CHENNAI"),
         (12, "WEB DEVELOPMENT", "KOLKATA", "BANGALORE"),
         (14, "QUALITY ASSUARANCE", "CHENNAI", "KOLKATA"),
         (11, "CLOUD INFRA SUPPORT", "MUMBAI", "NOIDA"),
         (15, "ADM", "KOLKATA", "HYDRABAD"),
         (16, "EPS", "BANGALORE", "CHENNAI"),
         (17, "CRM", "GURUGRAM", "PUNE"),
         (20, "HR", "CHENNAI", "CHENNAI")
         ]

dept_schema = "Dept_Id int, Dept_Name string, Base_Location string, Project_Location string"

df_dept = spark.createDataFrame(data = data2, schema = dept_schema)

display(df_dept)

df_dept.createOrReplaceTempView("dept")

# COMMAND ----------

# DBTITLE 1,Inner Join in PySpark
df_inner_join = df_emp.join(df_dept, df_emp.Dept_Id == df_dept.Dept_Id, 'inner')
display(df_inner_join)

# COMMAND ----------

# MAGIC %sql
# MAGIC select E.Emp_Id, E.Emp_Name, 
# MAGIC D.Dept_Id, E.Designation, E.Salary, 
# MAGIC D.Dept_Id, D.Dept_Name, 
# MAGIC D.Base_Location, D.Project_Location
# MAGIC from Employee E 
# MAGIC join Dept D on E.Dept_Id = D.Dept_Id

# COMMAND ----------

# DBTITLE 1,Left Join
df_left_join = df_emp.join(df_dept, df_emp.Dept_Id == df_dept.Dept_Id, 'left')
display(df_left_join)

# COMMAND ----------

# MAGIC %sql
# MAGIC select E.Emp_Id, E.Emp_Name, 
# MAGIC D.Dept_Id, E.Designation, E.Salary, 
# MAGIC D.Dept_Id, D.Dept_Name, 
# MAGIC D.Base_Location, D.Project_Location
# MAGIC from Employee E 
# MAGIC left join Dept D on E.Dept_Id = D.Dept_Id

# COMMAND ----------

# DBTITLE 1,Right Join
df_right_join = df_emp.join(df_dept, df_emp.Dept_Id == df_dept.Dept_Id, 'right')
display(df_right_join)

# COMMAND ----------

# MAGIC %sql
# MAGIC select E.Emp_Id, E.Emp_Name, 
# MAGIC E.Dept_Id, E.Designation, E.Salary, 
# MAGIC D.Dept_Id, D.Dept_Name, 
# MAGIC D.Base_Location, D.Project_Location
# MAGIC from Employee E 
# MAGIC right join Dept D on E.Dept_Id = D.Dept_Id

# COMMAND ----------

# DBTITLE 1,Full Outer join
df_full_outer_join = df_emp.join(df_dept, df_emp.Dept_Id == df_dept.Dept_Id, 'outer')
display(df_full_outer_join)

# COMMAND ----------

# MAGIC %sql
# MAGIC select E.Emp_Id, E.Emp_Name, 
# MAGIC D.Dept_Id, E.Designation, E.Salary, 
# MAGIC D.Dept_Id, D.Dept_Name, 
# MAGIC D.Base_Location, D.Project_Location
# MAGIC from Employee E 
# MAGIC full outer join Dept D on E.Dept_Id = D.Dept_Id

# COMMAND ----------

# DBTITLE 1,Dept wise highest salary
# MAGIC %sql
# MAGIC select Dept_Id, max(Salary) as Highest_Salary
# MAGIC from Employee 
# MAGIC group by
# MAGIC
# MAGIC Dept_Id

# COMMAND ----------

# DBTITLE 1,5th Highest Salary in SQL
# MAGIC %sql
# MAGIC select Emp_Name, Salary
# MAGIC from 
# MAGIC (select Emp_Name, Salary, dense_rank() over (order by Salary desc) as r
# MAGIC from Employee) as sub_query
# MAGIC where 
# MAGIC r = 5

# COMMAND ----------


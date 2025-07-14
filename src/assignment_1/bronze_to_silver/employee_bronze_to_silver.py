# Databricks notebook source
# MAGIC %run "../source_to_bronze/utils.ipynb"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import current_date

# COMMAND ----------

employee_schema = StructType([
    StructField("EmployeeID", StringType(), True),
    StructField("EmpName", StringType(), True),
    StructField("DepartmentID", StringType(), True),
    StructField("CountryID", StringType(), True),
    StructField("Salary", DoubleType(), True),
    StructField("Age", IntegerType(), True)
])


# COMMAND ----------

employee_df_1 = read_csv_with_schema("/Volumes/workspace/default/data_csv/Employee-Q1.csv", employee_schema)
display(employee_df_1)

# COMMAND ----------

from pyspark.sql.functions import udf
import re

# COMMAND ----------

def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
def camel_to_snake_func(col_name):
    return camel_to_snake(col_name)

camel_to_snake_udf = udf(camel_to_snake_func, StringType())

# COMMAND ----------

old_columns = employee_df_1.columns

new_columns = [camel_to_snake(col) for col in old_columns]  # from utils, not UDF inside DataFrame

for old_col, new_col in zip(old_columns, new_columns):
    employee_df_1 = employee_df_1.withColumnRenamed(old_col, new_col)

display(employee_df_1)

# COMMAND ----------

employee_df_1 = employee_df_1.withColumn("load_date", current_date())

spark.sql("CREATE DATABASE IF NOT EXISTS Employee_info")

employee_df_1.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/workspace/default/data_csv/employee_info/dim_employee")

# COMMAND ----------

df = spark.read.format("delta").load("/Volumes/workspace/default/data_csv/employee_info/dim_employee")
display(df)
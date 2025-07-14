# Databricks notebook source
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrame to CSV") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %run "/Workspace/Users/aradhana.dubey@diggibyte.com/source_to_bronze/utils.ipynb"

# COMMAND ----------

employee_df = spark.read.option("header", True).csv("/Volumes/workspace/default/data_csv/Employee-Q1.csv")
employee_df.printSchema()
employee_df.show()

# COMMAND ----------

department_df = spark.read.option("header", True).csv("/Volumes/workspace/default/data_csv/Department-Q1.csv")
country_df = spark.read.option("header", True).csv("/Volumes/workspace/default/data_csv/Country-Q1.csv")
department_df.display()
country_df.display()

# COMMAND ----------


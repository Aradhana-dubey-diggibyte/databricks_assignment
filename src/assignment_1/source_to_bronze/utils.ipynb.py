# Databricks notebook source
def read_csv_file(path):
    return spark.read.option("header", True).format("csv").load(path)

# COMMAND ----------

# function to write a dataFrame
def write_csv_file(df, output_path):
    df.write.mode("overwrite").option("header", True).format("csv").save(output_path)


# COMMAND ----------

# function to read a csv file with schema
def read_csv_with_schema(path, schema):
    return spark.read.option("header", True).schema(schema).format("csv").load(path)
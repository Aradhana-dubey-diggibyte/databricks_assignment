# Databricks notebook source
# MAGIC %run "../source_to_bronze/utils.ipynb"

# COMMAND ----------

df = spark.read.format("delta").load("/Volumes/workspace/default/data_csv/employee_info/dim_employee")
display(df)

# COMMAND ----------

from pyspark.sql.functions import sum
from pyspark.sql.functions import avg
from pyspark.sql.functions import current_date

# COMMAND ----------

dept_sal_df = df.groupBy("department_id") \
    .agg(sum("salary").alias("total_salary")) \
    .orderBy("total_salary", ascending=False)

display(dept_sal_df)


# COMMAND ----------

dept_country_count_df = df.groupBy("department_id", "country_id") \
    .count() \
    .withColumnRenamed("count", "employee_count")

display(dept_country_count_df)

# COMMAND ----------

department_df = read_csv_file("/Volumes/workspace/default/data_csv/Department-Q1.csv")
country_df = read_csv_file("/Volumes/workspace/default/data_csv/Country-Q1.csv")

department_df = department_df \
    .withColumnRenamed("DepartmentID", "department_id") \
    .withColumnRenamed("DepartmentName", "department_name")


# COMMAND ----------

country_df = country_df \
    .withColumnRenamed("CountryCode", "country_id") \
    .withColumnRenamed("CountryName", "country_name")



emp_dept_country_df = df.select("department_id", "country_id").distinct() \
    .join(department_df, on="department_id", how="inner") \
    .join(country_df, on="country_id", how="inner") \
    .select("department_name", "country_name")

# COMMAND ----------

avg_age_df = df.groupBy("department_id") \
    .agg(avg("age").alias("average_age"))

display(avg_age_df)

# COMMAND ----------

dept_salary_df = dept_sal_df.withColumn("at_load_date", current_date())
dept_country_count_df = dept_country_count_df.withColumn("at_load_date", current_date())
emp_dept_country_df = emp_dept_country_df.withColumn("at_load_date", current_date())
avg_age_df = avg_age_df.withColumn("at_load_date", current_date())

display(dept_sal_df)
display(dept_country_count_df)
display(emp_dept_country_df)
display(avg_age_df)


# COMMAND ----------

final_df = dept_salary_df.unionByName(dept_country_count_df, allowMissingColumns=True) \
    .unionByName(emp_dept_country_df, allowMissingColumns=True) \
    .unionByName(avg_age_df, allowMissingColumns=True)

final_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("replaceWhere", "at_load_date = current_date()") \
    .save("/Volumes/workspace/default/data_csv/gold_employee/fact_employee")

display(final_df)